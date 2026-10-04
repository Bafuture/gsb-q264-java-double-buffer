package com.example.gsb.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * 双缓冲 vs 读写锁在不同读写比下的吞吐量对比。
 *
 * <p>两种方案保护同一份状态（16 个 long）：读操作求和，写操作整体 +1。
 * 对比只断言两边都真实完成了读写（防止空转），耗时/吞吐以表格形式输出，
 * 避免在共享 CI 机器上产生不稳定的硬性断言。
 */
class PerformanceComparisonTest {

    private static final int STATE_SIZE = 16;
    private static final long SCENARIO_MILLIS = 250;

    private record Scenario(String label, int readerThreads, int writerThreads) {
    }

    private record Result(long reads, long writes, long millis) {
        private long totalOps() {
            return reads + writes;
        }
    }

    @Test
    @Timeout(120)
    void compareDoubleBufferAgainstReadWriteLock() throws Exception {
        List<Scenario> scenarios = List.of(
                new Scenario("读写比 1:1 ", 2, 2),
                new Scenario("读写比 8:1 ", 8, 1),
                new Scenario("读写比 32:1", 32, 1));

        System.out.println();
        System.out.println("场景        | 方案            | 读操作数   | 写操作数  | 总吞吐(ops/ms) | 耗时(ms)");
        System.out.println("------------+-----------------+-----------+----------+----------------+---------");
        for (Scenario scenario : scenarios) {
            Result db = runDoubleBuffer(scenario);
            Result rw = runReadWriteLock(scenario);
            printRow(scenario.label(), "DoubleBuffer", db);
            printRow(scenario.label(), "ReadWriteLock", rw);
            assertThat(db.reads()).as("double-buffer reads").isPositive();
            assertThat(db.writes()).as("double-buffer writes").isPositive();
            assertThat(rw.reads()).as("rwlock reads").isPositive();
            assertThat(rw.writes()).as("rwlock writes").isPositive();
        }
    }

    private static void printRow(String scenario, String impl, Result result) {
        System.out.printf("%s  | %-15s | %9d | %8d | %14.1f | %7d%n",
                scenario, impl, result.reads(), result.writes(),
                result.totalOps() / (double) result.millis(), result.millis());
    }

    private static Result runDoubleBuffer(Scenario scenario) throws Exception {
        DoubleBuffer<long[]> db = new DoubleBuffer<>(() -> new long[STATE_SIZE]);
        AtomicBoolean stop = new AtomicBoolean();
        LongAdder reads = new LongAdder();
        LongAdder writes = new LongAdder();

        ExecutorService pool = Executors.newFixedThreadPool(scenario.readerThreads() + scenario.writerThreads());
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < scenario.readerThreads(); i++) {
            futures.add(pool.submit(() -> {
                long sink = 0;
                while (!stop.get()) {
                    sink += db.read(PerformanceComparisonTest::sum);
                    reads.increment();
                }
                if (sink == Long.MIN_VALUE) {
                    throw new AssertionError("unreachable");
                }
            }));
        }
        for (int i = 0; i < scenario.writerThreads(); i++) {
            futures.add(pool.submit(() -> {
                while (!stop.get()) {
                    db.write(state -> {
                        for (int k = 0; k < state.length; k++) {
                            state[k]++;
                        }
                    });
                    writes.increment();
                }
            }));
        }

        Result result = runFor(pool, futures, stop, reads, writes);
        assertThat(db.stats().swapCount()).isEqualTo(writes.sum());
        return result;
    }

    private static Result runReadWriteLock(Scenario scenario) throws Exception {
        long[] state = new long[STATE_SIZE];
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        AtomicBoolean stop = new AtomicBoolean();
        LongAdder reads = new LongAdder();
        LongAdder writes = new LongAdder();

        ExecutorService pool = Executors.newFixedThreadPool(scenario.readerThreads() + scenario.writerThreads());
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < scenario.readerThreads(); i++) {
            futures.add(pool.submit(() -> {
                long sink = 0;
                while (!stop.get()) {
                    lock.readLock().lock();
                    try {
                        sink += sum(state);
                    } finally {
                        lock.readLock().unlock();
                    }
                    reads.increment();
                }
                if (sink == Long.MIN_VALUE) {
                    throw new AssertionError("unreachable");
                }
            }));
        }
        for (int i = 0; i < scenario.writerThreads(); i++) {
            futures.add(pool.submit(() -> {
                while (!stop.get()) {
                    lock.writeLock().lock();
                    try {
                        for (int k = 0; k < state.length; k++) {
                            state[k]++;
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }
                    writes.increment();
                }
            }));
        }

        return runFor(pool, futures, stop, reads, writes);
    }

    private static Result runFor(ExecutorService pool, List<Future<?>> futures, AtomicBoolean stop,
                                 LongAdder reads, LongAdder writes) throws Exception {
        long start = System.nanoTime();
        Thread.sleep(SCENARIO_MILLIS);
        stop.set(true);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        for (Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
        pool.shutdownNow();
        return new Result(reads.sum(), writes.sum(), elapsedMillis);
    }

    private static long sum(long[] state) {
        long total = 0;
        for (long value : state) {
            total += value;
        }
        return total;
    }
}
