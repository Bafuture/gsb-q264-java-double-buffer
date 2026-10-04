package com.example.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Throughput comparison between the lock-free double buffer and a classic
 * read-write lock at different read/write ratios. Both implementations expose
 * the same tiny workload (sum / increment a 4-long array). The consumer is
 * created once per worker thread so the hot paths allocate nothing.
 */
@Timeout(120)
class PerformanceComparisonTest {

    private static final int THREADS = 8;
    private static final long WARMUP_MS = 300;
    private static final long MEASURE_MS = 700;

    private interface SharedState {
        /** Called once per worker thread; the returned read op is reused. */
        Runnable newReader(long[] sink);

        void writeOp();
    }

    /** Baseline: every read and write takes a read-write lock. */
    private static final class RwLockState implements SharedState {
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final long[] data = new long[4];
        private final AtomicBoolean failed;

        RwLockState(AtomicBoolean failed) {
            this.failed = failed;
        }

        @Override
        public Runnable newReader(long[] sink) {
            return () -> {
                lock.readLock().lock();
                try {
                    long s = 0;
                    for (long v : data) {
                        s += v;
                    }
                    if (s != data[0] * 4L) {
                        failed.set(true);
                    }
                    sink[0] += s;
                } finally {
                    lock.readLock().unlock();
                }
            };
        }

        @Override
        public void writeOp() {
            lock.writeLock().lock();
            try {
                for (int i = 0; i < data.length; i++) {
                    data[i]++;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    /** Lock-free ping-pong: one atomic swap per write, readers take no locks. */
    private static final class DoubleBufferState implements SharedState {
        private final DoubleBuffer<long[]> db = new DoubleBuffer<>(() -> new long[4], 1);
        private final AtomicBoolean failed;

        DoubleBufferState(AtomicBoolean failed) {
            this.failed = failed;
        }

        @Override
        public Runnable newReader(long[] sink) {
            Consumer<long[]> reader = arr -> {
                long s = 0;
                for (long v : arr) {
                    s += v;
                }
                if (s != arr[0] * 4L) {
                    failed.set(true);
                }
                sink[0] += s;
            };
            return () -> db.readVoid(reader);
        }

        @Override
        public void writeOp() {
            db.writeBatch(arr -> {
                for (int i = 0; i < arr.length; i++) {
                    arr[i]++;
                }
            });
        }
    }

    /** {reads, writes} completed during the measurement window. */
    private static long[] run(SharedState state, int readPercent) throws InterruptedException {
        LongAdder reads = new LongAdder();
        LongAdder writes = new LongAdder();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);
        long begin = System.nanoTime() + 50_000_000L;
        long warmupEnd = begin + TimeUnit.MILLISECONDS.toNanos(WARMUP_MS);
        long stopAt = warmupEnd + TimeUnit.MILLISECONDS.toNanos(MEASURE_MS);

        for (int t = 0; t < THREADS; t++) {
            Thread worker = new Thread(() -> {
                long[] sink = new long[1];
                Runnable reader = state.newReader(sink);
                await(start);
                while (true) {
                    long now = System.nanoTime();
                    if (now >= stopAt) {
                        break;
                    }
                    boolean isRead = ThreadLocalRandom.current().nextInt(100) < readPercent;
                    if (isRead) {
                        reader.run();
                    } else {
                        state.writeOp();
                    }
                    if (now >= warmupEnd) {
                        if (isRead) {
                            reads.increment();
                        } else {
                            writes.increment();
                        }
                    }
                }
                blackhole(sink[0]);
                done.countDown();
            }, "worker-" + t);
            worker.setDaemon(true);
            worker.start();
        }
        start.countDown();
        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        return new long[] {reads.sum(), writes.sum()};
    }

    private static volatile long blackhole;

    private static void blackhole(long v) {
        blackhole += v;
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Test
    void compareThroughputAtDifferentRatios() throws InterruptedException {
        int[] readPercent = {99, 90, 50, 10};
        System.out.println();
        System.out.println("throughput (8 threads, " + MEASURE_MS + " ms measurement each):");
        System.out.printf("%-12s | %-26s | %-26s | %s%n",
                "read:write", "read-write lock", "double buffer", "speedup");
        for (int p : readPercent) {
            AtomicBoolean rwFailed = new AtomicBoolean();
            AtomicBoolean dbFailed = new AtomicBoolean();
            long[] rw = run(new RwLockState(rwFailed), p);
            long[] db = run(new DoubleBufferState(dbFailed), p);
            long rwOps = rw[0] + rw[1];
            long dbOps = db[0] + db[1];
            double seconds = MEASURE_MS / 1000.0;
            System.out.printf("%3d:%-8d | %12d op/s (r:%8d w:%7d) | %12d op/s (r:%8d w:%7d) | %.2fx%n",
                    p, 100 - p,
                    (long) (rwOps / seconds), rw[0], rw[1],
                    (long) (dbOps / seconds), db[0], db[1],
                    (double) dbOps / rwOps);

            assertThat(rwFailed.get()).isFalse();
            assertThat(dbFailed.get()).isFalse();
            assertThat(rwOps).as("lock ops at ratio %d:100", p).isGreaterThan(50_000);
            assertThat(dbOps).as("double-buffer ops at ratio %d:100", p).isGreaterThan(50_000);
        }
        System.out.println("note: higher read ratios favor the double buffer; "
                + "write-heavy ratios favor the lock (see README).");
    }
}
