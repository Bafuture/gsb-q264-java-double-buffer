package com.example.gsb.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class DoubleBufferTest {

    /** 两个字段必须始终相等；写者故意在中间制造"写一半"的撕裂状态。 */
    private static final class Registers {
        private long left;
        private long right;
    }

    @Test
    @Timeout(30)
    void readersNeverObserveTornBufferWhileWriterMutates() throws Exception {
        // 只保留 1 代退役缓冲，强迫写者频繁复用旧缓冲，最大化复用压力。
        DoubleBuffer<Registers> db = new DoubleBuffer<>(Registers::new, 1);
        ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong writes = new AtomicLong();
        AtomicLong reads = new AtomicLong();

        ExecutorService pool = Executors.newFixedThreadPool(5);
        Future<?> writer = pool.submit(() -> {
            for (long v = 1; !stop.get(); v++) {
                final long value = v;
                db.write(reg -> {
                    reg.left = value;          // 撕裂中间态：left 已改、right 未改
                    Thread.yield();
                    reg.right = value;
                });
                writes.incrementAndGet();
            }
        });
        List<Future<?>> readers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            readers.add(pool.submit(() -> {
                while (!stop.get()) {
                    db.read(reg -> {
                        if (reg.left != reg.right) {
                            violations.add("torn read: left=" + reg.left + " right=" + reg.right);
                        }
                        reads.incrementAndGet();
                        return null;
                    });
                }
            }));
        }

        Thread.sleep(400);
        stop.set(true);
        writer.get(5, TimeUnit.SECONDS);
        for (Future<?> reader : readers) {
            reader.get(5, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        assertThat(violations).isEmpty();
        assertThat(writes.get()).isGreaterThan(0);
        assertThat(reads.get()).isGreaterThan(0);
        assertThat(db.stats().swapCount()).isEqualTo(writes.get());
        assertThat(db.stats().activeReaders()).isZero();
    }

    @Test
    @Timeout(30)
    void swapIsImmediatelyVisibleAndPinnedReaderKeepsOldBuffer() throws Exception {
        DoubleBuffer<Registers> db = new DoubleBuffer<>(Registers::new, 2);
        db.write(reg -> {
            reg.left = 1;
            reg.right = 1;
        });
        Long firstRead = db.read(reg -> reg.left);
        assertThat(firstRead).isEqualTo(1);

        // 一个读者进入旧缓冲后阻塞住，把旧缓冲钉住。
        CountDownLatch readerEntered = new CountDownLatch(1);
        CountDownLatch releaseReader = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<Long> pinned = pool.submit(() -> db.read(reg -> {
            readerEntered.countDown();
            awaitQuietly(releaseReader);
            return reg.left;
        }));
        assertThat(readerEntered.await(5, TimeUnit.SECONDS)).isTrue();

        // 切换后新读者立刻看到新数据。
        db.write(reg -> {
            reg.left = 2;
            reg.right = 2;
        });
        Long secondRead = db.read(reg -> reg.left);
        assertThat(secondRead).isEqualTo(2);
        assertThat(db.stats().activeReaders()).isEqualTo(1);

        // 被钉住的读者仍然读到旧缓冲的完整旧数据（旧缓冲未被复用篡改）。
        releaseReader.countDown();
        assertThat(pinned.get(5, TimeUnit.SECONDS)).isEqualTo(1);
        pool.shutdownNow();
        assertThat(db.stats().activeReaders()).isZero();
    }

    @Test
    @Timeout(30)
    void writerWaitsForPinnedReaderBeforeRecyclingRetiredBuffer() throws Exception {
        // 保留 0 代：没有任何缓冲可新建，写者必须等旧缓冲读者排空。
        DoubleBuffer<Registers> db = new DoubleBuffer<>(Registers::new, 0);
        db.write(reg -> {
            reg.left = 1;
            reg.right = 1;
        });

        CountDownLatch readerEntered = new CountDownLatch(1);
        CountDownLatch releaseReader = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<Long> pinned = pool.submit(() -> db.read(reg -> {
            readerEntered.countDown();
            awaitQuietly(releaseReader);
            return reg.left;
        }));
        assertThat(readerEntered.await(5, TimeUnit.SECONDS)).isTrue();

        // 第一次切换复用初始写缓冲退役后的空槽，不阻塞；耗尽所有空闲缓冲。
        db.write(reg -> {
            reg.left = 2;
            reg.right = 2;
        });
        Long afterFirstRecycle = db.read(reg -> reg.left);
        assertThat(afterFirstRecycle).isEqualTo(2);

        // 第二次切换时唯一的退役缓冲仍被钉住，写者必须阻塞等待。
        Future<?> blockedWriter = pool.submit(() -> db.write(reg -> {
            reg.left = 3;
            reg.right = 3;
        }));
        Thread.sleep(200);
        assertThat(blockedWriter.isDone()).isFalse();
        assertThat(db.stats().delayedRecycleCount()).isEqualTo(1);
        assertThat(db.stats().activeReaders()).isEqualTo(1);
        // 写者被阻塞期间，读者看到的仍是上一次发布的完整数据。
        Long whileWriterBlocked = db.read(reg -> reg.left);
        assertThat(whileWriterBlocked).isEqualTo(2);

        // 钉住的读者退出后，等待中的写者被唤醒并完成切换。
        releaseReader.countDown();
        assertThat(pinned.get(5, TimeUnit.SECONDS)).isEqualTo(1);
        blockedWriter.get(5, TimeUnit.SECONDS);
        Long afterDrain = db.read(reg -> reg.left);
        assertThat(afterDrain).isEqualTo(3);
        assertThat(db.stats().swapCount()).isEqualTo(3);
        pool.shutdownNow();
    }

    @Test
    @Timeout(30)
    void batchCommitAppliesAllMutationsInOneSwap() {
        DoubleBuffer<List<Integer>> db = new DoubleBuffer<>(ArrayList::new);
        DoubleBuffer<List<Integer>>.BatchWriter batch = db.batchWriter();
        batch.add(list -> list.add(1))
             .add(list -> list.add(2))
             .add(list -> list.add(3));
        assertThat(batch.pendingCount()).isEqualTo(3);

        // 提交前：读者看不到任何批量修改，也没有发生切换。
        List<Integer> beforeCommit = db.read(List::copyOf);
        assertThat(beforeCommit).isEmpty();
        assertThat(db.stats().swapCount()).isZero();

        batch.commit();

        // 提交后：三条修改作为整体一次性可见，且只切换了一次。
        List<Integer> afterCommit = db.read(List::copyOf);
        assertThat(afterCommit).containsExactly(1, 2, 3);
        assertThat(db.stats().swapCount()).isEqualTo(1);
        assertThat(batch.pendingCount()).isZero();

        // 空提交不触发切换。
        db.batchWriter().commit();
        assertThat(db.stats().swapCount()).isEqualTo(1);
    }

    @Test
    @Timeout(60)
    void concurrentWritersAndReadersStayConsistent() throws Exception {
        // 缓冲为 8 个槽位的数组，每次写入把所有槽位刷成同一个值；
        // 任何一次读都必须看到 8 个槽位完全相等，否则即为并发不一致。
        DoubleBuffer<long[]> db = new DoubleBuffer<>(() -> new long[8], 1);
        ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong writes = new AtomicLong();
        AtomicLong reads = new AtomicLong();

        ExecutorService pool = Executors.newFixedThreadPool(7);
        List<Future<?>> futures = new ArrayList<>();
        for (int w = 0; w < 3; w++) {
            final long base = (w + 1L) << 40;
            futures.add(pool.submit(() -> {
                for (long i = 1; !stop.get(); i++) {
                    final long value = base + i;
                    db.write(arr -> {
                        for (int k = 0; k < arr.length; k++) {
                            arr[k] = value;
                            if (k % 3 == 0) {
                                Thread.yield();
                            }
                        }
                    });
                    writes.incrementAndGet();
                }
            }));
        }
        for (int r = 0; r < 4; r++) {
            futures.add(pool.submit(() -> {
                while (!stop.get()) {
                    db.read(arr -> {
                        long first = arr[0];
                        for (long slot : arr) {
                            if (slot != first) {
                                violations.add("inconsistent array: first=" + first + " found=" + slot);
                            }
                        }
                        reads.incrementAndGet();
                        return null;
                    });
                }
            }));
        }

        Thread.sleep(400);
        stop.set(true);
        for (Future<?> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        assertThat(violations).isEmpty();
        assertThat(writes.get()).isGreaterThan(0);
        assertThat(reads.get()).isGreaterThan(0);
        assertThat(db.stats().swapCount()).isEqualTo(writes.get());
        assertThat(db.stats().activeReaders()).isZero();
        // 最终发布的数组自身也必须一致。
        Boolean finalConsistent = db.read(arr -> {
            long first = arr[0];
            for (long slot : arr) {
                if (slot != first) {
                    return false;
                }
            }
            return true;
        });
        assertThat(finalConsistent).isTrue();
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
