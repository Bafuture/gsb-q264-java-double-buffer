package com.example.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Batch writes mutate a private scratch buffer and publish exactly once;
 * intermediate state is invisible, failed batches are discarded, and
 * concurrent batches are serialized without lost commits.
 */
class BatchCommitTest {

    private static DoubleBuffer<List<Integer>> snapshotBuffer() {
        // Full-snapshot model: a reused list is cleared before each batch.
        return new DoubleBuffer<>(ArrayList::new, 1, List::clear);
    }

    @Test
    @Timeout(30)
    void readersOnlySeeStateBeforeOrAfterBatchNeverTheMiddle() throws InterruptedException {
        DoubleBuffer<List<Integer>> db = snapshotBuffer();
        db.writeBatch(l -> l.add(0));

        CountDownLatch halfDone = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        Thread writer = new Thread(() -> db.writeBatch(l -> {
            for (int i = 1; i <= 1_000; i++) {
                l.add(i);
                if (i == 500) {
                    halfDone.countDown();
                    await(proceed);
                }
            }
        }));
        writer.start();
        assertThat(halfDone.await(10, TimeUnit.SECONDS)).isTrue();

        // While the batch is half done on the private scratch buffer, readers
        // still observe the previous committed snapshot and only one switch
        // has happened so far.
        for (int i = 0; i < 100; i++) {
            assertThat(db.<Integer>read(List::size)).isEqualTo(1);
        }
        assertThat(db.stats().switchCount()).isEqualTo(1);

        proceed.countDown();
        writer.join(10_000);
        assertThat(writer.isAlive()).isFalse();
        assertThat(db.<Integer>read(List::size)).isEqualTo(1_000);
        assertThat(db.stats().switchCount()).isEqualTo(2);
    }

    @Test
    void oneBatchIsExactlyOneSwitchRegardlessOfChangeCount() {
        DoubleBuffer<List<Integer>> db = snapshotBuffer();
        db.writeBatch(l -> {
            for (int i = 0; i < 10_000; i++) {
                l.add(i);
            }
        });
        assertThat(db.<Integer>read(List::size)).isEqualTo(10_000);
        assertThat(db.stats().switchCount()).isEqualTo(1);
    }

    @Test
    void failedBatchIsDiscardedAndOldSnapshotStaysVisible() {
        DoubleBuffer<List<Integer>> db = snapshotBuffer();
        db.writeBatch(l -> l.add(42));

        assertThatThrownBy(() -> db.writeBatch(l -> {
            l.add(1);
            l.add(2);
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class).hasMessage("boom");

        assertThat(db.<Integer>read(List::size)).isEqualTo(1);
        assertThat(db.<Integer>read(l -> l.get(0))).isEqualTo(42);
        assertThat(db.stats().switchCount()).isEqualTo(1);
        // The discarded scratch buffer is returned to the pool and reusable.
        db.writeBatch(l -> l.add(7));
        assertThat(db.<Integer>read(List::size)).isEqualTo(1);
        assertThat(db.<Integer>read(l -> l.get(0))).isEqualTo(7);
        assertThat(db.liveGenerations()).isLessThanOrEqualTo(2);
    }

    @Test
    @Timeout(60)
    void concurrentBatchesAreSerializedAndAllCommitsLand() throws Exception {
        DoubleBuffer<AtomicReference<Integer>> db =
                new DoubleBuffer<>(() -> new AtomicReference<>(0), 1);
        int writers = 6;
        int batchesPerWriter = 500;
        AtomicInteger sequence = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers);
        AtomicReference<Throwable> error = new AtomicReference<>();

        for (int w = 0; w < writers; w++) {
            Thread t = new Thread(() -> {
                await(start);
                try {
                    for (int i = 0; i < batchesPerWriter; i++) {
                        int n = sequence.incrementAndGet();
                        db.writeBatch(box -> box.set(n));
                    }
                } catch (Throwable e) {
                    error.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            });
            t.start();
        }
        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(error.get()).isNull();

        int total = writers * batchesPerWriter;
        assertThat(db.stats().switchCount()).isEqualTo(total);
        int finalValue = db.read(AtomicReference::get);
        assertThat(finalValue).isBetween(1, total);

        // Every committed snapshot is a fully published, single-write value.
        for (int i = 0; i < 1_000; i++) {
            int v = db.read(AtomicReference::get);
            assertThat(v).isBetween(1, total);
        }
    }

    @Test
    void carryOverModelKeepsPreviousContentWithoutResetter() {
        // No resetter: with retention = 1 each generation carries the content of
        // its own previous use, so delta-style in-place updates accumulate on
        // each alternating buffer.
        DoubleBuffer<List<Integer>> db = new DoubleBuffer<>(ArrayList::new, 1);
        for (int i = 0; i < 10; i++) {
            int value = i;
            db.writeBatch(l -> l.add(value));
        }
        // Exactly half of the batches landed on each generation.
        assertThat(db.<Integer>read(List::size)).isEqualTo(5);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
