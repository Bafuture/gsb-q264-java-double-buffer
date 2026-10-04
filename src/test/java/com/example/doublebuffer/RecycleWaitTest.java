package com.example.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Old buffers are reused only after their readers drain; keeping more
 * generations lets the writer avoid waiting; generations are not leaked.
 */
class RecycleWaitTest {

    private static final class Box {
        int version;
    }

    @Test
    @Timeout(30)
    void writerBlocksOnRetentionExhaustionAndResumesAfterDrain() throws InterruptedException {
        DoubleBuffer<Box> db = new DoubleBuffer<>(Box::new, 1);
        db.writeBatch(b -> b.version = 1); // gen #1 active

        CountDownLatch readerInside = new CountDownLatch(1);
        CountDownLatch releaseReader = new CountDownLatch(1);
        Thread reader = new Thread(() -> db.readVoid(box -> {
            readerInside.countDown();
            await(releaseReader);
        }), "holder-of-gen1");
        reader.start();
        assertThat(readerInside.await(10, TimeUnit.SECONDS)).isTrue();

        db.writeBatch(b -> b.version = 2); // gen #2 active; gen #1 retired & held
        assertThat(db.stats().switchCount()).isEqualTo(2);

        // Third commit: only 2 generations exist and both are busy -> wait.
        CountDownLatch writerStarted = new CountDownLatch(1);
        CountDownLatch writerFinished = new CountDownLatch(1);
        Thread writer = new Thread(() -> {
            writerStarted.countDown();
            db.writeBatch(b -> b.version = 3);
            writerFinished.countDown();
        }, "blocked-writer");
        writer.start();
        assertThat(writerStarted.await(10, TimeUnit.SECONDS)).isTrue();

        Thread.sleep(200);
        assertThat(writer.isAlive()).isTrue();
        assertThat(db.stats().delayedRecycleCount()).isEqualTo(1);
        assertThat(db.stats().activeReaders()).isEqualTo(1);

        // Once the in-flight reader drains, reuse proceeds and commit lands.
        releaseReader.countDown();
        assertThat(writerFinished.await(10, TimeUnit.SECONDS)).isTrue();
        reader.join(10_000);
        assertThat(db.<Integer>read(b -> b.version)).isEqualTo(3);
        assertThat(db.stats().switchCount()).isEqualTo(3);
    }

    @Test
    @Timeout(30)
    void extraGenerationsLetWriterProceedWithoutWaiting() throws InterruptedException {
        DoubleBuffer<Box> db = new DoubleBuffer<>(Box::new, 3);
        db.writeBatch(b -> b.version = 0);

        CountDownLatch readerInside = new CountDownLatch(1);
        CountDownLatch releaseReader = new CountDownLatch(1);
        Thread reader = new Thread(() -> db.readVoid(box -> {
            readerInside.countDown();
            await(releaseReader);
        }));
        reader.start();
        assertThat(readerInside.await(10, TimeUnit.SECONDS)).isTrue();

        // One slow reader blocks the oldest generation; with 3 spare
        // generations the writer allocates one extra buffer and then keeps
        // reusing the two free ones, never waiting.
        for (int v = 1; v <= 12; v++) {
            int value = v;
            db.writeBatch(b -> b.version = value);
        }
        assertThat(db.stats().delayedRecycleCount()).isZero();
        assertThat(db.liveGenerations()).isEqualTo(3);
        assertThat(db.<Integer>read(b -> b.version)).isEqualTo(12);

        releaseReader.countDown();
        reader.join(10_000);
    }

    @Test
    void generationsAreRecycledNotLeakedWithNoSlowReaders() {
        DoubleBuffer<Box> db = new DoubleBuffer<>(Box::new, 1);
        for (int i = 0; i < 2_000; i++) {
            int value = i;
            db.writeBatch(b -> b.version = value);
        }
        assertThat(db.liveGenerations()).isEqualTo(2);
        assertThat(db.stats().switchCount()).isEqualTo(2_000);
        assertThat(db.stats().delayedRecycleCount()).isZero();
    }

    @Test
    void retainedGenerationsIsConfigurable() {
        DoubleBuffer<Box> db = new DoubleBuffer<>(Box::new, 5);
        assertThat(db.retainedGenerations()).isEqualTo(5);
        assertThat(db.liveGenerations()).isEqualTo(1);
        for (int i = 0; i < 60; i++) {
            int value = i;
            db.writeBatch(b -> b.version = value);
        }
        // No slow readers -> only the two ping-pong buffers are ever needed.
        assertThat(db.liveGenerations()).isEqualTo(2);
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
