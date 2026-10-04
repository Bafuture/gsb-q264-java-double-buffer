package com.example.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Switch semantics: a commit takes effect immediately for new readers, while
 * readers already inside the old buffer finish on the old data undisturbed.
 */
class SwitchSemanticsTest {

    private static final class Box {
        int version;
    }

    @Test
    void committedDataIsImmediatelyVisible() {
        DoubleBuffer<Box> db = new DoubleBuffer<>(Box::new, 1);
        assertThat(db.<Integer>read(b -> b.version)).isEqualTo(0);

        db.writeBatch(b -> b.version = 1);
        assertThat(db.<Integer>read(b -> b.version)).isEqualTo(1);

        db.writeBatch(b -> b.version = 2);
        assertThat(db.<Integer>read(b -> b.version)).isEqualTo(2);

        assertThat(db.stats().switchCount()).isEqualTo(2);
        assertThat(db.stats().activeReaders()).isZero();
    }

    @Test
    @Timeout(30)
    void inFlightReaderKeepsReadingOldBufferAfterSwitch() throws InterruptedException {
        DoubleBuffer<Box> db = new DoubleBuffer<>(Box::new, 2);
        db.writeBatch(b -> b.version = 1);

        CountDownLatch readerInside = new CountDownLatch(1);
        CountDownLatch switched = new CountDownLatch(1);
        CountDownLatch mayLeave = new CountDownLatch(1);
        AtomicReference<Box> heldByReader = new AtomicReference<>();
        AtomicReference<Integer> versionSeenAfterSwitch = new AtomicReference<>();

        Thread reader = new Thread(() -> db.readVoid(box -> {
            heldByReader.set(box);
            readerInside.countDown();
            await(switched);
            // The switch already happened, but this reader is still on the old
            // buffer and must keep seeing the old data.
            versionSeenAfterSwitch.set(box.version);
            mayLeave.countDown();
        }), "blocked-reader");
        reader.start();
        assertThat(readerInside.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(db.stats().activeReaders()).isEqualTo(1);

        db.writeBatch(b -> b.version = 2);
        switched.countDown();

        // New readers immediately see the new generation...
        assertThat(db.<Integer>read(b -> b.version)).isEqualTo(2);
        // ...while the in-flight reader still sees the old one.
        assertThat(mayLeave.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(versionSeenAfterSwitch.get()).isEqualTo(1);
        reader.join(10_000);

        // And the old buffer really is a different object than the new one.
        AtomicReference<Box> current = new AtomicReference<>();
        db.readVoid(current::set);
        assertThat(heldByReader.get()).isNotSameAs(current.get());
        assertThat(db.stats().activeReaders()).isZero();
    }

    @Test
    @Timeout(30)
    void activeReaderCountIsTracked() throws InterruptedException {
        DoubleBuffer<Box> db = new DoubleBuffer<>(Box::new, 1);
        int blocked = 3;
        CountDownLatch inside = new CountDownLatch(blocked);
        CountDownLatch release = new CountDownLatch(1);
        Thread[] readers = new Thread[blocked];
        for (int i = 0; i < blocked; i++) {
            readers[i] = new Thread(() -> db.readVoid(box -> {
                inside.countDown();
                await(release);
            }));
            readers[i].start();
        }
        assertThat(inside.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(db.stats().activeReaders()).isEqualTo(blocked);

        release.countDown();
        for (Thread reader : readers) {
            reader.join(10_000);
        }
        assertThat(db.stats().activeReaders()).isZero();
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
