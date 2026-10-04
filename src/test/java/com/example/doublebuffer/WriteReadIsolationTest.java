package com.example.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Proves the core guarantee: a reader never observes a buffer that is being
 * written, not even a single torn field, while the writer hammers commits.
 */
class WriteReadIsolationTest {

    /** Payload whose invariant is {@code a == b}; a torn read sees {@code a != b}. */
    private static final class Pair {
        long a;
        long b;
    }

    private static final int COMMITS = 20_000;
    private static final int READERS = 4;

    @Test
    @Timeout(60)
    void readersNeverObservePartiallyWrittenBuffer() throws InterruptedException {
        DoubleBuffer<Pair> db = new DoubleBuffer<>(Pair::new, 1);
        AtomicBoolean failed = new AtomicBoolean();
        AtomicReference<String> detail = new AtomicReference<>();
        AtomicLong reads = new AtomicLong();
        AtomicBoolean writerDone = new AtomicBoolean();

        Thread writer = new Thread(() -> {
            for (long i = 1; i <= COMMITS && !failed.get(); i++) {
                long v = i;
                db.writeBatch(p -> {
                    // Two separate plain stores with a delay in between: any
                    // reader that could see this scratch buffer mid-write would
                    // observe a != b.
                    p.a = v;
                    Thread.onSpinWait();
                    p.b = v;
                });
            }
            writerDone.set(true);
        }, "writer");

        Thread[] readers = new Thread[READERS];
        CountDownLatch ready = new CountDownLatch(READERS);
        for (int r = 0; r < READERS; r++) {
            readers[r] = new Thread(() -> {
                ready.countDown();
                while (!writerDone.get() && !failed.get()) {
                    db.readVoid(p -> {
                        reads.incrementAndGet();
                        if (p.a != p.b && failed.compareAndSet(false, true)) {
                            detail.set("torn read: a=" + p.a + " b=" + p.b);
                        }
                    });
                }
            }, "reader-" + r);
            readers[r].start();
        }
        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        writer.start();
        writer.join(30_000);
        for (Thread reader : readers) {
            reader.join(30_000);
        }

        assertThat(failed.get()).as(detail.get()).isFalse();
        assertThat(reads.get()).isGreaterThan(0);
        assertThat(db.stats().switchCount()).isEqualTo(COMMITS);
        // Every read sees a fully committed pair.
        assertThat(db.<Long>read(p -> p.a)).isEqualTo((long) COMMITS);
        assertThat(db.<Long>read(p -> p.b)).isEqualTo((long) COMMITS);
    }

    @Test
    @Timeout(60)
    void consistencyHoldsWhenSlowReadersForceRecycling() throws InterruptedException {
        // retainedGenerations = 1 (only 2 buffers total) plus readers that
        // linger inside the read section force the writer to wait for
        // recycling; the invariant must still hold.
        DoubleBuffer<Pair> db = new DoubleBuffer<>(Pair::new, 1);
        AtomicBoolean failed = new AtomicBoolean();
        AtomicReference<String> detail = new AtomicReference<>();
        AtomicBoolean writerDone = new AtomicBoolean();

        Thread writer = new Thread(() -> {
            for (long i = 1; i <= COMMITS && !failed.get(); i++) {
                long v = i;
                db.writeBatch(p -> {
                    p.a = v;
                    p.b = v;
                });
            }
            writerDone.set(true);
        }, "writer");

        Thread[] readers = new Thread[READERS];
        for (int r = 0; r < READERS; r++) {
            readers[r] = new Thread(() -> {
                while (!writerDone.get() && !failed.get()) {
                    db.readVoid(p -> {
                        if (p.a != p.b && failed.compareAndSet(false, true)) {
                            detail.set("torn read: a=" + p.a + " b=" + p.b);
                        }
                        // Linger inside the read section so the writer has to
                        // wait for this generation to drain before reusing it.
                        LockSupport.parkNanos(500);
                    });
                }
            }, "reader-" + r);
            readers[r].start();
        }
        writer.start();
        writer.join(30_000);
        for (Thread reader : readers) {
            reader.join(30_000);
        }

        assertThat(failed.get()).as(detail.get()).isFalse();
        assertThat(db.stats().switchCount()).isEqualTo(COMMITS);
        assertThat(db.liveGenerations()).isEqualTo(2);
    }
}
