package com.example.doublebuffer;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A lock-free, single-writer / multi-reader double buffer (ping-pong buffer)
 * with generational retention.
 *
 * <p>The writer mutates one buffer while readers read another. A switch is a
 * single atomic-reference swap, so freshly committed data becomes visible
 * immediately and readers never observe a half-written buffer. Retired buffers
 * are reused only after every reader that acquired them has finished; extra
 * generations can be kept (configured by {@code retainedGenerations}) so the
 * writer rarely has to wait even when readers are slow.
 *
 * <h2>Threading model</h2>
 * <ul>
 *   <li>at most one writer at a time between {@link #beginWrite()} and
 *       {@link #commit()}/{@link #abortWrite()} (the single-writer protocol);
 *       {@link #writeBatch(Consumer)} serializes writers internally and is safe
 *       to call from any thread;</li>
 *   <li>any number of concurrent readers via {@link #read(Function)};</li>
 *   <li>a thread must not read while it is writing.</li>
 * </ul>
 *
 * <h2>Reclamation protocol (hazard-pointer style)</h2>
 * Each buffer generation lives in a {@code Slot} with an atomic reader count
 * and a volatile {@code mutating} flag. A reader claims a slot by incrementing
 * the count and only then validates that the slot is still active and not
 * being mutated; the writer flips {@code mutating} before taking a slot back
 * and then re-checks the count. The two store-then-load pairs form a Dekker
 * style handshake, so at least one side always observes the other and a buffer
 * is never mutated while a reader holds it.
 *
 * @param <T> the mutable buffer type
 */
public final class DoubleBuffer<T> {

    /** A single buffer generation. */
    private static final class Slot<T> {
        final T buffer;
        final AtomicInteger readers = new AtomicInteger();
        /** Set by the writer before it starts mutating this slot. */
        volatile boolean mutating;

        Slot(T buffer) {
            this.buffer = buffer;
        }
    }

    private static final int SPIN_ITERATIONS = 256;
    private static final long PARK_NANOS = 1_000L;

    private final Supplier<? extends T> bufferFactory;

    /**
     * Optional reset action applied to a <em>reused</em> buffer before it is
     * handed to the writer. When {@code null}, a reused buffer still carries
     * the content written two generations ago (in-place delta model); when
     * provided, every batch starts from a clean buffer (full snapshot model).
     */
    private final Consumer<? super T> resetter;

    /** Maximum number of buffer generations alive at the same time. */
    private final int maxGenerations;

    /** The buffer readers currently see; swapped atomically on commit. */
    private volatile Slot<T> active;

    /**
     * Retired (previously active) slots, oldest first. Only the writer mutates
     * this deque; {@link #stats()} iterates it weakly consistently.
     */
    private final ConcurrentLinkedDeque<Slot<T>> retired = new ConcurrentLinkedDeque<>();

    /** Number of slots allocated so far (slots are never destroyed). */
    private volatile int liveGenerations;

    private volatile Slot<T> writeSlot;

    private volatile long switchCount;
    private volatile long delayedRecycleCount;

    /** The thread currently parked while waiting for readers to drain. */
    private volatile Thread parkedWriter;

    /** Serializes callers of {@link #writeBatch}; never touches readers. */
    private final Lock writeLock = new ReentrantLock();

    /**
     * @param bufferFactory       creates a fresh empty buffer
     * @param retainedGenerations how many spare generations to keep besides the
     *                            active one. {@code 1} is the classic two-buffer
     *                            ping-pong setup (at most 2 buffers alive);
     *                            larger values trade memory for throughput when
     *                            readers can be slow
     */
    public DoubleBuffer(Supplier<? extends T> bufferFactory, int retainedGenerations) {
        this(bufferFactory, retainedGenerations, null);
    }

    /**
     * @param bufferFactory       creates a fresh empty buffer
     * @param retainedGenerations how many spare generations to keep besides the
     *                            active one; {@code 1} is the classic two-buffer
     *                            ping-pong setup
     * @param resetter            applied to a reused buffer before it is handed
     *                            to the writer, or {@code null} to keep the
     *                            previous content (carry-over)
     */
    public DoubleBuffer(Supplier<? extends T> bufferFactory, int retainedGenerations,
                        Consumer<? super T> resetter) {
        if (bufferFactory == null) {
            throw new IllegalArgumentException("bufferFactory must not be null");
        }
        if (retainedGenerations < 1) {
            throw new IllegalArgumentException(
                    "retainedGenerations must be >= 1 (classic double buffer), got "
                            + retainedGenerations);
        }
        this.bufferFactory = bufferFactory;
        this.resetter = resetter;
        this.maxGenerations = retainedGenerations + 1;
        this.active = new Slot<>(bufferFactory.get());
        this.liveGenerations = 1;
    }

    // ------------------------------------------------------------------
    // Writer side
    // ------------------------------------------------------------------

    /**
     * Acquires a private scratch buffer and starts a new write session. The
     * returned buffer is never the one readers are looking at and is never
     * handed out twice before being committed. Single-writer protocol: must be
     * followed by exactly one {@link #commit()} or {@link #abortWrite()}.
     */
    public T beginWrite() {
        if (writeSlot != null) {
            throw new IllegalStateException("beginWrite() called twice without commit()");
        }
        Slot<T> slot = acquireWriteSlot();
        writeSlot = slot;
        return slot.buffer;
    }

    /**
     * Publishes the buffer of the current write session with a single atomic
     * swap. New readers immediately observe the new data; readers already
     * inside the old buffer finish undisturbed, and the old buffer is recycled
     * once they drain.
     */
    public void commit() {
        Slot<T> slot = writeSlot;
        if (slot == null) {
            throw new IllegalStateException("commit() without beginWrite()");
        }
        // Release-publishes every write made to the buffer during the session;
        // readers acquire the same slot before touching its payload.
        Slot<T> old = active;
        active = slot;
        slot.mutating = false;
        retired.addLast(old);
        writeSlot = null;
        switchCount++;
    }

    /**
     * Discards the current write session without switching. The scratch buffer
     * stays allocated and goes back to the retired pool for later reuse.
     */
    public void abortWrite() {
        Slot<T> slot = writeSlot;
        if (slot == null) {
            throw new IllegalStateException("abortWrite() without beginWrite()");
        }
        slot.mutating = false;
        retired.addLast(slot);
        writeSlot = null;
    }

    /**
     * Atomic batch write: mutate a private scratch buffer inside {@code writer}
     * and publish it once when the action returns. No intermediate state is
     * ever visible to readers, and a burst of changes costs exactly one switch.
     * If {@code writer} throws, the batch is discarded and the visible buffer
     * is left unchanged. Safe to call concurrently from multiple threads.
     */
    public void writeBatch(Consumer<? super T> writer) {
        writeLock.lock();
        try {
            T buffer = beginWrite();
            try {
                writer.accept(buffer);
            } catch (RuntimeException | Error e) {
                abortWrite();
                throw e;
            }
            commit();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Picks a scratch slot: a retired slot whose readers have drained
     * (preferring the oldest generation), otherwise a freshly allocated one
     * while below {@link #maxGenerations}; otherwise blocks until the oldest
     * generation drains. Returns the slot with {@code mutating} already set and
     * a verified reader count of zero.
     */
    private Slot<T> acquireWriteSlot() {
        // Single pass over the retired queue: busy slots are rotated to the
        // back in their original relative order.
        int seen = retired.size();
        for (int i = 0; i < seen; i++) {
            Slot<T> slot = retired.pollFirst();
            if (slot == null) {
                break;
            }
            if (slot.readers.get() == 0) {
                return claim(slot);
            }
            retired.addLast(slot);
        }
        if (liveGenerations < maxGenerations) {
            liveGenerations++;
            return new Slot<>(bufferFactory.get());
        }
        // Retention exhausted: the oldest generation must be reclaimed, which
        // means waiting for its in-flight readers to finish.
        delayedRecycleCount++;
        while (true) {
            Slot<T> oldest = retired.peekFirst();
            if (oldest != null && oldest.readers.get() == 0) {
                retired.pollFirst();
                return claim(oldest);
            }
            awaitIdle(() -> {
                Slot<T> slot = retired.peekFirst();
                return slot == null || slot.readers.get() != 0;
            });
        }
    }

    /**
     * Marks a slot as being mutated and confirms no reader slipped in between
     * the drain check and the mark. Stale claimants observe {@code mutating}
     * and back off, so this wait is always short. On interruption the slot is
     * returned to the retired pool so it is never lost.
     */
    private Slot<T> claim(Slot<T> slot) {
        slot.mutating = true;
        try {
            awaitIdle(() -> slot.readers.get() != 0);
        } catch (RuntimeException e) {
            slot.mutating = false;
            retired.addLast(slot);
            throw e;
        }
        if (resetter != null) {
            resetter.accept(slot.buffer);
        }
        return slot;
    }

    /**
     * Spins briefly, then parks in short slices until {@code busy} reports
     * false. Readers unpark the writer (best-effort) when a count drops to
     * zero, so the park timeout is only a safety net.
     */
    private void awaitIdle(BooleanSupplier busy) {
        while (busy.getAsBoolean()) {
            for (int spin = 0; spin < SPIN_ITERATIONS && busy.getAsBoolean(); spin++) {
                Thread.onSpinWait();
            }
            if (!busy.getAsBoolean()) {
                return;
            }
            parkedWriter = Thread.currentThread();
            // Re-check after publishing parkedWriter to avoid a missed wake-up.
            if (busy.getAsBoolean()) {
                LockSupport.parkNanos(PARK_NANOS);
            }
            parkedWriter = null;
            if (Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException(
                        "interrupted while waiting for readers to drain",
                        new InterruptedException());
            }
        }
    }

    // ------------------------------------------------------------------
    // Reader side
    // ------------------------------------------------------------------

    /**
     * Reads the currently active buffer. {@code reader} runs against a stable
     * snapshot: even if the writer switches and reuses generations
     * concurrently, the buffer passed in is never mutated until {@code reader}
     * returns.
     *
     * @return the value produced by {@code reader}
     */
    public <R> R read(Function<? super T, ? extends R> reader) {
        Slot<T> slot;
        // Claim first, validate after: a reader arriving in the tiny window
        // between swap and reclaim increments a retired slot, notices, and
        // retries on the new active one.
        while (true) {
            slot = active;
            slot.readers.incrementAndGet();
            if (slot == active && !slot.mutating) {
                break;
            }
            release(slot);
        }
        try {
            return reader.apply(slot.buffer);
        } finally {
            release(slot);
        }
    }

    /** Convenience read for actions that do not return a value. */
    public void readVoid(Consumer<? super T> reader) {
        read(buf -> {
            reader.accept(buf);
            return null;
        });
    }

    private void release(Slot<T> slot) {
        if (slot.readers.decrementAndGet() == 0) {
            Thread writer = parkedWriter;
            if (writer != null) {
                LockSupport.unpark(writer);
            }
        }
    }

    // ------------------------------------------------------------------
    // Introspection / statistics
    // ------------------------------------------------------------------

    /** Returns a best-effort snapshot of counters and live state. */
    public BufferStats stats() {
        int readers = active.readers.get();
        for (Slot<T> slot : retired) {
            readers += slot.readers.get();
        }
        Slot<T> writing = writeSlot;
        if (writing != null) {
            readers += writing.readers.get();
        }
        return new BufferStats(switchCount, delayedRecycleCount, readers, liveGenerations);
    }

    /** Number of buffer generations currently allocated (never shrinks). */
    public int liveGenerations() {
        return liveGenerations;
    }

    /** Configured spare generations kept besides the active buffer. */
    public int retainedGenerations() {
        return maxGenerations - 1;
    }
}
