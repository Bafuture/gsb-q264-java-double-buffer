package com.example.gsb.doublebuffer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 双缓冲（乒乓缓冲）切换组件。
 *
 * <p>任意时刻维护两类缓冲：
 * <ul>
 *   <li><b>写缓冲</b>：写者独占修改，读者永远看不到它；</li>
 *   <li><b>读缓冲</b>：通过 {@link AtomicReference} 发布，任意数量读者并发读。</li>
 * </ul>
 *
 * <p>写者在写缓冲上完成全部修改后调用一次切换（{@link #write} 或 {@link BatchWriter#commit}），
 * 原子地把写缓冲发布为新的读缓冲，原读缓冲进入"退役队列"。正在读旧缓冲的线程通过
 * 槽位上的读者计数把旧缓冲"钉住"：只要计数 &gt; 0，该缓冲就绝不会被写者复用，
 * 因此读者绝不可能读到被写入一半的缓冲。
 *
 * <p><b>旧缓冲复用</b>：退役队列中读者计数归零的槽位会被重新用作写缓冲（乒乓复用）；
 * 若所有退役槽位仍有读者，则当退役数量小于 {@code maxRetainedGenerations} 时新建
 * 一块缓冲（用内存换不阻塞），否则写者阻塞等待，直到某块旧缓冲的读者全部退出。
 *
 * <p>线程模型：所有写操作（{@link #write}、{@link #commit}）内部用写者监视器串行化，
 * 可被多个写线程调用；读操作完全无锁。{@link BatchWriter} 实例本身非线程安全，
 * 每个写者应持有自己的实例。
 *
 * @param <B> 用户缓冲类型
 */
public final class DoubleBuffer<B> {

    /** 一块缓冲外加钉住它的读者计数。 */
    private static final class Slot<B> {
        private final B buffer;
        private final AtomicInteger readers = new AtomicInteger();
        private volatile boolean retired;

        private Slot(B buffer) {
            this.buffer = buffer;
        }
    }

    /** 运行统计快照。 */
    public record Stats(long swapCount, long delayedRecycleCount, int activeReaders) {
    }

    /** 默认保留的退役缓冲代数：2 代以内允许新建缓冲而不是阻塞写者。 */
    public static final int DEFAULT_RETAINED_GENERATIONS = 2;

    private final Supplier<B> bufferFactory;
    private final int maxRetainedGenerations;

    /** 当前对读者发布的读缓冲。volatile 语义保证缓冲内容的可见性（写 happens-before 读）。 */
    private final AtomicReference<Slot<B>> published;

    /** 写者监视器：串行化所有写操作，并用于等待退役缓冲排空。 */
    private final Object writerMonitor = new Object();

    /** 退役缓冲队列（FIFO），仅在持锁（writerMonitor）时访问。 */
    private final Deque<Slot<B>> retired = new ArrayDeque<>();

    /** 写缓冲；为 null 表示刚完成一次切换，下次写入前需要重新获取。 */
    private Slot<B> writeSlot;

    private final AtomicInteger activeReaders = new AtomicInteger();
    private final AtomicLong swapCount = new AtomicLong();
    private final AtomicLong delayedRecycleCount = new AtomicLong();

    public DoubleBuffer(Supplier<B> bufferFactory) {
        this(bufferFactory, DEFAULT_RETAINED_GENERATIONS);
    }

    public DoubleBuffer(Supplier<B> bufferFactory, int maxRetainedGenerations) {
        if (maxRetainedGenerations < 0) {
            throw new IllegalArgumentException("maxRetainedGenerations must be >= 0, was " + maxRetainedGenerations);
        }
        this.bufferFactory = Objects.requireNonNull(bufferFactory, "bufferFactory");
        this.maxRetainedGenerations = maxRetainedGenerations;
        this.published = new AtomicReference<>(new Slot<>(bufferFactory.get()));
        this.writeSlot = new Slot<>(bufferFactory.get());
    }

    /**
     * 读取当前发布的缓冲。读期间该缓冲保证不会被写者复用。
     *
     * <p>实现要点：先对拿到的槽位读者计数 +1，再二次确认它仍是发布槽位。若在两步之间
     * 发生了切换，则放弃并重试。写者复用槽位前观察计数，因此要么读者计数已可见而等待，
     * 要么读者的二次确认失败而退出，两者不可能同时进入临界区。
     */
    public <R> R read(Function<? super B, ? extends R> reader) {
        while (true) {
            Slot<B> slot = published.get();
            slot.readers.incrementAndGet();
            activeReaders.incrementAndGet();
            if (published.get() == slot) {
                try {
                    return reader.apply(slot.buffer);
                } finally {
                    release(slot);
                }
            }
            release(slot);
        }
    }

    private void release(Slot<B> slot) {
        activeReaders.decrementAndGet();
        if (slot.readers.decrementAndGet() == 0 && slot.retired) {
            synchronized (writerMonitor) {
                writerMonitor.notifyAll();
            }
        }
    }

    /** 在写缓冲上执行一次修改，随后立即切换发布（单次写入版本）。 */
    public void write(Consumer<? super B> mutation) {
        synchronized (writerMonitor) {
            ensureWriteSlotLocked();
            mutation.accept(writeSlot.buffer);
            swapLocked();
        }
    }

    public BatchWriter batchWriter() {
        return new BatchWriter();
    }

    public Stats stats() {
        return new Stats(swapCount.get(), delayedRecycleCount.get(), activeReaders.get());
    }

    private void ensureWriteSlotLocked() {
        if (writeSlot == null) {
            writeSlot = acquireWriteSlotLocked();
        }
    }

    /**
     * 原子切换：旧读缓冲退役，写缓冲发布。
     * 发布之后才置空写缓冲，使下次写入必须先安全地获取一块干净缓冲。
     */
    private void swapLocked() {
        Slot<B> oldPublished = published.get();
        oldPublished.retired = true;
        retired.addLast(oldPublished);
        published.set(writeSlot);
        writeSlot = null;
        swapCount.incrementAndGet();
    }

    /**
     * 获取下一块写缓冲：优先复用退役队列中已排空的缓冲；否则在保留代数之内新建；
     * 再否则阻塞等待，直到某个退役缓冲的最后一个读者退出。
     */
    private Slot<B> acquireWriteSlotLocked() {
        Slot<B> drained = pollDrainedLocked();
        if (drained != null) {
            return drained;
        }
        if (retired.size() < maxRetainedGenerations) {
            return new Slot<>(bufferFactory.get());
        }
        delayedRecycleCount.incrementAndGet();
        while (true) {
            Slot<B> slot = waitForDrainLocked();
            if (slot != null) {
                return slot;
            }
        }
    }

    private Slot<B> pollDrainedLocked() {
        for (Iterator<Slot<B>> it = retired.iterator(); it.hasNext(); ) {
            Slot<B> slot = it.next();
            if (slot.readers.get() == 0) {
                it.remove();
                slot.retired = false;
                return slot;
            }
        }
        return null;
    }

    private Slot<B> waitForDrainLocked() {
        try {
            writerMonitor.wait();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for a retired buffer to drain", e);
        }
        return pollDrainedLocked();
    }

    /**
     * 批量写入器：多次 {@link #add} 的修改在本地累积，{@link #commit} 时一次性
     * 应用到写缓冲并只触发一次切换，避免频繁切换。实例非线程安全。
     */
    public final class BatchWriter {

        private final List<Consumer<? super B>> pending = new ArrayList<>();

        public BatchWriter add(Consumer<? super B> mutation) {
            Objects.requireNonNull(mutation, "mutation");
            pending.add(mutation);
            return this;
        }

        public int pendingCount() {
            return pending.size();
        }

        /** 把全部挂起修改应用到写缓冲并切换发布一次；无挂起修改时为空操作。 */
        public void commit() {
            if (pending.isEmpty()) {
                return;
            }
            List<Consumer<? super B>> batch = new ArrayList<>(pending);
            pending.clear();
            synchronized (writerMonitor) {
                ensureWriteSlotLocked();
                for (Consumer<? super B> mutation : batch) {
                    mutation.accept(writeSlot.buffer);
                }
                swapLocked();
            }
        }
    }
}
