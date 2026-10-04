package com.example.doublebuffer;

/**
 * Immutable snapshot of {@link DoubleBuffer} runtime statistics.
 *
 * @param switchCount         total number of buffer switches (commits) so far
 * @param delayedRecycleCount total number of times the writer had to wait for readers
 *                            of a retired buffer to drain before reusing it
 * @param activeReaders       number of readers currently inside a read section
 * @param liveGenerations     number of buffer generations currently allocated
 *                            (1 active + retired ones kept for reuse)
 */
public record BufferStats(long switchCount,
                          long delayedRecycleCount,
                          int activeReaders,
                          int liveGenerations) {

    @Override
    public String toString() {
        return "BufferStats{switchCount=" + switchCount
                + ", delayedRecycleCount=" + delayedRecycleCount
                + ", activeReaders=" + activeReaders
                + ", liveGenerations=" + liveGenerations + '}';
    }
}
