package csl.actor.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SampleTiming {
    protected AtomicInteger count = new AtomicInteger();
    protected AtomicInteger next = new AtomicInteger();
    protected AtomicInteger last = new AtomicInteger();
    protected AtomicLong success = new AtomicLong();

    protected final int second;
    protected final int max;
    protected final int reset;
    protected final int shiftInc;

    /**
     * {0, 2^14,  2^15, 2^16, ..., 2^26, 2^17,... }
     */
    public SampleTiming() {
        this(0, 1 << 14, 1 << 26, 1 << 17, 1);
    }

    public SampleTiming(int secondAndReset, int max) {
        this(0, secondAndReset, max, secondAndReset, 1);
    }

    public SampleTiming(int second, int max, int reset, int shiftInc) {
        this(0, second, max, reset, shiftInc);
    }

    public SampleTiming(int cycle) {
        this(cycle, cycle, Integer.MAX_VALUE, cycle, 0);
    }

    /**
     * @param first    the first next count
     * @param second   the second next count
     * @param max      the limit of the next count: if the next overs the max, immediately set to the reset
     * @param reset    the reset count, must be greater than second
     * @param shiftInc the next multiply 2^shiftInc (0:x1, 1:x2, 2:x4, 3:x8, ...)
     */
    public SampleTiming(int first, int second, int max, int reset, int shiftInc) {
        this.next.set(Math.max(first, 0));
        this.second = Math.max(second, 0);
        this.max = Math.max(max, 0);
        this.reset = Math.max(Math.min(max, reset), second);
        this.shiftInc = Math.max(Math.min(30, shiftInc), 0);
    }

    public int getLast() {
        return last.get();
    }

    public int getCount() {
        return count.get();
    }

    public int getNext() {
        return next.get();
    }

    public boolean check() {
        return count.get() >= next.get();
    }

    /**
     * increment the count, check it reached to the next, obtain the success,
     *  enter synchronized,
     *   only one thread that successfully CAS the count with +1 can update the next,
     *   reset the count with -next.
     * @return true if the timing is sampled
     */
    public boolean next() {
        int lastCount = this.count.getAndIncrement();
        int lastNext = this.next.get();
        if (lastCount == lastNext) {
            long lastSuccess = this.success.get();
            synchronized (this) {
                int nextNext = nextCount(lastNext);
                boolean checkSync = this.success.compareAndSet(lastSuccess, lastSuccess + 1L);
                if (checkSync) {
                    this.next.set(nextNext);
                    this.count.addAndGet(-lastNext);
                    this.last.set(lastCount);
                }
                return checkSync;
            }
        } else {
            return false;
        }
    }

    private int nextCount(int nt) {
        long next = nt < second ? second : (((long) nt) << shiftInc);
        int ntSet;
        if (next > max) {
            ntSet = reset;
        } else {
            ntSet = (int) next;
        }
        return ntSet;
    }
}
