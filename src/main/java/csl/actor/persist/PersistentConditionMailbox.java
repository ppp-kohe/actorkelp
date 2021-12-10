package csl.actor.persist;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.Message;
import csl.actor.MessageBundle;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public interface PersistentConditionMailbox {
    ActorSystem.SystemLogger getLogger();

    boolean needToPersist(MailboxManageable mailbox, long size);

    default boolean needToPersistInPersistLock(MailboxManageable mailbox, long size) {
        return needToPersist(mailbox, size);
    }

    default boolean needToPersistInOffer(MailboxManageable mailbox, long size) {
        return needToPersist(mailbox, size);
    }

    default boolean needToPersistInPoll(MailboxManageable mailbox, long size) {
        return needToPersist(mailbox, size);
    }

    default boolean needToPersistInInComing(MailboxManageable mailbox, long size, boolean continuePersisting) {
        return continuePersisting || needToPersist(mailbox, size);
    }


    class PersistentConditionMailboxNever implements PersistentConditionMailbox {
        @Override
        public boolean needToPersist(MailboxManageable mailbox, long size) {
            return false;
        }

        @Override
        public ActorSystem.SystemLogger getLogger() {
            return new ActorSystemDefault.SystemLoggerErr();
        }
    }

    class PersistentConditionMailboxSizeLimit implements PersistentConditionMailbox {
        protected long sizeLimit;
        protected ActorSystem.SystemLogger logger;

        @Override
        public ActorSystem.SystemLogger getLogger() {
            return logger;
        }

        public PersistentConditionMailboxSizeLimit(long sizeLimit, ActorSystem.SystemLogger logger) {
            this.sizeLimit = sizeLimit;
            this.logger = logger;
        }

        @Override
        public boolean needToPersist(MailboxManageable mailbox, long size) {
            boolean res = size > sizeLimit;
            if (res && logger != null && MailboxPersistableReplacement.logPersist) {
                logger.log(true, MailboxPersistableReplacement.logColorPersist,
                        "Mailbox needToPersist -> true : %,d > limit:%,d", size, sizeLimit);
            }
            return res;
        }

        @Override
        public boolean needToPersistInPersistLock(MailboxManageable mailbox, long size) {
            boolean res = size > sizeLimit;
            if (res && logger != null && MailboxPersistableReplacement.logPersist) {
                logger.log(true, MailboxPersistableReplacement.logColorPersist,
                        "Mailbox needToPersist -> true : %,d > limit:%,d", size, sizeLimit);
            }
            return res;
        }

        @Override
        public boolean needToPersistInOffer(MailboxManageable mailbox, long size) {
            return size > sizeLimit && size > mailbox.getPreviousSizeOnMemory();
        }

        @Override
        public boolean needToPersistInPoll(MailboxManageable mailbox, long size) {
            return size > sizeLimit && size > mailbox.getPreviousSizeOnMemory();
        }
    }

    class PersistentConditionMailboxSampling implements PersistentConditionMailbox {
        protected volatile long sizeLimit;
        protected SampleTiming logTimingNeedToPersist = new SampleTiming();
        protected ActorSystem.SystemLogger logger;
        protected MailboxSampler sampler;

        public PersistentConditionMailboxSampling(long sizeLimit, ActorSystem.SystemLogger logger) {
            this(sizeLimit, logger, new MailboxSampler(null, logger));
        }

        public PersistentConditionMailboxSampling(long sizeLimit, ActorSystem.SystemLogger logger, MailboxSampler sampler) {
            this.sizeLimit = sizeLimit;
            this.logger = logger;
            this.sampler = sampler;
        }

        public void setSizeLimit(long sizeLimit) {
            this.sizeLimit = sizeLimit;
        }

        @Override
        public ActorSystem.SystemLogger getLogger() {
            return logger;
        }


        @Override
        public boolean needToPersist(MailboxManageable mailbox, long size) {
            long currentSample = sampler.currentSampleWithUpdating(mailbox);
            if (size > sizeLimit) {
                return needToPersistRuntime(size, sizeLimit, currentSample);
            }
            return false;
        }

        @Override
        public boolean needToPersistInInComing(MailboxManageable mailbox, long size, boolean continuePersisting) {
            if (continuePersisting) {
                sampler.currentSampleWithUpdating(mailbox);
                return true;
            } else {
                return needToPersist(mailbox, size);
            }
        }

        public boolean needToPersistRuntime(long size, long sizeLimit, long currentSample) {
            boolean log = logTimingNeedToPersist.next();
            long free = runtimeAvailableBytes();
            long estimated = (size + sizeLimit) * currentSample;
            boolean res = estimated > free;
            if ((log || res) && MailboxPersistableReplacement.logPersist) {
                logger.log(true, MailboxPersistableReplacement.logColorPersist,
                        "Mailbox needToPersist -> %s: size=%,d sizeLimit=%,d sample=%,d estimated=%,d free=%,d (%3.1f%%)",
                        res, size, sizeLimit, currentSample, estimated, free, free == 0 ? Double.POSITIVE_INFINITY : ((estimated / (double) free) * 100.0));
            }
            return res;
        }

        public long runtimeAvailableBytes() {
            Runtime rt = Runtime.getRuntime();
            return rt.maxMemory() - rt.totalMemory() + rt.freeMemory();
        }

    }

    class MailboxSampler {
        protected AtomicLong sampleTotal = new AtomicLong();
        protected AtomicLong sampleCount = new AtomicLong();
        protected SampleTiming logTimingUpdate = new SampleTiming();
        protected AtomicInteger sampleTiming = new AtomicInteger();
        public static int sampleTimingUpdate = 10_000;
        protected MailboxManageable mailbox;
        protected ActorSystem.SystemLogger logger;

        public MailboxSampler(MailboxManageable mailbox, ActorSystem.SystemLogger logger) {
            this.mailbox = mailbox;
            this.logger = logger;
        }

        public ActorSystem.SystemLogger getLogger() {
            return logger;
        }

        public int getSampleTiming() {
            return sampleTiming.get();
        }
        public long currentSampleWithUpdating() {
            return currentSampleWithUpdating(null);
        }

        public long currentSampleWithUpdating(MailboxManageable mailbox) {
            Message<?> msg;
            if (mailbox == null) {
                mailbox = this.mailbox;
            } else if (this.mailbox == null) {
                this.mailbox = mailbox;
            }
            if (mailbox != null &&
                    sampleTiming.getAndIncrement() % sampleTimingUpdate == 0 &&
                    !((msg = mailbox.getQueue().peek()) instanceof MailboxPersistableReplacement.MessageOnStorage) &&
                    msg != null && Message.unwrapHolder(msg.getData()) instanceof Serializable) {
                return updateCurrentSample(mailbox, msg);
            } else {
                return currentSample();
            }
        }

        public long updateCurrentSample(MailboxManageable mailbox, Message<?> msg) {
            int t = sampleTiming.get();
            if (t >= sampleTimingUpdate) {
                sampleTiming.set(0);
            }
            MessageBundle<?> bundle;
            if (msg instanceof MessageBundle<?> && !(bundle = (MessageBundle<?>) msg).getData().isEmpty()) {
                //take 10
                List<?> bundleList = bundle.getData();
                int bundleSize = bundleList.size();
                int bundleSkip = Math.max(bundleSize / 11, 1);
                long v = currentSample();
                for (int i = bundleSkip; i < bundleSize; i += bundleSize) {
                    v = updateCurrentSampleEach(mailbox, t,
                            new MessageBundle.MessageAccepted<>(bundle.getTarget(), bundleList.get(i)));
                }
                return v;
            } else {
                return updateCurrentSampleEach(mailbox, t, msg);
            }
        }

        public long updateCurrentSampleEach(MailboxManageable mailbox, int sampleTiming, Message<?> data) {
            boolean log = logTimingUpdate.next();
            try (Output output = new Output(4096)) { //a lengthy message causes a buffer overflow error
                mailbox.getSerializer().write(output, data);
                output.flush();
                long sampleSize = output.total();
                long v = sampleTotal.addAndGet(sampleSize) / sampleCount.incrementAndGet();
                if (log && MailboxPersistableReplacement.logPersist) {
                    logger.log(true, MailboxPersistableReplacement.logColorPersist, "updateCurrentSample timing=%,d lastSample=[%,d] <%s> total=%,d count=%,d -> %,d",
                            sampleTiming, sampleSize, logger.toStringLimit(data), sampleTotal.get(), sampleCount.get(), v);
                }
                return v;
            } catch (Exception ex) {
                if (log && MailboxPersistableReplacement.logPersist) {
                    logger.log(true, MailboxPersistableReplacement.logColorPersist, "updateCurrentSample timing=%,d lastSample=<%s> total=%,d count=%,d error:%s",
                            sampleTiming, logger.toStringLimit(data), sampleTotal.get(), sampleCount.get(), ex);
                }
                if (ex instanceof KryoException && ex.getMessage().contains("overflow")) {
                    return sampleTotal.addAndGet(4096) / sampleCount.incrementAndGet();
                } else {
                    //serialization failure
                    return currentSample();
                }
            }
        }

        public long currentSample() {
            long count = sampleCount.get();
            if (count == 0) {
                return 100;
            } else {
                return Math.max(16, sampleTotal.get() / sampleCount.get());
            }
        }
    }

    class SampleTiming {
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
         *
         * @param first the first next count
         * @param second the second next count
         * @param max the limit of the next count: if the next overs the max, immediately set to the reset
         * @param reset the reset count, must be greater than second
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
}
