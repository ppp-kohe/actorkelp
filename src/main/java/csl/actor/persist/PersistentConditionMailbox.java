package csl.actor.persist;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystem;
import csl.actor.Message;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@FunctionalInterface
public interface PersistentConditionMailbox {
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

    class PersistentConditionMailboxNever implements PersistentConditionMailbox {
        @Override
        public boolean needToPersist(MailboxManageable mailbox, long size) {
            return false;
        }
    }

    class PersistentConditionMailboxSizeLimit implements PersistentConditionMailbox {
        protected long sizeLimit;

        public PersistentConditionMailboxSizeLimit(long sizeLimit) {
            this.sizeLimit = sizeLimit;
        }

        @Override
        public boolean needToPersist(MailboxManageable mailbox, long size) {
            return size > sizeLimit;
        }

        @Override
        public boolean needToPersistInOffer(MailboxManageable mailbox, long size) {
            return size > sizeLimit && size > mailbox.getPreviousSize();
        }

        @Override
        public boolean needToPersistInPoll(MailboxManageable mailbox, long size) {
            return size > sizeLimit && size > mailbox.getPreviousSize();
        }
    }

    class PersistentConditionMailboxSampling implements PersistentConditionMailbox {
        protected long sizeLimit;
        protected AtomicLong sampleTotal = new AtomicLong();
        protected AtomicLong sampleCount = new AtomicLong();
        protected AtomicInteger sampleTiming = new AtomicInteger();
        protected SampleTiming logTimingUpdate = new SampleTiming();
        protected SampleTiming logTimingNeedToPersist = new SampleTiming();
        protected ActorSystem.SystemLogger logger;

        public PersistentConditionMailboxSampling(long sizeLimit, ActorSystem.SystemLogger logger) {
            this.sizeLimit = sizeLimit;
            this.logger = logger;
        }

        public ActorSystem.SystemLogger getLogger() {
            return logger;
        }

        public int getSampleTiming() {
            return sampleTiming.get();
        }

        @Override
        public boolean needToPersist(MailboxManageable mailbox, long size) {
            if (size > sizeLimit) {
                long currentSample = currentSampleWithUpdating(mailbox);
                return needToPersistRuntime(size, sizeLimit, currentSample);
            }
            return false;
        }

        public long currentSampleWithUpdating(MailboxManageable mailbox) {
            Message<?> msg;
            if (mailbox != null &&
                    sampleTiming.getAndIncrement() % 1_000 == 0 &&
                    !((msg = mailbox.getQueue().peek()) instanceof MailboxPersistableReplacement.MessageOnStorage) &&
                    msg != null && msg.getData() instanceof Serializable) {
                return updateCurrentSample(mailbox, msg);
            } else {
                return currentSample();
            }
        }

        public boolean needToPersistRuntime(long size, long sizeLimit, long currentSample) {
            boolean log = logTimingNeedToPersist.next();
            long free =  runtimeAvailableBytes();
            long estimated = (size + sizeLimit) * currentSample;
            boolean res = estimated > free;
            if (log || res) {
                logger.log(MailboxPersistableReplacement.logPersist, MailboxPersistableReplacement.logColorPersist,
                        "Mailbox needToPersist: size=%,d sizeLimit=%,d sample=%,d estimated=%,d free=%,d (%3.1f%%) -> %s",
                        size, sizeLimit, currentSample, estimated, free, free == 0 ? Double.POSITIVE_INFINITY : ((estimated / (double) free) * 100.0), res);
            }
            return res;
        }

        public long runtimeAvailableBytes() {
            Runtime rt = Runtime.getRuntime();
            return rt.maxMemory() - rt.totalMemory();
        }

        public long updateCurrentSample(MailboxManageable mailbox, Message<?> msg) {
            int t = sampleTiming.get();
            if (t >= 1000_000_000) {
                sampleTiming.set(0);
            }
            boolean log = logTimingUpdate.next();
            try (Output output = new Output(4096)) { //a lengthy message causes a buffer overflow error
                mailbox.getSerializer().write(output, msg);
                output.flush();
                long sampleSize = output.total();
                long v = sampleTotal.addAndGet(sampleSize) / sampleCount.incrementAndGet();
                if (log) {
                    logger.log(MailboxPersistableReplacement.logPersist, MailboxPersistableReplacement.logColorPersist, "updateCurrentSample timing=%,d lastSample=[%,d] <%s> total=%,d count=%,d -> %,d",
                            t, sampleSize, logger.toStringLimit(msg), sampleTotal.get(), sampleCount.get(), v);
                }
                return v;
            } catch (Exception ex) {
                if (log) {
                    logger.log(MailboxPersistableReplacement.logPersist, MailboxPersistableReplacement.logColorPersist, "updateCurrentSample timing=%,d lastSample=<%s> total=%,d count=%,d error:%s",
                            t, logger.toStringLimit(msg), sampleTotal.get(), sampleCount.get(), ex);
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

        protected int firstShift; //0 -> 2^firstShift -> 2^(firstShift+2) -> ...
        protected int maxShift;
        protected int resetShift; //0 -> ... -> 2^maxShift -> 2^resetShift

        public SampleTiming() {
            this(14, 30, 21);
        }
        public SampleTiming(int firstShift, int maxShift, int resetShift) {
            this.firstShift = Math.min(30, firstShift);
            this.maxShift = Math.min(30, maxShift);
            this.resetShift = Math.min(30, resetShift);
        }

        public boolean check() {
            return count.get() >= next.get();
        }

        public boolean next() {
            int max = 1 << maxShift;
            int t = count.getAndIncrement();
            if (t > max) {
                count.set(0);
            }

            int nt = next.get();
            if (t >= nt) {
                int next = nt <= 0 ? (1 << firstShift) : (nt << 2);
                while (next > 0 && next < Math.min(max, t)) {
                    next <<= 2;
                }
                if (next < 0 || next >= max) {
                    this.next.set(1 << resetShift);
                } else {
                    this.next.set(next);
                }
                return true;
            } else {
                return false;
            }
        }
    }
}
