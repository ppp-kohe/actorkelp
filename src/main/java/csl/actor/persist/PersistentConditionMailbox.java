package csl.actor.persist;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.Message;
import csl.actor.MessageBundle;
import csl.actor.util.SampleTiming;

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
            if (res && logger != null) {
                logger.log(MailboxPersistableReplacement.logPersist, MailboxPersistableReplacement.logColorPersist,
                        "Mailbox needToPersist -> true : %,d > limit:%,d", size, sizeLimit);
            }
            return res;
        }

        @Override
        public boolean needToPersistInPersistLock(MailboxManageable mailbox, long size) {
            boolean res = size > sizeLimit;
            if (res && logger != null) {
                logger.log(MailboxPersistableReplacement.logPersist, MailboxPersistableReplacement.logColorPersist,
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
            if (log || res) {
                logger.log(MailboxPersistableReplacement.logPersist, MailboxPersistableReplacement.logColorPersist,
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
                if (log) {
                    logger.log(MailboxPersistableReplacement.logPersist, MailboxPersistableReplacement.logColorPersist, "updateCurrentSample timing=%,d lastSample=[%,d] <%s> total=%,d count=%,d -> %,d",
                            sampleTiming, sampleSize, logger.toStringLimit(data), sampleTotal.get(), sampleCount.get(), v);
                }
                return v;
            } catch (Exception ex) {
                if (log) {
                    logger.log(MailboxPersistableReplacement.logPersist, MailboxPersistableReplacement.logColorPersist, "updateCurrentSample timing=%,d lastSample=<%s> total=%,d count=%,d error:%s",
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

}
