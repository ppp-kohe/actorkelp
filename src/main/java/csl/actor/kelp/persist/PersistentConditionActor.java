package csl.actor.kelp.persist;

import csl.actor.Actor;
import csl.actor.ActorSystem;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.kelp.behavior.HistogramTree;
import csl.actor.kelp.behavior.MailboxKelp;
import csl.actor.persist.MailboxManageable;
import csl.actor.persist.MailboxPersistableReplacement;
import csl.actor.persist.PersistentConditionMailbox;
import csl.actor.persist.PersistentFileManager;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.util.ConfigBase;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public interface PersistentConditionActor {
    PersistentConditionMailbox getConditionMailbox();
    KeyHistogramsPersistable.PersistentConditionHistogram getConditionHistogram();
    MailboxKelp.ReducedSize getReducedSize();

    default void updateActor(Actor self) {}

    boolean logPersist = System.getProperty("csl.actor.persist.log", "true").equals("true");
    boolean logDebugPersist = System.getProperty("csl.actor.persist.debug", "false").equals("true");
    int logColorPersist = ActorSystem.systemPropertyColor("csl.actor.persist.color", 2);

    interface MemoryManagedActor {
        /**
         * @return the total memory usage ratio of all actors in the total memory; default is 1.0. it cannot be changed
         */
        default double memoryManageRatioTotalActor() {
            return 1.0;
        }
        default double memoryManageRatioMailbox() {
            return 33;
        }
        default double memoryManageRatioTree() {
            return memoryManageRatioTreeNode() + memoryManageRatioTreeValues();
        }
        default double memoryManageRatioTreeNode() {
            return 15;
        }
        default double memoryManageRatioTreeValues() {
            return 40;
        }
        default double memoryManageRatioSendingNext() {
            return 2;
        }
        default double memoryManageRatioFields() {
            return 10;
        }
    }

    static PersistentConditionActorDefault createSizeLimit(Actor initSelf, PersistentFileManager m, long onMemorySize, int reduceCheck, double reduceRatio,
                                                           KeyHistogramsPersistable.HistogramTreePersistableConfig config, ActorSystem.SystemLogger loggerBase) {
        SystemLoggerConditionActor logger = new SystemLoggerConditionActor(loggerBase, initSelf);
        return new PersistentConditionActorDefault(
                new PersistentConditionMailbox.PersistentConditionMailboxSizeLimit(onMemorySize, logger),
                new KeyHistogramsPersistable.PersistentConditionHistogramSizeLimit(config, logger),
                new ReducedSizeDefault(reduceCheck, reduceRatio, logger),
                logger);
    }

    static PersistentConditionActorDefault createNever(Actor initSelf, ActorSystem.SystemLogger logger, int reduceCheck, double reduceRatio) {
        SystemLoggerConditionActor loggerCond = new SystemLoggerConditionActor(logger, initSelf);
        return new PersistentConditionActorNever(new ReducedSizeDefault(reduceCheck, reduceRatio, logger), loggerCond);
    }


    class PersistentConditionActorDefault implements PersistentConditionActor {
        protected PersistentConditionMailbox mailbox;
        protected KeyHistogramsPersistable.PersistentConditionHistogram histogram;
        protected MailboxKelp.ReducedSize reduce;
        protected SystemLoggerConditionActor logger;

        public PersistentConditionActorDefault(PersistentConditionMailbox mailbox, KeyHistogramsPersistable.PersistentConditionHistogram histogram,
                                               MailboxKelp.ReducedSize reduce, SystemLoggerConditionActor logger) {
            this.mailbox = mailbox;
            this.histogram = histogram;
            this.reduce = reduce;
            this.logger = logger;
        }

        @Override
        public PersistentConditionMailbox getConditionMailbox() {
            return mailbox;
        }

        @Override
        public KeyHistogramsPersistable.PersistentConditionHistogram getConditionHistogram() {
            return histogram;
        }

        @Override
        public MailboxKelp.ReducedSize getReducedSize() {
            return reduce;
        }

        @Override
        public void updateActor(Actor self) {
            logger.setActor(self);
        }

    }

    class PersistentConditionActorNever extends PersistentConditionActorDefault {

        public PersistentConditionActorNever(MailboxKelp.ReducedSize reduce, SystemLoggerConditionActor logger) {
            super(new PersistentConditionMailbox.PersistentConditionMailboxNever(),
                    new KeyHistogramsPersistable.PersistentConditionHistogramSizeLimit(Integer.MAX_VALUE, 1, logger),
                    reduce, logger);
        }

        @Override
        public PersistentConditionMailbox getConditionMailbox() {
            return mailbox;
        }

        @Override
        public KeyHistogramsPersistable.PersistentConditionHistogram getConditionHistogram() {
            return histogram;
        }
    }

    Map<Object, LogTiming> updateActorLogHistory = new ConcurrentHashMap<>();
    class LogTiming {
        public Instant time = Instant.EPOCH;
        private Lock lock = new ReentrantLock();

        public boolean updateTime(Duration check) {
            if (lock.tryLock()) {
                try {
                    Instant now = Instant.now();
                    if (Duration.between(time, now).compareTo(check) >= 0) {
                        time = now;
                        return true;
                    } else {
                        return false;
                    }
                } finally {
                    lock.unlock();
                }
            } else {
                return false;
            }
        }
    }

    class PersistentConditionActorRuntime implements PersistentConditionActor {
        protected PersistConditionMailboxMemoryLimit mailbox;
        protected PersistentConditionHistogramMemoryLimit histogram;
        protected MailboxKelp.ReducedSize reduce;
        protected long actorMaxMemory;
        protected double totalActorMemoryRate;
        protected double memoryCheckedThreshold;
        protected double memoryPersistThreshold;
        protected int treeNodeTableSize;
        protected SystemLoggerConditionActor logger;
        protected PersistentConditionMailbox.MailboxSampler sampler;
        protected PersistentConditionMailbox.SampleTiming actorUpdateTiming = new PersistentConditionMailbox.SampleTiming(1 << 7, 1 << 22, 1 << 10, 1);
        protected RuntimeMemoryGuard memoryGuard;

        /**
         *
         * @param initSelf the initializing actor
         * @param totalActorMemoryRate maxMemory * totalActorMemoryRate / existingActors = actorMaxMemory (default: 0.7)
         * @param memoryCheckedThreshold actorMaxMemory * memoryCheckedThreshold = start_persisting_bytes (default: 0.8)
         * @param memoryPersistThreshold actorMaxMemory * memoryPersistThreshold = goal_persisting_bytes (default: 0.3)
         * @param treeNodeTableSize the treeLimit (default: 200)
         * @param treeReduceCheckSize the reduce-check threshold
         * @param loggerBase the logger
         */
        public PersistentConditionActorRuntime(Actor initSelf, double totalActorMemoryRate,
                                               double memoryCheckedThreshold,
                                               double memoryPersistThreshold,
                                               int treeNodeTableSize,
                                               int treeReduceCheckSize,
                                               ActorSystem.SystemLogger loggerBase) {
            this.totalActorMemoryRate = totalActorMemoryRate;
            this.memoryCheckedThreshold = memoryCheckedThreshold;
            this.memoryPersistThreshold = memoryPersistThreshold;
            this.treeNodeTableSize = treeNodeTableSize;
            this.logger = new SystemLoggerConditionActor(loggerBase, initSelf);
            this.sampler = new PersistentConditionMailbox.MailboxSampler(null, this.logger);
            this.memoryGuard = new RuntimeMemoryGuard(totalActorMemoryRate, memoryCheckedThreshold);
            mailbox = new PersistConditionMailboxMemoryLimit(10_000_000, this.logger, sampler, memoryGuard);
            histogram = new PersistentConditionHistogramMemoryLimit(memoryPersistThreshold / memoryCheckedThreshold, 100_000, 10_000_000,
                    this.logger, memoryGuard);
            reduce = new ReducedSizeSampling(treeReduceCheckSize, sampler, this.logger);
        }

        @Override
        public PersistentConditionMailbox getConditionMailbox() {
            return mailbox;
        }

        @Override
        public KeyHistogramsPersistable.PersistentConditionHistogram getConditionHistogram() {
            return histogram;
        }

        @Override
        public MailboxKelp.ReducedSize getReducedSize() {
            return reduce;
        }

        @Override
        public void updateActor(Actor self) { //timing : after all actors created and processed several samples
            logger.setActor(self);
            if (!actorUpdateTiming.next()) {
                return;
            }

            MailboxManageable mbox = null;
            if (self.getMailbox() instanceof MailboxKelp) {
                mbox = ((MailboxKelp) self.getMailbox()).getMailbox();
            } else if (self.getMailbox() instanceof MailboxManageable) {
                mbox = (MailboxManageable) self.getMailbox();
            }
            sampler.currentSampleWithUpdating(mbox);

            Runtime rt = Runtime.getRuntime();
            long maxMemory = rt.maxMemory();
            ActorSystem system = self.getSystem();
            if (system instanceof ActorSystemRemote) {
                system = ((ActorSystemRemote) system).getLocalSystem();
            }
            if (self instanceof MemoryManagedActor && system instanceof ActorSystemKelp.ActorSystemDefaultForKelp) {
                double existingActors = Math.max(0.000001, ((ActorSystemKelp.ActorSystemDefaultForKelp) system).getMemoryManagedActors());
                long prevActorMax = actorMaxMemory;
                double actorsRatio = ((MemoryManagedActor) self).memoryManageRatioTotalActor();
                actorMaxMemory = (long) (maxMemory * totalActorMemoryRate / existingActors * actorsRatio);

                ConfigBase.FormatAndArgs logFmt = (prevActorMax != actorMaxMemory ? logFormat(self) : null);
                if (logFmt != null) {
                    logFmt = logFmt.append(new ConfigBase.FormatAndArgs(
                            "persist updateActor: existActors=%,1.2f actorsRatio=%1.2f actorMaxMemory=%s totalMemRate=%1.1f%% checkThr=%1.1f%% lowThr=%1.1f%%",
                            existingActors,
                            actorsRatio,
                            bytesString(actorMaxMemory),
                            totalActorMemoryRate * 100.0,
                            totalActorMemoryRate * memoryCheckedThreshold * 100.0,
                            totalActorMemoryRate * memoryPersistThreshold * 100.0));
                }

                updateActorParts((MemoryManagedActor) self, logFmt);
            }
        }

        protected ConfigBase.FormatAndArgs logFormat(Actor self) {
            Object logKey = actorLogKey(self); //key : class-name, and self is unit
            if (logKey != null && updateActorLogHistory.computeIfAbsent(logKey, _k -> new LogTiming())
                    .updateTime(Duration.ofSeconds(5))) { //first time or 5 sec.
                return new ConfigBase.FormatAndArgs("");
            }
            return null;
        }

        public Object actorLogKey(Actor self) {
            return self.getClass().toString();
        }

        public void updateActorParts(MemoryManagedActor self, ConfigBase.FormatAndArgs logFmt) {
            double mbox = self.memoryManageRatioMailbox();

            int treeCount = 0;
            if (self instanceof ActorKelp<?>) {
                treeCount = ((ActorKelp<?>) self).getMailboxAsKelp().getEntrySize(); //the number of trees
            }

            double treeNode = (treeCount == 0 ? 0 : self.memoryManageRatioTreeNode());
            double treeValues = (treeCount == 0 ? 0 : self.memoryManageRatioTreeValues());
            double send = self.memoryManageRatioSendingNext();
            double fld = self.memoryManageRatioFields();
            double total = mbox + treeNode + treeValues + send + fld;

            long mboxMemory = (long) (actorMaxMemory * (mbox / total));
            long treeNodeMemory = (long) (actorMaxMemory * (treeNode / total));
            long treeValuesMemory = (long) (actorMaxMemory * (treeValues / total));
            long sendMemory = (long) (actorMaxMemory * (send / total));
            long fldMemory = (long) (actorMaxMemory * (fld / total));

            if (logFmt != null) {
                logFmt = logFmt.append(new ConfigBase.FormatAndArgs(
                        "%n    mbox=(%s %1.1f%%) trees=%,d treeNode=(%s %1.1f%%) treeVals=(%s %1.1f%%) send=(%s %1.1f%%) fld=(%s %1.1f%%)",
                        bytesString(mboxMemory), (mbox / total) * 100.0,
                        treeCount,
                        bytesString(treeNodeMemory), (treeNode / total) * 100.0,
                        bytesString(treeValuesMemory), (treeValues / total) * 100.0,
                        bytesString(sendMemory), (send / total) * 100.0,
                        bytesString(fldMemory), (fld / total) * 100.0));
            }

            logFmt = updateActorMailbox(self, mboxMemory, logFmt);
            logFmt = updateActorHistogram(self, treeCount, treeNodeMemory, treeValuesMemory, logFmt);

            if (logFmt != null) {
                logger.log(logPersist, logColorPersist, logFmt.format, logFmt.args);
            }
        }

        public ConfigBase.FormatAndArgs updateActorMailbox(MemoryManagedActor self, long mboxMemory, ConfigBase.FormatAndArgs logFmt) {
            long msgDataMemory = sampler.currentSampleWithUpdating();
            long msgSizeLimit = (long) ((mboxMemory * memoryCheckedThreshold) / msgDataMemory);
            mailbox.setSizeLimit(msgSizeLimit);
            if (logFmt != null) {
                logFmt = logFmt.append(new ConfigBase.FormatAndArgs("%n    msgSampleBytes=%,d -> msgSizeLimit=%,d", msgDataMemory, msgSizeLimit));
            }
            return logFmt;
        }

        public ConfigBase.FormatAndArgs updateActorHistogram(MemoryManagedActor self, int treeCount, long treeNodeMemory, long treeValuesMemory, ConfigBase.FormatAndArgs logFmt) {
            long msgDataMemory = sampler.currentSample();

            long tableBytes = histogram.estimatedTreeBytes(treeNodeTableSize);
            double leafBytes = histogram.estimatedTreeLeafBytes() * treeNodeTableSize * 3.5;
            double tableLeafRate = tableBytes / (tableBytes + leafBytes);

            long tableNodeSizeLimit = (treeCount == 0 ? 0 : (long) ((treeNodeMemory * memoryCheckedThreshold) * tableLeafRate / tableBytes / treeCount)); //leafFields and nodTreeFields
            long valuesLimit = (treeCount == 0 ? 0 : (long) ((treeValuesMemory * memoryCheckedThreshold) /  msgDataMemory / treeCount));

            if (logFmt != null) {
                logFmt = logFmt.append(new ConfigBase.FormatAndArgs(" nodesLimit=%,d valsLimit=%,d", tableNodeSizeLimit, valuesLimit));
            }

            histogram.setSizeLimit(tableNodeSizeLimit, valuesLimit);
            return logFmt;
        }
    }

    class RuntimeMemoryGuard {
        protected long totalActorMaxMemory;
        protected double memoryCheckedThreshold;
//        protected static PersistentConditionMailbox.SampleTiming timing =
//                new PersistentConditionMailbox.SampleTiming(0, 0);
//        protected static AtomicLong gcCount = new AtomicLong();
//        protected static AtomicLong gcSuccess = new AtomicLong();

        public RuntimeMemoryGuard(double totalActorMemoryRate, double memoryCheckedThreshold) {
            this.totalActorMaxMemory = (long) (Runtime.getRuntime().maxMemory() * totalActorMemoryRate);
            this.memoryCheckedThreshold = memoryCheckedThreshold;
        }

        public boolean hasSufficientFree() {
//            if (!hasSufficientFreeCheck()) {
//                if (timing.next()) {
//                    System.gc();
//                    long gcc = gcCount.incrementAndGet();
//                    boolean r = hasSufficientFreeCheck();
//                    if (r) {
//                        gcSuccess.incrementAndGet();
//                    }
//                    if (gcc % 1000 == 0) {
//                        System.err.println(report());
//                    }
//                    return r;
//                } else {
//                    return false;
//                }
//            } else {
//                return true;
//            }
//        }
//
//        protected boolean hasSufficientFreeCheck() {
            //no gc() call: slow
            return usage() < (long) (totalActorMaxMemory * memoryCheckedThreshold);
        }

        protected long usage() {
            Runtime rt = Runtime.getRuntime();
            return rt.totalMemory() - rt.freeMemory();
        }

        public String report() {
            long use = usage();
            long thr = (long) (totalActorMaxMemory * memoryCheckedThreshold);

            return String.format("mem: use:%s %s %s (totalActorMax:%s * %1.1f%%)",
                    bytesString(use), (use < thr ? "<" : ">="), bytesString(thr), bytesString(totalActorMaxMemory), memoryCheckedThreshold * 100.0
                        /*gcCount.get(), gcSuccess.get()*/);
        }
    }

    PersistentConditionActor.ReducedSizeDefault DEFAULT_REDUCED_SIZE = new PersistentConditionActor.ReducedSizeDefault();

    class ReducedSizeDefault implements MailboxKelp.ReducedSize {
        protected int reduceRuntimeCheckingThreshold;
        protected double reduceRuntimeRemainingBytesToSizeRatio;
        protected long traversalDelayMs = 300L;
        protected ActorSystem.SystemLogger logger;

        protected AtomicInteger reduceTimingOnMemory = new AtomicInteger();
        protected AtomicInteger reduceTimingPersist = new AtomicInteger();

        protected AtomicInteger reduceTimingTraversal = new AtomicInteger();
        protected Instant lastTraversal = Instant.now();
        protected Duration lastDuration = Duration.ZERO; //for logging

        protected PersistentConditionMailbox.SampleTiming logReduce = new PersistentConditionMailbox.SampleTiming(1 << 16, 1 << 30, 1 << 20, 2);

        public ReducedSizeDefault() {
            this(null);
        }

        public ReducedSizeDefault(ActorSystem.SystemLogger logger) {
            this(100_000, 0.003, logger);
        }

        public ReducedSizeDefault(int reduceRuntimeCheckingThreshold, double reduceRuntimeRemainingBytesToSizeRatio, ActorSystem.SystemLogger logger) {
            this.reduceRuntimeCheckingThreshold = reduceRuntimeCheckingThreshold;
            this.reduceRuntimeRemainingBytesToSizeRatio = reduceRuntimeRemainingBytesToSizeRatio;
            this.logger = logger;
        }

        @Override
        public boolean needToReduceForComplete(boolean allowPersist, long sizeChecked, int reduceReq) {
            AtomicInteger timing = (allowPersist ? reduceTimingPersist : reduceTimingOnMemory);
            boolean timingOk = false;
            int t = completeTiming();
            if (timing.incrementAndGet() >= t) {
                timing.set(0);
                timingOk = true;
            }
            int threshold = -1;
            boolean r = timingOk &&
                    sizeChecked >= (threshold = Math.max(reduceReq, reduceRuntimeCheckingThreshold));
            if (r && logger != null) {
                logger.log(logPersist, logColorPersist, "needToReduceComplete: timing:%,d %s:%,d >= threshold:%,d",
                        t, (allowPersist ? "allVals" : "valsOnMem"), sizeChecked, threshold);
            }
            return r;
        }

        protected int completeTiming() {
            return reduceRuntimeCheckingThreshold / 4;
        }

        @Override
        public int nextReducedSize(long size) {
            int consuming = (int) Math.min(Integer.MAX_VALUE, size);
            int rrt = reduceRuntimeCheckingThreshold;
            long aom = -1;
            if (consuming >= rrt) { //a large size, refer free memory size
                aom = availableOnMemoryMessages();
                //consuming = (int) Math.min(consuming, Math.max(rrt, aom));
                consuming = (int) Math.min(consuming, aom);
            }
            logReducedSize(size, aom, consuming);
            return consuming;
        }

        protected void logReducedSize(long size, long availableOnMemoryMessages, int consuming) {
            if (logger != null) {
                if (logReduce.next()) {
                    int n = logReduce.getLast();
                    logger.log(logPersist, logColorPersist, "nextReduceSize: timing:%,d vals:%,d - consuming:%,d -> after:%,d %s", n, size, consuming,
                            (size - consuming),
                            (availableOnMemoryMessages >= 0 ? String.format("(availableMsgsOnMem:%,d)", availableOnMemoryMessages) : ""));
                }
            }
        }

        public long availableOnMemoryMessages() {
            return (long) (freeMemory() * reduceRuntimeRemainingBytesToSizeRatio);
        }

        protected long freeMemory() {
            Runtime rt = Runtime.getRuntime();
            return rt.maxMemory() - rt.totalMemory() + rt.freeMemory();
        }

        @Override
        public boolean needToReduceForTraversal(boolean allowPersist, long treeSize, long treeSizeOnMemory, HistogramTree tree) {
            boolean timingOk = false;
            if (reduceTimingTraversal.incrementAndGet() >= traversalTiming()) {
                reduceTimingTraversal.set(0);
                timingOk = true;
            }
            return timingOk &&
                    needToReduceSize(allowPersist, treeSize, treeSizeOnMemory) &&
                    needToReduceTime() &&
                    (needToReduceMemory(allowPersist, treeSize, treeSizeOnMemory) ||
                     needToReduceTree(allowPersist, treeSize, treeSizeOnMemory, tree));
        }

        protected long traversalTiming() {
            return Math.min((long) reduceRuntimeCheckingThreshold * 2L, Integer.MAX_VALUE);
        }

        protected boolean needToReduceSize(boolean allowPersist, long treeSize, long treeSizeOnMemory) {
            return (allowPersist ? treeSize : treeSizeOnMemory) >= reduceRuntimeCheckingThreshold;
        }

        protected boolean needToReduceTime() {
            Instant now = Instant.now();
            Duration d = Duration.between(Instant.now(), lastTraversal);
            if (Duration.ofMillis(traversalDelayMs).compareTo(d) < 0) {
                lastTraversal = now;
                lastDuration = d;
                return true;
            } else {
                return false;
            }
        }

        protected boolean needToReduceMemory(boolean allowPersist, long treeSize, long treeSizeOnMemory) {
            long aom = availableOnMemoryMessages();
            boolean r = treeSizeOnMemory >= aom;
            if (r && logPersist) {
                logNeedToReduce(allowPersist, treeSize, treeSizeOnMemory,
                        String.format("&& valsOnMem:%,d >= availableMsgsOnMem:%,d", treeSizeOnMemory, aom));
            }
            return r;
        }

        protected boolean needToReduceTree(boolean allowPersist, long treeSize, long treeSizeOnMemory, HistogramTree tree) {
            boolean r = tree.needToReduce();
            if (r && logPersist) {
                logNeedToReduce(allowPersist, treeSize, treeSizeOnMemory, "&& tree.needToReduce=true");
            }
            return r;
        }

        protected void logNeedToReduce(boolean allowPersist, long treeSize, long treeSizeOnMemory, String rest) {
            if (logger != null) {
                String time = String.format("timing:%,d lastTrav:%s >%,dms", traversalTiming(), lastDuration, traversalDelayMs);
                if (allowPersist) {
                    logger.log(logColorPersist, "needToReduceTrav true: %s && allVals:%,d >= threshold:%,d (valsOnMem:%,d) %s",
                            time, treeSize, reduceRuntimeCheckingThreshold, treeSizeOnMemory, rest);
                } else {
                    logger.log(logColorPersist, "needToReduceTrav true: %s && valsOnMem:%,d >= threshold:%,d (allVals:%,d)  %s",
                            time, treeSizeOnMemory, reduceRuntimeCheckingThreshold, treeSize, rest);
                }
            }
        }

    }

    class ReducedSizeSampling extends ReducedSizeDefault {
        protected PersistentConditionMailbox.MailboxSampler sampler;

        public ReducedSizeSampling(int reduceRuntimeCheckingThreshold, PersistentConditionMailbox.MailboxSampler sampler, ActorSystem.SystemLogger logger) {
            super(reduceRuntimeCheckingThreshold, 1, logger);
            this.sampler = sampler;
        }

        @Override
        public long availableOnMemoryMessages() {
            return (freeMemory() / (sampler.currentSample() + 8L)); //CellArray
        }
    }

    class PersistConditionMailboxMemoryLimit implements PersistentConditionMailbox {
        protected volatile long sizeLimit;
        protected ActorSystem.SystemLogger logger;
        protected MailboxSampler sampler;
        protected RuntimeMemoryGuard memoryGuard;
        protected SampleTiming logTiming = new SampleTiming();

        public PersistConditionMailboxMemoryLimit(long sizeLimit, ActorSystem.SystemLogger logger, MailboxSampler sampler,
                                                  RuntimeMemoryGuard memoryGuard) {
            this.sizeLimit = sizeLimit;
            this.logger = logger;
            this.sampler = sampler;
            this.memoryGuard = memoryGuard;
        }

        public void setSizeLimit(long sizeLimit) {
            this.sizeLimit = sizeLimit;
        }

        @Override
        public ActorSystem.SystemLogger getLogger() {
            return logger;
        }

        @Override
        public boolean needToPersistInOffer(MailboxManageable mailbox, long size) {
            sampler.currentSampleWithUpdating(mailbox);

            return needToPersist(mailbox, size);
        }

        @Override
        public boolean needToPersist(MailboxManageable mailbox, long size) {
            if (size > sizeLimit) {
                if (memoryGuard.hasSufficientFree()) {
                    if (logTiming.next()) {
                        logger.log(logPersist, MailboxPersistableReplacement.logColorPersist, "persist mailbox: (cancel %s) timing:%,d %,d > limit:%,d",
                                memoryGuard.report(),
                                logTiming.getLast(),
                                size, sizeLimit);
                    }
                    return false;
                } else {
                    if (logTiming.next()) logger.log(logPersist, MailboxPersistableReplacement.logColorPersist, "persist mailbox: %,d > limit:%,d", size, sizeLimit);
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    class PersistentConditionHistogramMemoryLimit implements KeyHistogramsPersistable.PersistentConditionHistogram {
        protected MailboxManageable mailbox;
        protected double memoryPersistThresholdFromChecked;
        protected volatile long sizeValuesLimit;
        protected volatile long sizeNodesLimit;
        protected ActorSystem.SystemLogger logger;
        protected PersistentConditionMailbox.SampleTiming logTimingNeedToPersistFail = new PersistentConditionMailbox.SampleTiming(1 << 20, 1 << 30, 1 << 22, 2);
        protected PersistentConditionMailbox.SampleTiming logTimingNeedToPersistSuccess = new PersistentConditionMailbox.SampleTiming(4, 128);
        protected RuntimeMemoryGuard memoryGuard;

        public PersistentConditionHistogramMemoryLimit(double memoryPersistThresholdFromChecked, long sizeNodesLimit, long sizeValuesLimit,
                                                       ActorSystem.SystemLogger logger, RuntimeMemoryGuard memoryGuard) {
            this.memoryPersistThresholdFromChecked = memoryPersistThresholdFromChecked;
            this.logger = logger;
            this.sizeNodesLimit = sizeNodesLimit;
            this.sizeValuesLimit = sizeValuesLimit;
            this.memoryGuard = memoryGuard;
        }

        public void setSizeLimit(long sizeNodesLimit, long sizeValuesLimit) {
            this.sizeNodesLimit = sizeNodesLimit;
            this.sizeValuesLimit = sizeValuesLimit;
        }

        @Override
        public KeyHistogramsPersistable.HistogramPersistentOperation needToPersist(String callerInfo, HistogramTreePersistable tree) {
            long leafSizeOnMem = tree.getLeafSizeOnMemory();
            long nodeSizeOnMem = tree.getNodeSizeOnMemory();
            long treeValues = tree.getTreeSizeOnMemory();

            KeyHistogramsPersistable.HistogramPersistentOperation result;
            if (nodeSizeOnMem > sizeNodesLimit) {
                result =  KeyHistogramsPersistable.HistogramPersistentOperationType.FullTree;
            } else if (treeValues > sizeValuesLimit) {
                long sizeValuesGoal = (long) (sizeValuesLimit * memoryPersistThresholdFromChecked);
                if (Math.abs(treeValues - sizeValuesGoal) / (double) sizeValuesGoal < 0.1) {
                    result = KeyHistogramsPersistable.HistogramPersistentOperationType.FullTree;
                } else {
                    result = new KeyHistogramsPersistable.HistogramPersistentOperationTree(sizeValuesGoal);
                }
            } else {
                result = KeyHistogramsPersistable.HistogramPersistentOperationType.None;
            }

            boolean memChecked = false;
            String memInfo = "";
            if (!result.isNone()) {
                memChecked = true;
                if (memoryGuard.hasSufficientFree()) {
                    result = KeyHistogramsPersistable.HistogramPersistentOperationType.None;
                }
            }

            boolean log;
            int logTiming;
            if (!result.isNone()) {
                log = logTimingNeedToPersistSuccess.next();
                logTiming = logTimingNeedToPersistSuccess.getLast();
            } else {
                log = logTimingNeedToPersistFail.next();
                logTiming = logTimingNeedToPersistFail.getLast();
            }
            if (log && memChecked) {
                if (result.isNone()) {
                    memInfo = " (canceled " + memoryGuard.report() + ") ";
                } else {
                    memInfo = " (" + memoryGuard.report() + ") ";
                }
            }

            if (log) {
                logger.log(PersistentFileManager.logPersist, KeyHistogramsPersistable.logPersistColor,
                        "Histogram(%h) %s: needToPersist -> %s : %s timing:%,d treeVals=(%,d limit:%,d) nodes=(tbl:%,d leaf:%,d limit:%,d leafNZValuesRatio:%3.2f)",
                        System.identityHashCode(tree), callerInfo, result, memInfo, logTiming,
                        treeValues, sizeValuesLimit,
                        nodeSizeOnMem, leafSizeOnMem, sizeNodesLimit,
                        tree.leafSizeNonZeroToSizeRatio());
            }
            return result;
        }

        public long estimatedTreeBytes(int treeTableLimit) {
            return 60L + 8L * treeTableLimit;
        }
        public long estimatedTreeLeafBytes() {
            return 96L;
        }

        @Override
        public ActorSystem.SystemLogger getLogger() {
            return logger;
        }
    }

    static String bytesString(long b) {
        double size;
        String unit;
        if (b >= 1000_000_000_000_000L) {
            size = b / 1000_000_000_000_000.0;
            unit = "P";
        } else if (b >= 1000_000_000_000L) {
            size = b / 1000_000_000_000.0;
            unit = "T";
        } else if (b >= 1000_000_000L) {
            size = b / 1000_000_000.0;
            unit = "G";
        } else if (b >= 1000_000L) {
            size = b / 1000_000.0;
            unit = "M";
        } else if (b >= 1000L) {
            size = b / 1000.0;
            unit = "K";
        } else {
            size = b;
            unit = "";
        }
        return String.format("%2.2f%sB", size, unit);
    }

    class SystemLoggerConditionActor implements ActorSystem.SystemLogger {
        protected ActorSystem.SystemLogger logger;
        protected AtomicReference<Actor> actor = new AtomicReference<>();

        public SystemLoggerConditionActor(ActorSystem.SystemLogger logger, Actor self) {
            this.logger = logger;
            this.actor.set(self);
        }

        public void setActor(Actor actor) {
            if (this.actor.get() == null) {
                this.actor.set(actor);
            }
        }

        @Override
        public void log(int color, String fmt, Object... args) {
            ConfigBase.FormatAndArgs fa = format(fmt, args);
            logger.log(color, fa.format, fa.args);
        }

        @Override
        public void log(boolean flag, int color, String fmt, Object... args) {
            if (flag) {
                ConfigBase.FormatAndArgs fa = format(fmt, args);
                logger.log(color, fa.format, fa.args);
            }
        }

        protected ConfigBase.FormatAndArgs format(String fmt, Object... args) {
            ConfigBase.FormatAndArgs fa = new ConfigBase.FormatAndArgs(fmt, args)
                    .append(new ConfigBase.FormatAndArgs("%n    %s", memoryInfo()));
            if (actor.get() == null) {
                return fa;
            } else {
                return fa.append(new ConfigBase.FormatAndArgs("%n    %s", actor.get()));
            }
        }

        public String memoryInfo() {
            Runtime rt = Runtime.getRuntime();
            long max = rt.maxMemory();
            long free = max - rt.totalMemory() + rt.freeMemory();
            return String.format("memory: used:%s, free:%s (%2.1f%%)",
                    bytesString(rt.totalMemory() - rt.freeMemory()),
                    bytesString(free),
                    (free / (double) max) * 100.0);
        }

        @Override
        public void log(boolean flag, int color, Throwable ex, String fmt, Object... args) {
            if (flag) {
                ConfigBase.FormatAndArgs fa = format(fmt, args);
                logger.log(true, color, ex, fa.format, fa.args);
            }
        }

        @Override
        public ActorSystem.SystemLogToStringLimit toStringLimit(Object o) {
            return new ActorSystem.SystemLogToStringLimit(o) {
                @Override
                public String toString() {
                    return Objects.toString(o);
                }
            };
        }

        @Override
        public String toString() {
            return String.format("%s@%h(%s, actor=%h)",
                    getClass().getSimpleName(),
                    System.identityHashCode(this),
                    logger,
                    System.identityHashCode(actor));
        }
    }
}
