package csl.actor.kelp;

import csl.actor.*;
import csl.actor.cluster.ActorPlacement;
import csl.actor.kelp.behavior.*;
import csl.actor.kelp.persist.PersistentConditionActor;
import csl.actor.kelp.shuffle.*;
import csl.actor.persist.MailboxManageable;
import csl.actor.persist.MailboxPersistableIncoming;
import csl.actor.persist.PersistentFileManager;
import csl.actor.util.FileSplitter;
import csl.actor.util.PathModifier;
import csl.actor.util.Staging;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public abstract class ActorKelp<SelfType extends ActorKelp<SelfType>> extends ActorDefault
        implements Staging.StagingSupported, Staging.StagingPointMembers, ActorKelpFileReader<SelfType>, KelpStage<SelfType>, AutoCloseable,
                    PersistentConditionActor.MemoryManagedActor {
    protected ActorKelpInternalFactory internalFactory = ActorKelpInternalFactory.DEFAULT_FACTORY;
    protected ActorRef nextStage;
    protected FileSplitter fileSplitter;
    protected ConfigKelp config;
    protected boolean unit = false;
    protected int shuffleIndex = -1;
    protected int mergedCount = 1;
    protected Set<String> mergedActorNames = Collections.emptySet();
    protected Set<ActorRef> shuffleOriginals = new HashSet<>(1);
    protected List<KelpDispatcher.SelectiveDispatcher> selectiveDispatchers = null; //initialized by initBehavior
    protected List<KelpDispatcher.DispatchUnit> selfDispatcher;
    
    protected volatile ActorKelpStats.ActorKelpProcessingStats processingStats;
    protected ActorKelpStats.ActorKelpMessageHandledStats handledStats = new ActorKelpStats.ActorKelpMessageHandledStats();
    protected AtomicReference<Supplier<ActorKelpStats.ActorKelpStageEndStats>> stageEndStats = new AtomicReference<>(null);

    protected PersistentConditionActor memoryCondition;

    public static boolean logDebugKelp = System.getProperty("csl.actor.kelp.debug", "false").equals("true");
    public static int logDebugKelpColor = ActorSystem.systemPropertyColor("csl.actor.kelp.color", 126);

    public ActorKelp(ActorSystem system, String name, Mailbox mailbox, ActorBehavior behavior, ConfigKelp config) {
        super(system, name, null, null);
        if (config == null) {
            this.config = ConfigKelp.CONFIG_DEFAULT;
        } else {
            this.config = config;
        }
        if (system instanceof ActorKelpBuilder) {
            this.internalFactory = ((ActorKelpBuilder) system).internalFactory();
        }
        this.memoryCondition = initMemoryCondition();
        this.mailbox = mailbox;
        this.behavior = behavior;
    }

    public ActorKelpInternalFactory getInternalFactory() {
        return internalFactory;
    }

    public ActorKelp(ActorSystem system, String name, ConfigKelp config, Object constructionState) {
        this(system, name, null, null, config);
        initConstructionState(constructionState);
        initMailboxAndBehavior();
    }

    protected void initConstructionState(Object constructionState) {}

    public Object getConstructionState() {
        return null;
    }

    /**
     * the primary constructor of the class.
     *  {@link ActorKelpSerializable} restores an actor instance
     *    by calling the constructor which takes same arg-types of the constructor.
     * @param system the system
     * @param name the name
     * @param config the config
     */
    public ActorKelp(ActorSystem system, String name, ConfigKelp config) {
        this(system, name, null, null, config);
        initMailboxAndBehavior();
    }

    public ActorKelp(ActorSystem system, ConfigKelp config) {
        this(system, null, config);
        setNameRandom();
    }

    public ActorKelp(ActorSystem system) {
        this(system, null, (ConfigKelp) null);
        setNameRandom();
    }

    public void setNameRandom() {
        name = getClass().getSimpleName() + NAME_ID_SEPARATOR + UUID.randomUUID();
        system.register(this);
    }

    public void setNameInternal(String name) {
        if (this.name != null) {
            system.unregister(this);
        }
        this.name = name;
        system.register(this);
    }

    /**
     * @return true if the instance is not manually created and a shuffle entry
     */
    public boolean isUnit() {
        return unit;
    }

    /**
     * it can mark the actor as an processing unit, and
     *   at stage connection, it suppresses {@link #shuffle()}
     * <pre>
     *     pre.connects(new MyKelpActor().asUnit());
     * </pre>
     * @return this
     */
    @SuppressWarnings("unchecked")
    public SelfType asUnit() {
        setUnit(true);
        return (SelfType) this;
    }

    public void setUnit(boolean unit) {
        this.unit = unit;
    }

    public int getShuffleIndex() {
        return shuffleIndex;
    }

    public void setShuffleIndex(int shuffleIndex) {
        this.shuffleIndex = shuffleIndex;
    }

    public void setMergedCount(int mergedCount) {
        this.mergedCount = mergedCount;
    }

    public int getMergedCount() {
        return mergedCount;
    }

    public void setMergedActorNames(Set<String> mergedActorNames) {
        this.mergedActorNames = mergedActorNames;
    }

    public Set<String> getMergedActorNames() {
        return mergedActorNames;
    }

    public Set<ActorRef> getShuffleOriginals() {
        return shuffleOriginals;
    }

    ///////////// config


    public ConfigKelp getConfig() {
        return config;
    }

    public String getMailboxPath() {
        return config.mailboxPath;
    }

    public String getOutputDir() {
        return config.outputDir;
    }

    public boolean isPersist() {
        return config.persist;
    }

    public boolean isPersistRuntimeCondition() {
        return config.persistRuntimeCondition;
    }

    public long getMailboxOnMemorySize() {
        return config.mailboxOnMemorySize;
    }

    public long getSplitLength() {
        return config.splitLength;
    }

    public int getReduceRuntimeCheckingThreshold() {
        return config.reduceRuntimeCheckingThreshold;
    }

    public double getReduceRuntimeRemainingBytesToSizeRatio() {
        return config.reduceRuntimeRemainingBytesToSizeRatio;
    }

    public long getTraverseDelayTimeMs() {
        return config.traverseDelayTimeMs;
    }

    public long getPruneGreaterThanLeaf() {
        return (long) config.pruneGreaterThanLeafThresholdFactor * getMailboxThreshold();
    }

    public double getPruneLessThanNonZeroLeafRate() {
        return config.pruneLessThanNonZeroLeafRate;
    }

    public int getShuffleBufferSize() {
        return config.shuffleBufferSize;
    }

    public int getShufflePartitions() {
        return config.shufflePartitions;
    }

    public int getShuffleBufferSizeFile() {
        return config.shuffleBufferSizeFile;
    }

    public int getMailboxThreshold() {
        return config.mailboxThreshold;
    }
    public int getMailboxTreeSize() {
        return config.mailboxTreeSize;
    }

    public long getDispatcherRandomSeed() {
        return config.dispatcherRandomSeed;
    }

    public Random createDispatcherRandom() {
        return new Random(getDispatcherRandomSeed());
    }

    public long getFileLineWaitMs() {
        return config.fileLineWaitMs;
    }
    public long getFileLineWaitLines() {
        return config.fileLineWaitLines;
    }


    public double getTotalActorMemoryRate() {
        return config.totalActorMemoryRate;
    }
    public double getMemoryCheckedThreshold() {
        return config.memoryCheckedThreshold;
    }
    public double getMemoryPersistThreshold() {
        return config.memoryPersistThreshold;
    }

    ///////////////

    protected PersistentFileManager getPersistentFile() {
        return internalFactory.getPersistentFile(this);
    }

    protected void initMailboxAndBehavior() {
        memoryCondition = initMemoryCondition();
        mailbox = initMailbox();
        behavior = initBehavior();
        selfDispatcher = initSelfDispatcher();
    }

    protected PersistentConditionActor initMemoryCondition() {
        return internalFactory.initMemoryCondition(this, getPersistentFile());
    }

    public PersistentConditionActor getMemoryCondition() {
        return memoryCondition;
    }

    @Override
    protected Mailbox initMailbox() {
        PersistentFileManager m = getPersistentFile();
        MailboxManageable mbox = initMailboxDefault(m);
        return initMailboxKelp(mbox, initTreeFactory(m));
    }

    protected MailboxManageable initMailboxDefault(PersistentFileManager m) {
        return internalFactory.initMailboxDefault(this, m);
    }

    protected KeyHistograms initTreeFactory(PersistentFileManager m) {
        return internalFactory.initTreeFactory(this, m);
    }

    protected MailboxKelp initMailboxKelp(MailboxManageable m, KeyHistograms treeFactory) {
        return internalFactory.initMailboxKelp(this, m, treeFactory);
    }

    protected List<KelpDispatcher.DispatchUnit> initSelfDispatcher() {
        return Collections.singletonList(internalFactory.createShuffleEntryEmpty(this));
    }

    public MailboxKelp getMailboxAsKelp() {
        return (MailboxKelp) super.getMailbox();
    }

    @Override
    protected ActorBehaviorBuilderKelp behaviorBuilder() {
        return internalFactory.behaviorBuilder(this);
    }

    @Override
    protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilder builder) {
        return initBehavior((ActorBehaviorBuilderKelp) builder);
    }

    protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilderKelp builder) {
        return super.initBehavior(builder);
    }

    @Override
    public void setNextStage(ActorRef nextStage) {
        this.nextStage = internalFactory.setNextStage(this, this.nextStage, nextStage);
    }

    @Override
    public ActorRef nextStageActor() {
        return nextStage;
    }

    public void nextStageTell(Object data) {
        ActorRef next = nextStageActor();
        if (next != null) {
            next.tell(data);
        }
    }

    ///// processes


    @Override
    public void processMessage(Message<?> message) {
        internalFactory.processMessageBefore(this);
        memoryCondition.updateActor(this);
        processPrune();
        super.processMessage(message);
        processHistogram(message);
        internalFactory.processMessageAfter(this);
        if (message instanceof MessageBundle.MessageAccepted) {
            handledStats.increment();
        }
    }

    @Override
    public void processMessageSpecial(Message<?> message) {
        MailboxKelp.MessageDataControl ctrl = getMailboxAsKelp().getMessageControlDataFromMessage(message);
        if (ctrl != null) {
            ctrl.control(message, getMailboxAsKelp());
        }
        super.processMessage(message);
    }

    public void processMessageBundle(MessageBundle<Object> mb) {
        processMessageBundle(this, mb);
    }

    public static void processMessageBundle(ActorKelp<?> self, MessageBundle<Object> mb) {
        ActorKelpStats.ActorKelpProcessingStats prevStats = self.processingStats;
        ActorKelpStats.ActorKelpProcessingStatsMessageBundle stats =
                new ActorKelpStats.ActorKelpProcessingStatsMessageBundle(prevStats, mb.dataSize(), System.identityHashCode(mb));
        self.processingStats = stats;
        int i = 0;
        for (Object d : mb.getData()) {
            self.processMessage(new MessageBundle.MessageAccepted<>(self, d));  //MessageBundle is already accepted by Dispatcher
            stats.update(i);
            ++i;
        }
        self.processingStats = prevStats;
    }

    @Override
    public boolean processStageExited(Object taskKey) {
        if (reserveTraversalBeforeComplete()) {
            return false;
        } else {
            internalFactory.processStagingCompleted(this, taskKey);
            processStagingCompletedAndFlushImpl(taskKey);
            return true;
        }
    }

    public void processStagingCompletedAndFlushImpl(Object taskKey) {
        processStagingCompletedImpl(taskKey);
        flush();
    }

    public void processStagingCompletedImpl(Object taskKey) {
        //clear mailbox
        Mailbox defaultMailbox = getMailboxAsKelp().getMailbox();
        if (defaultMailbox instanceof MailboxPersistableIncoming) {
            ((MailboxPersistableIncoming) defaultMailbox).delete();
        }

        //clear histogram
        //TODO avoid multiple execution?
        getSystem().getLogger().log(logDebugKelp, logDebugKelpColor, "%s processStagingCompletedImpl(%s)",
                this, taskKey);

        getMailboxAsKelp()
                .processStageEnd(this, taskKey, getReducedSize());
    }

    public boolean reserveTraversalBeforeComplete() {
        HistogramEntry e = getMailboxAsKelp().remainingProcessesEntry();
        if (e != null) {
            e.reserveTraversal(this);
            return true;
        } else {
            return false;
        }
    }

    /**
     * clear zero-leaves :
     *     leafSize &gt; {@link #getPruneGreaterThanLeaf()} (200K keys)
     *     and nonZeroLeafSize &lt; {@link #getPruneLessThanNonZeroLeafRate()} (0.2: 20%).
     *     <p>
     *         Thus, clear at least 200K x 0.2 = 40_000 leaves.
     *         This means after at least 40K messages arrived.
     *     </p>
     *     <p>
     *         eventually() trees never be pruned, because of polling of values from leaves are basically happen at the stage end.
     *     </p>
     */
    public void processPrune() {
        getMailboxAsKelp().prune(
                getPruneGreaterThanLeaf(),
                getPruneLessThanNonZeroLeafRate());
    }

    public void processHistogram(Message<?> message) {
        getMailboxAsKelp().processTraversalReservedAndHistogram(this, getReducedSize(), message);
        //ActorBehaviorMatchKey1 immediately process and prune a single value
    }

    @Override
    public KelpStageGraphActor stageGraph() {
        try {
            return internalFactory.stageGraph(this);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    ///// file reader

    @Override
    public CompletableFuture<KelpStageGraphActor> startReading(String path, Instant startTime, Consumer<KelpStageGraphActor> setup) {
        try {
            KelpStageGraphActor a = stageGraph();
            setup.accept(a);
            return a.startAwaitTellFile(startTime, path);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void processFileSplit(FileSplitter.FileSplit split) {
        if (fileSplitter == null) {
            fileSplitter = FileSplitter.getWithSplitLength(getSplitLength(), PathModifier.getPathModifier(system));
        }
        try {
            if (split.getFileLength() == 0) {
                fileSplitter.splitIterator(split.getPath())
                        .forEachRemaining(this::processFileSplitForEachNext);
            } else {
                processFileSplitRead(split);
            }
            flush();
        } catch (Exception ex) {
            config.log(ex, "splitter=%s split=%s", fileSplitter, split);
        }
    }

    public void processFileSplitRead(FileSplitter.FileSplit split) throws Exception {
        Iterator<String> readingSplitIterator = fileSplitter.openLineIterator(split);
        ActorKelpStats.ActorKelpProcessingStats prevStats = processingStats;
        ActorKelpStats.ActorKelpProcessingStatsFileSplit stats = new ActorKelpStats.ActorKelpProcessingStatsFileSplit(prevStats, split);
        this.processingStats = stats;
        long lines = 0;
        long waitLines = getFileLineWaitLines();
        long waitMs = getFileLineWaitMs();
        Duration waitMsD = Duration.ofMillis(waitMs);
        Instant prevCheck = Instant.now();
        Instant startTime = prevCheck;
        Duration totalWait = Duration.ZERO;
        while (readingSplitIterator.hasNext()) {
            processFileSplitLine(readingSplitIterator.next());
            stats.updateRate(Duration.between(startTime, Instant.now()), totalWait, readingSplitIterator);
            if (waitLines > 0 && lines % waitLines == 0) {
                Instant now = Instant.now();
                Duration time = waitMsD.minus(Duration.between(prevCheck, now));
                prevCheck = now;
                if (!time.isNegative() && !time.isZero()) {
                    if (!time.minus(Duration.ofSeconds(10)).isNegative()) {
                        config.log("read waits > 10s: %s", time);
                    }
                    try {
                        int n = (time.toNanosPart() % 1000_000);
                        Thread.sleep(time.toMillis(), n);
                    } catch (IllegalArgumentException ex) {
                        System.err.println(ex + " : " + time);
                        ex.printStackTrace();
                    }
                    totalWait = totalWait.plus(time);
                }
                stats.updateRate(Duration.between(startTime, Instant.now()), totalWait, readingSplitIterator);
            }
            ++lines;
        }
        config.log("read finish: %s totalWait=%s: %s", split, totalWait, this);
        stats.updateRate(Duration.between(startTime, Instant.now()), totalWait, 1.0);
        this.processingStats = prevStats;
    }

    public void processFileSplitLine(String line) {
        processMessage(new MessageBundle.MessageAccepted<>(this, line));
    }

    public ActorKelpStats.ActorKelpMessageHandledStats getHandledStats() {
        return handledStats;
    }

    public ActorKelpStats.ActorKelpProcessingStats getProcessingStats() {
        return processingStats;
    }

    @Override
    public void flush() {
        internalFactory.flush(this);
    }

    protected void processFileSplitForEachNext(FileSplitter.FileSplit split) {
        if (nextStage != null) {
            nextStage.tell(split);
        } else {
            tell(split); //process by self
        }
    }

    /**
     * do {@link #flush()}.
     * a sub-class can override the method for disabling the actor at merging
     */
    @Override
    public void close() {
        flush();
        getMailboxAsKelp().terminateAfterSerialized();
        internalFactory.close(this);
    }

    @Override
    public String toStringContents() {
        return Stream.of(toStringContentsName(), toStringContentsWithoutName())
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining(" "));
    }

    public String toStringContentsWithoutName() {
        return Stream.of(toStringContentsStats(), toStringContentsMailboxTree(),
                        handledStats.toStringContents(), toStringContentsStageEnd())
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining(" "));
    }

    public String toStringContentsName() {
        if (name != null) {
            return Staging.shortenId(name);
        } else {
            return "";
        }
    }

    public String toStringContentsStats() {
        ActorKelpStats.ActorKelpProcessingStats stats = this.processingStats;
        if (stats == null) {
            return "";
        } else {
            return stats.toStringContents();
        }
    }

    public ActorKelpStats.ActorKelpMailboxTreeStats getMailboxTreeStats() {
        return new ActorKelpStats.ActorKelpMailboxTreeStats(this);
    }

    public String toStringContentsMailboxTree() {
        return getMailboxTreeStats().toStringContents();
    }

    public void setStageEndStats(Supplier<ActorKelpStats.ActorKelpStageEndStats> stageEndStats) {
        this.stageEndStats.set(stageEndStats);
    }

    public String toStringContentsStageEnd() {
        Supplier<ActorKelpStats.ActorKelpStageEndStats> s = stageEndStats.get();
        if (s != null) {
            return s.get().toStringContents();
        } else {
            return "";
        }
    }

    public ActorKelpStats.ActorKelpStageEndStats getStageEndStats() {
        Supplier<ActorKelpStats.ActorKelpStageEndStats> s = stageEndStats.get();
        if (s != null) {
            return s.get();
        } else {
            return null;
        }
    }

    /**
     * do {@link #flush()}.
     * a sub-class can override the method for ending the all stage exits.
     */
    @Override
    public void stageEnd() {
        flush();
    }

    ///// serializable

    public ActorKelpSerializable<SelfType> toSerializable() {
        return toSerializable(true);
    }

    @SuppressWarnings("unchecked")
    public ActorKelpSerializable<SelfType> toSerializable(boolean includeMailbox) {
        return internalFactory.toSerializable(this, includeMailbox);
    }

    /**
     * @return a serializable object which will be set to {@link ActorKelpSerializable#internalState}.
     *    The default impl. uses {@link ActorKelpSerializable#getBuilder(Class)}
     */
    public Object toInternalState() {
        try {
            return internalFactory.getInternalStateBuilder(this)
                    .toState(getPersistentFile().getSerializer(), this);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * @param data a serialized state created by {@link #toInternalState()}
     */
    public void setInternalState(Object data) {
        setInternalState(data, true);
    }

    public void setInternalState(Object data, boolean needToCopy) {
        try {
            internalFactory.getInternalStateBuilder(this)
                    .setState(getPersistentFile().getSerializer(), this, data, needToCopy);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public void setSerializable(ActorKelpSerializable<SelfType> state) {
        state.restore((SelfType) this);
    }

    @SuppressWarnings("unchecked")
    public void merge(ActorKelpSerializable<SelfType> another) {
        another.mergeTo((SelfType) this);
    }

    public void mergeInternalState(ActorKelpSerializable<SelfType> another, ActorKelpSerializable.MergingContext context, Object data) {
        mergeInternalState(context, data);
    }

    public void mergeInternalState(ActorKelpSerializable.MergingContext context, Object data) {
        try {
            internalFactory.getInternalStateBuilder(this)
                    .merge(context, this, data);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface TransferredState {
        MergerOpType mergeType() default MergerOpType.Default;
        /**
         * @return specified class for constructing merger operation for the target field.
         *   It uses <code>public static get({@link MergerOpType},{@link java.lang.reflect.Type})</code>
         *    and obtains {@link ActorKelpMergerFunctions.MergerFunction}.
         *   The default is {@link ActorKelpMergerFunctions.MergerFunctionDefault} and
         *     {@link ActorKelpMergerFunctions.MergerFunctionDefault#get(MergerOpType, Type)} just calls
         *       {@link ActorKelpMergerFunctions#getMergerFunction(MergerOpType, Type)}
         *         which employs a default operation specified by {@link #mergeType()}.
         */
        Class<? extends ActorKelpMergerFunctions.MergerFunction<?>> mergeFunc()
                default ActorKelpMergerFunctions.MergerFunctionDefault.class;
        /**
         * @return indicates the target field is not restored and the field is used only merging.
         */
        boolean mergeOnly() default false;
    }

    public enum MergerOpType {
        /** <ul>
         * <li>{@link ActorKelpMergerFunctions.Mergeable}: calling l.merge(r) </li>
         * <li>Map: {k=merge(lv,rv)}</li>
         * <li>otherwise: {@link #None} </li>
         * </ul>
         */
        Default,
        /** <ul>
         * <li>numbers: l+r </li>
         * <li>number arrays: {l[i]+r[i]} </li>
         * <li>{@link Set} or {@link List}: v.addAll(l); v.addAll(r);</li>
         * <li>Map: {k=merge(lv,rv)}</li>
         * </ul>
         */
        Add,
        /** <ul>
         *  <li>numbers: l*r </li>
         *  <li>number arrays: {l[i]*r[i]}</li>
         *  <li>Map: {k=merge(lv,rv)}</li>
         *  </ul>
         */
        Multiply,
        /** <ul>
         *  <li>numbers: mean(l,r) e.g. for int, {@link ActorKelpSerializable.MergingContext#mean(int, int)}</li>
         *  <li>number arrays: {mean(l[i],r[i])}</li>
         *  <li>Map: {k=merge(lv,rv)}</li>
         *  </ul>
         */
        Mean,
        /** <ul>
         *  <li>numbers: max(l,r) e.g. {@link Math#max(int, int)}</li>
         *  <li>number arrays: {max(l[i],r[i])}</li>
         *  <li>{@link Comparable}: compareTo(l,r) and select &gt;0 </li>
         *  <li>Map: {k=merge(lv,rv)}</li>
         *  </ul>
         */
        Max,
        /** <ul>
         *  <li>numbers: min(l,r) e.g. {@link Math#min(int, int)}</li>
         *  <li>number arrays: {min(l[i],r[i])}</li>
         *  <li>{@link Comparable}: compareTo(l,r) and select &lt;0 </li>
         *  <li>Map: {k=merge(lv,rv)}</li>
         *  </ul>
         */
        Min,
        /**
         * l or r if l==null
         */
        None;
    }


    ////// reducedSize

    public MailboxKelp.ReducedSize getReducedSize() {
        return memoryCondition.getReducedSize();
    }

    //// shuffle

    public ActorPlacement getPlacement() {
        return ActorPlacement.getPlacement(getSystem());
    }

    public ActorRefShuffleKelp<SelfType> shuffle() {
        return shuffle(Integer.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<SelfType> getActorType() {
        return (Class<SelfType>) getClass();
    }

    @SuppressWarnings("unchecked")
    public ActorRefShuffleKelp<SelfType> shuffle(int bufferSizeMax) {
        ActorKelpStateSharing.StateSharingActor sharingActor = internalFactory.createSharingActor(this);
        shuffleOriginals.add(sharingActor);

        ActorKelpSerializable<SelfType> serialized = toSerializable(false);
        ActorPlacement place = getPlacement();

        int partitions = getShufflePartitions();
        int bufferSize = Math.min(bufferSizeMax, getShuffleBufferSize());

        List<ActorRef> entries =
                IntStream.range(0, partitions)
                        .mapToObj(i -> createAndPlaceShuffle(place, serialized, i))
                        .collect(Collectors.toList());

        sharingActor.getMembers().addAll(entries);

        return createShuffle(
                getSystem(),
                entries,
                getSelectiveDispatchers().stream()
                    .map(KelpDispatcher.SelectiveDispatcher::copy)
                    .collect(Collectors.toList()),
                bufferSize,
                (Class<SelfType>) getClass());
    }

    protected ActorRefShuffleKelp<SelfType> createShuffle(ActorSystem system, List<ActorRef> entries,
                                                          List<KelpDispatcher.SelectiveDispatcher> extractorsAndDispatchers, int bufferSize,
                                                          Class<SelfType> actorType) {
        return internalFactory.createShuffle(this, entries, extractorsAndDispatchers, bufferSize);
    }



    @Deprecated
    @SuppressWarnings("unchecked")
    public List<ActorKelpFunctions.KeyExtractor<?,?>> getKeyExtractors() {
        return getMailboxAsKelp().getEntries().stream()
                .map(HistogramEntry::getProcessor)
                .filter(ActorBehaviorKelp.ActorBehaviorMatchKey.class::isInstance)
                .map(ActorBehaviorKelp.ActorBehaviorMatchKey.class::cast)
                .flatMap(p -> ((List<ActorKelpFunctions.KeyExtractor<?,?>>) p.getKeyExtractors()).stream())
                .collect(Collectors.toList());
    }

    public void initSelectiveDispatchers(List<ActorBehaviorKelp.SelectiveDispatcherFactory> extractorsAndDispatchers) {
        this.selectiveDispatchers = extractorsAndDispatchers.stream()
                .map(e -> e.createSelectiveDispatcher(this))
                .collect(Collectors.toList());
    }

    public KelpDispatcher createDispatcher(ActorKelpFunctions.DispatcherFactory factory) {
        if (factory != null) {
            return factory.create();
        } else {
            return dispatchShuffle().create();
        }
    }

    public List<KelpDispatcher.SelectiveDispatcher> getSelectiveDispatchers() {
        return selectiveDispatchers;
    }

    protected ActorRef createAndPlaceShuffle(ActorPlacement place, ActorKelpSerializable<SelfType> serialized, int i) {
        try {
            Actor a = serialized.restoreShuffle(system, i, getConfig());
            if (place != null) {
                return place.place(a);
            } else {
                return a;
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * called when creation as a shuffle member
     */
    public void initRestoreShuffle() { }

    /**
     * called when creation as a merged state
     */
    public void initRestoreMerge() { }

    /**
     * called when creation as a placement process
     */
    public void initRestorePlace() { }

    public void initRestorePlaceLocal() {
        initRestorePlace();
    }

    @Override
    public <NextActorType extends Actor> KelpStage<NextActorType> connects(Class<NextActorType> actorType, ActorRef ref) {
        ref = ActorRefShuffle.connectStageInitialActor(system, ref, getShuffleBufferSizeMax());
        if (ref instanceof ActorRefShuffle) {
            ref = ((ActorRefShuffle) ref).use();
        }
        setNextStage(ref);
        return toKelpStage(system, actorType, ref, getShuffleBufferSize());
    }

    public int getShuffleBufferSizeMax() {
        return Integer.MAX_VALUE;
    }

    @SuppressWarnings("unchecked")
    public static <NextActorType extends Actor> KelpStage<NextActorType> toKelpStage(ActorSystem system,
                                                                                     Class<NextActorType> actorType,
                                                                                     ActorRef ref, int bufferSizeForSingle) {
        if (ref instanceof ActorRefShuffleKelp<?>) {
            return (KelpStage<NextActorType>) ref;
        } else {
            return new ActorRefShuffleSingle<>(system, actorType, ref, bufferSizeForSingle);
        }
    }

    @Override
    public List<ActorRef> getMemberActors() {
        return Collections.singletonList(this);
    }

    @Override
    public List<? extends ActorRef> getStagingSubjectActors() {
        return getMemberActors();
    }

    @Override
    public boolean isNonSubject() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SelfType merge() {
        return (SelfType) this;
    }

    @Override
    public SelfType getMergedState() {
        try (ActorKelpMergerSharing<SelfType> m = new ActorKelpMergerSharing<>(system, config)) {
            return m.mergeToLocalSync(Collections.singletonList(this));
        }
    }

    @Override
    public <StateType> StateType merge(BiFunction<ActorSystem, ConfigKelp, ? extends ActorKelpStateSharing<SelfType, StateType>> factory) {
        try (ActorKelpStateSharing<SelfType, StateType> m = factory.apply(system, new ConfigKelp())) {
            return m.mergeSync(Collections.singletonList(this));
        }
    }

    public void shareStateToOtherShuffleMembers() {
        //asynchronous: it needs to stop self processing
        getShuffleOriginals().forEach(m ->
                m.tell(new ActorKelpStateSharing.StateSharingRequest(this, ActorKelpMergerSharing.class)));
    }

    @Override
    public void forEach(Consumer<KelpDispatcher.DispatchUnit> task) {
        task.accept(selfDispatcher.get(0));
    }

    @Override
    public List<? extends KelpDispatcher.DispatchUnit> getDispatchUnits() {
        return selfDispatcher;
    }

    @Override
    public KelpDispatcher.DispatchUnit getDispatchUnit(int index) {
        return index == 0 ? selfDispatcher.get(0) : null;
    }

    public static class SelfDispatcher implements KelpDispatcher.DispatchUnit {
        protected ActorKelp<?> self;

        public SelfDispatcher() {}

        public SelfDispatcher(ActorKelp<?> self) {
            this.self = self;
        }

        @Override
        public void tell(Object data) {
            MessageBundle.MessageAccepted<?> accepted = new MessageBundle.MessageAccepted<>(
                    self, data);
            if (self.getSystem().isSpecialMessageData(data)) {
                self.processMessageSpecial(accepted);
            } else {
                self.processMessage(accepted);
            }
        }

        @Override
        public void tellMessage(Message<?> message) {
            MessageBundle.MessageAccepted<?> accepted = new MessageBundle.MessageAccepted<>(
                            message.getTarget(), message.getData());
            if (self.getSystem().isSpecialMessage(message)) {
                self.processMessageSpecial(accepted);
            } else {
                self.processMessage(accepted);
            }
        }

        @Override
        public int getIndex() {
            return -1;
        }

        @Override
        public void flush() {}

        @Override
        public boolean hasRemainingMessage() {
            return false;
        }

        @Override
        public List<? extends ActorRef> getStagingSubjectActors() {
            return Collections.singletonList(self);
        }
    }

    ///// utilities for builder

    public static <T> ActorKelpFunctions.KeyExtractorFunction<T,T> identity() {
        return ActorKelpFunctions.KeyExtractorFunction.identity();
    }

    public ActorKelpFunctions.DispatcherFactory dispatchAll() {
        return internalFactory.dispatchAll(this);
    }

    public ActorKelpFunctions.DispatcherFactory dispatchShuffle() {
        return internalFactory.dispatchShuffle(this);
    }

    public ActorKelpFunctions.DispatcherFactory dispatchRandomOne() {
        return internalFactory.dispatchRandomOne(this);
    }

    /**
     * @return scatter messages to all members with random poisson(1) dist.
     */
    public ActorKelpFunctions.DispatcherFactory dispatchRandomPoison1() {
        return internalFactory.dispatchRandomPoison1(this);
    }

    public ActorKelpFunctions.DispatcherFactory dispatchRandomOne(Random random) {
        return internalFactory.dispatchRandomOne(this, random);
    }

    public ActorKelpFunctions.DispatcherFactory dispatchRandomPoison1(Random random) {
        return internalFactory.dispatchRandomPoison1(this, random);
    }

}
