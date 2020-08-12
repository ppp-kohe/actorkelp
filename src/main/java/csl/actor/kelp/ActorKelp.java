package csl.actor.kelp;

import csl.actor.*;
import csl.actor.cluster.ActorPlacement;
import csl.actor.kelp.behavior.*;
import csl.actor.kelp.shuffle.*;
import csl.actor.persist.MailboxManageable;
import csl.actor.persist.MailboxPersistableIncoming;
import csl.actor.persist.PersistentFileManager;
import csl.actor.util.FileSplitter;
import csl.actor.util.PathModifier;
import csl.actor.util.StagingActor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class ActorKelp<SelfType extends ActorKelp<SelfType>> extends ActorDefault
        implements StagingActor.StagingSupported, ActorKelpFileReader<SelfType>, KelpStage<SelfType>, AutoCloseable {
    protected ActorRef nextStage;
    protected FileSplitter fileSplitter;
    protected ConfigKelp config;
    protected boolean unit = false;
    protected int shuffleIndex = -1;
    protected int mergedCount = 1;
    protected Set<String> mergedActorNames = Collections.emptySet();
    protected Set<ActorRef> shuffleOriginals = new HashSet<>(1);
    protected List<KelpDispatcher.SelectiveDispatcher> selectiveDispatchers = null; //initialized by initBehavior

    public ActorKelp(ActorSystem system, String name, Mailbox mailbox, ActorBehavior behavior, ConfigKelp config) {
        super(system, name, mailbox, behavior);
        if (config == null) {
            this.config = ConfigKelp.CONFIG_DEFAULT;
        } else {
            this.config = config;
        }
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
        this.mailbox = initMailbox();
        this.behavior = initBehavior();
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
        name = getClass().getSimpleName() + "_" + UUID.randomUUID();
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

    ///////////////

    protected PersistentFileManager getPersistentFile() {
        return PersistentFileManager.getPersistentFile(system, getMailboxPath());
    }

    protected Mailbox initMailbox() {
        PersistentFileManager m = getPersistentFile();
        MailboxDefault mbox = initMailboxDefault(m);
        return initMailboxKelp(mbox, initTreeFactory(mbox, m));
    }

    protected MailboxDefault initMailboxDefault(PersistentFileManager m) {
        if (isPersist()) {
            return new MailboxPersistableIncoming(m, getMailboxOnMemorySize());
        } else {
            return new MailboxManageable.MailboxDefaultManageable(m);
        }
    }

    protected KeyHistograms initTreeFactory(MailboxDefault mbox, PersistentFileManager m) {
        return KeyHistogramsPersistable.createTreeFactory(
                isPersist(), isPersistRuntimeCondition(), mbox, m,
                new KeyHistogramsPersistable.HistogramTreePersistableConfigKelp(config));
    }

    protected MailboxKelp initMailboxKelp(MailboxDefault m, KeyHistograms treeFactory) {
        return new MailboxKelp(getMailboxTreeSize(), m, treeFactory);
    }

    public MailboxKelp getMailboxAsKelp() {
        return (MailboxKelp) super.getMailbox();
    }

    @Override
    protected ActorBehaviorBuilderKelp behaviorBuilder() {
        return new ActorBehaviorBuilderKelp(getMailboxAsKelp()::initMessageEntries, this::initSelectiveDispatchers);
    }

    protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilderKelp builder) {
        return super.initBehavior(builder);
    }

    @Override
    public void setNextStage(ActorRef nextStage) {
        this.nextStage = nextStage;
    }

    @Override
    public ActorRef nextStageActor() {
        return nextStage;
    }

    @Override
    public Iterable<? extends ActorRef> nextStageActors() {
        if (nextStage instanceof ActorRefShuffle) {
            return ((ActorRefShuffle) nextStage).getMemberActors();
        } else if (nextStage != null) {
            return Collections.singletonList(nextStage);
        } else {
            return Collections.emptyList();
        }
    }

    ///// processes


//    @Override
//    public boolean processMessageNext() {
//        if (getMailboxAsKelp().processHistogram(this)) {
//            return true;
//        } else {
//            return super.processMessageNext();
//        }
//    }

    @Override
    public void processMessage(Message<?> message) {
        processPrune();
        super.processMessage(message);
        getMailboxAsKelp().processHistogram(this); //ActorBehaviorMatchKey1 immediately process and prune a single value
    }

    public void processMessageBundle(MessageBundle<Object> mb) {
        processMessageBundle(this, mb);
    }

    public static void processMessageBundle(Actor self, MessageBundle<Object> mb) {
        mb.getData().forEach(d ->
                self.processMessage(new MessageAccepted<>(self, mb.getSender(), d))); //MessageBundle is already accepted by Dispatcher
    }

    public void processStagingCompleted(StagingActor.StagingCompleted comp) {
        processStagingCompleted(this, comp);
    }

    public static void processStagingCompleted(Actor self,
                                               StagingActor.StagingCompleted data) {
        if (self instanceof StagingActor.StagingSupported) {
            ActorRefShuffle.flush(((StagingActor.StagingSupported) self).nextStageActor(), self);
        }
        if (self instanceof ActorKelp<?>) {
            ((ActorKelp<?>) self).processStagingCompletedImpl(data);
        }
        data.accept(self);
    }

    public void processStagingCompletedImpl(StagingActor.StagingCompleted comp) {
        //clear mailbox
        Mailbox defaultMailbox = getMailboxAsKelp().getMailbox();
        if (defaultMailbox instanceof MailboxPersistableIncoming) {
            ((MailboxPersistableIncoming) defaultMailbox).delete();
        }

        //clear histogram
        //TODO avoid multiple execution?
        getMailboxAsKelp()
                .processStageEnd(this, comp.getTask().getKey(), getReducedSize());
    }

    public void processPrune() {
        getMailboxAsKelp().prune(
                getPruneGreaterThanLeaf(),
                getPruneLessThanNonZeroLeafRate());
    }

    ///// file reader

    @Override
    public CompletableFuture<StagingActor.StagingCompleted> startReading(String path, Instant startTime, Consumer<StagingActor> setup) {
        tell(new FileSplitter.FileSplit(path));
        StagingActor sa = StagingActor.staging(system)
                .withStartTime(startTime)
                .withWatcherSleepTimeMs(3);
        setup.accept(sa);
        return sa.start(this);
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
                fileSplitter.openLineIterator(split).forEachRemaining(line ->
                        processMessage(new MessageAccepted<>(this, this, line)));
                config.log("read finish: %s : %s", split, this);
            }
            flush();
        } catch (Exception ex) {
            config.log(ex, "splitter=%s split=%s", fileSplitter, split);
        }
    }

    @Override
    public void flush() {
        ActorRefShuffle.flush(nextStageActor(), this);
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
    }

    /**
     * default reader class
     * <pre>
     *     var r = new FileReader();
     *     r.connects(new MapperActor()); //mappers will receive FileSplit and String lines
     *
     *     r.startReading("file.txt").get();
     * </pre>
     */
    public static class FileReader extends ActorKelp<FileReader> {
        public FileReader(ActorSystem system, String name, ConfigKelp config) {
            super(system, name, config);
        }

        public FileReader(ActorSystem system, ConfigKelp config) {
            super(system, config);
        }

        public FileReader(ActorSystem system) {
            super(system);
        }

        @Override
        public int getShuffleBufferSizeMax() {
            return getShuffleBufferSizeFile();
        }
    }

    ///// serializable

    public ActorKelpSerializable<SelfType> toSerializable() {
        return toSerializable(true);
    }

    @SuppressWarnings("unchecked")
    public ActorKelpSerializable<SelfType> toSerializable(boolean includeMailbox) {
        return new ActorKelpSerializable<>((SelfType) this, includeMailbox);
    }

    /**
     * @return a serializable object which will be set to {@link ActorKelpSerializable#internalState}.
     *    The default impl. uses {@link ActorKelpSerializable#getBuilder(Class)}
     */
    public Object toInternalState() {
        try {
            return ActorKelpSerializable.getBuilder(getClass())
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
            ActorKelpSerializable.getBuilder(getClass())
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
            ActorKelpSerializable.getBuilder(getClass())
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
        return new MailboxKelp.ReducedSizeDefault(getReduceRuntimeCheckingThreshold(), getReduceRuntimeRemainingBytesToSizeRatio()) {
            @Override
            protected void logReducedSize(long size, long availableOnMemoryMessages, int consuming) {
                if (config.logSplit) {
                    config.log("%s reduceSize: %,d - %,d -> %,d", this, size, consuming, (size - consuming));
                }
            }

            @Override
            protected void logNeedToReduce(long size, long availableOnMemoryMessages) {
                if (config.logSplit) {
                    config.log("%s needToReduce: %,d > %,d & %,d", this, size, reduceRuntimeCheckingThreshold,
                            availableOnMemoryMessages);
                }
            }
        };
    }

    //// shuffle

    public ActorPlacement getPlacement() {
        Actor placement = getSystem().resolveActorLocalNamed(
                ActorRefLocalNamed.get(getSystem(), ActorPlacement.PLACEMENT_NAME));
        if (placement instanceof ActorPlacement) {
            return (ActorPlacement) placement;
        } else {
            return null;
        }
    }

    public ActorRefShuffleKelp<SelfType> shuffle() {
        return shuffle(Integer.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
    public ActorRefShuffleKelp<SelfType> shuffle(int bufferSizeMax) {
        ActorKelpStateSharing.StateSharingActor sharingActor = new ActorKelpStateSharing.StateSharingActor(system, config);
        shuffleOriginals.add(sharingActor);

        ActorKelpSerializable<SelfType> serialized = toSerializable();
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
                ActorRefShuffle.createDispatchUnits(
                        entries,
                        bufferSize),
                getSelectiveDispatchers().stream()
                    .map(KelpDispatcher.SelectiveDispatcher::copy)
                    .collect(Collectors.toList()),
                bufferSize,
                (Class<SelfType>) getClass());
    }

    protected ActorRefShuffleKelp<SelfType> createShuffle(ActorSystem system, List<ActorRefShuffle.ShuffleEntry> entries,
                                                          List<KelpDispatcher.SelectiveDispatcher> extractorsAndDispatchers, int bufferSize,
                                                          Class<SelfType> actorType) {
        return new ActorRefShuffleKelp<>(system, entries, extractorsAndDispatchers, bufferSize, actorType, config);
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

    protected void initSelectiveDispatchers(List<ActorBehaviorKelp.SelectiveDispatcherFactory> extractorsAndDispatchers) {
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
        task.accept(new ActorRefShuffle.ShuffleEntry(this, 0, -1));
    }

    @Override
    public List<? extends KelpDispatcher.DispatchUnit> getDispatchUnits() {
        return Collections.singletonList(new ActorRefShuffle.ShuffleEntry(this, 0, -1));
    }

    @Override
    public KelpDispatcher.DispatchUnit getDispatchUnit(int index) {
        return index == 0 ? new ActorRefShuffle.ShuffleEntry(this, 0, -1) : null;
    }

    ///// utilities for builder

    public static <T> ActorKelpFunctions.KeyExtractorFunction<T,T> identity() {
        return ActorKelpFunctions.KeyExtractorFunction.identity();
    }

    public static ActorKelpFunctions.DispatcherFactory dispatchAll() {
        return KelpDispatcher.DispatcherAll::new;
    }

    public static ActorKelpFunctions.DispatcherFactory dispatchShuffle() {
        return KelpDispatcher.DispatcherShuffle::new;
    }

    public static ActorKelpFunctions.DispatcherFactory dispatchRandomOne() {
        return KelpDispatcher.DispatcherRandomOne::new;
    }

    public static ActorKelpFunctions.DispatcherFactory dispatchRandomPoison1() {
        return KelpDispatcher.DispatcherRandomPoisson1::new;
    }

    public static ActorKelpFunctions.DispatcherFactory dispatchRandomOne(Random random) {
        return () -> new KelpDispatcher.DispatcherRandomOne(random);
    }

    public static ActorKelpFunctions.DispatcherFactory dispatchRandomPoison1(Random random) {
        return () -> new KelpDispatcher.DispatcherRandomPoisson1(random);
    }
}
