package csl.actor.kelp2;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;
import csl.actor.cluster.ActorPlacement;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.cluster.PersistentFileManager;
import csl.actor.kelp2.behavior.*;
import csl.actor.remote.ActorAddress;
import csl.actor.util.FileSplitter;
import csl.actor.util.StagingActor;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class ActorKelp<SelfType extends ActorKelp<SelfType>> extends ActorDefault
        implements StagingActor.StagingSupported, ActorKelpFileReader, KelpStage<SelfType> {
    protected ActorRef nextStage;
    protected FileSplitter fileSplitter;
    protected ConfigKelp config;
    protected boolean original = true;
    protected int shuffleIndex = -1;

    public ActorKelp(ActorSystem system, String name, Mailbox mailbox, ActorBehavior behavior, ConfigKelp config) {
        super(system, name, mailbox, behavior);
        if (config == null) {
            this.config = ConfigKelp.CONFIG_DEFAULT;
        } else {
            this.config = config;
        }
    }

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

    /**
     * @return true if the instance is manually created and not a suffle entry
     */
    public boolean isOriginal() {
        return original;
    }

    public void setOriginal(boolean original) {
        this.original = original;
    }

    public int getShuffleIndex() {
        return shuffleIndex;
    }

    public void setShuffleIndex(int shuffleIndex) {
        this.shuffleIndex = shuffleIndex;
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

    public int getMailboxThreshold() {
        return config.mailboxThreshold;
    }

    public double getPruneLessThanNonZeroLeafRate() {
        return config.pruneLessThanNonZeroLeafRate;
    }

    ///////////////

    protected PersistentFileManager getPersistentFile() {
        String path = getMailboxPath();
        return PersistentFileManager.getPersistentFile(system, () -> path);
    }

    protected Mailbox initMailbox() {
        PersistentFileManager m = getPersistentFile();
        return initMailboxKelp(initMailboxDefault(m));
    }

    protected MailboxDefault initMailboxDefault(PersistentFileManager m) {
        if (isPersist()) {
            return new MailboxPersistableKelp(m, getMailboxOnMemorySize());
        } else {
            return new MailboxDefault();
        }
    }

    protected MailboxKelp initMailboxKelp(MailboxDefault m) {
        return new MailboxKelp(1000, 32, m);
    }

    public MailboxKelp getMailboxAsKelp() {
        return (MailboxKelp) super.getMailbox();
    }

    @Override
    protected ActorBehaviorBuilderKelp behaviorBuilder() {
        return new ActorBehaviorBuilderKelp(getMailboxAsKelp()::initMessageEntries);
    }

    public static class MessageBundle<DataType> extends Message<List<DataType>> {
        public static final long serialVersionUID = 1L;

        public MessageBundle(ActorRef target, ActorRef sender, Iterable<? extends DataType> items) {
            super(target, sender, toList(items));
        }

        @Override
        public Message<List<DataType>> renewTarget(ActorRef target) {
            return new MessageBundle<>(target, sender, data);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" +
                    toStringData(Objects::toString) + " : " + target + " <- " + sender + ")";
        }

        @Override
        public String toString(Function<Object, Object> dataToStr) {
            return getClass().getSimpleName() + "(" +
                    toStringData(dataToStr) + " : " + target + " <- " + sender + ")";
        }

        public String toStringData(Function<Object,Object> dataToStr) {
            if (data == null) {
                return "null";
            } else if (data.isEmpty()) {
                return "[0]{}";
            } else {
                return String.format("[%,d]{%s, ...}", data.size(),
                        dataToStr.apply(data.get(0)));
            }
        }
    }

    public static <DataType> List<DataType> toList(Iterable<? extends DataType> items) {
        ArrayList<DataType> list = new ArrayList<>();
        for (DataType t : items) {
            list.add(t);
        }
        list.trimToSize();
        return list;
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


    @Override
    public boolean processMessageNext() {
        if (getMailboxAsKelp().processHistogram(this)) {
            return true;
        } else {
            return super.processMessageNext();
        }
    }

    @Override
    public void processMessage(Message<?> message) {
        processPrune();
        super.processMessage(message);
    }

    public void processMessageBundle(MessageBundle<Object> mb) {
        processMessageBundle(this, mb);
    }

    public static void processMessageBundle(Actor self, MessageBundle<Object> mb) {
        mb.getData().forEach(d ->
                self.processMessage(new Message<>(self, mb.getSender(), d)));
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
        if (defaultMailbox instanceof MailboxPersistableKelp) {
            ((MailboxPersistableKelp) defaultMailbox).delete();
        }

        //clear histogram
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
            fileSplitter = FileSplitter.getWithSplitLength(getSplitLength(), ConfigDeployment.getPathModifier(system));
        }
        try {
            if (split.getFileLength() == 0) {
                fileSplitter.splitIterator(split.getPath())
                        .forEachRemaining(this::processFileSplitForEachNext);
            } else {
                fileSplitter.openLineIterator(split).forEachRemaining(line ->
                        processMessage(new Message<>(this, this, line)));
                getSystem().getLogger().log("read finish: %s : %s", split, this);
            }
            ActorRefShuffle.flush(nextStageActor(), this);
        } catch (Exception ex) {
            getSystem().getLogger().log(true, 0, ex, "splitter=%s split=%s", fileSplitter, split);
        }
    }

    protected void processFileSplitForEachNext(FileSplitter.FileSplit split) {
        if (nextStage != null) {
            nextStage.tell(split);
        } else {
            tell(split); //process by self
        }
    }

    /**
     * default reader class
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
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .build();
        }
    }

    ///// serializable

    @SuppressWarnings("unchecked")
    public ActorKelpSerializable<SelfType> toSerializable() {
        return new ActorKelpSerializable<SelfType>((SelfType) this);
    }

    public Serializable toInternalState() {
        return null;
    }

    public void setInternalState(Serializable data) {

    }

    public static class ActorKelpSerializable<SelfType extends ActorKelp<SelfType>> implements Serializable {
        public static final long serialVersionUID = 1L;
        public Class<SelfType> actorType;
        public String name;
        public ConfigKelp config;
        public Message<?>[] messages;
        public List<KeyHistograms.HistogramTree> histograms;
        public Serializable internalState;

        public ActorKelpSerializable(SelfType actor) {
            init(actor);
        }

        protected void init(SelfType actor) {
            initActorType(actor);
            initName(actor);
            initConfig(actor);
            initInternalState(actor);
        }

        @SuppressWarnings("unchecked")
        protected void initActorType(SelfType actor) {
            actorType = (Class<SelfType>) actor.getClass();
        }

        protected void initName(SelfType actor) {
            name = actor.getName();
        }

        protected void initConfig(SelfType actor) {
            config = actor.getConfig();
        }

        protected void initInternalState(SelfType actor) {
            internalState = actor.toInternalState();
        }

        public void setMessages(Message<?>[] messages) {
            this.messages = messages;
        }

        public void setHistograms(List<KeyHistograms.HistogramTree> histograms) {
            this.histograms = histograms;
        }

        public SelfType restore(ActorSystem system, long num, ConfigKelp config) throws Exception {
            SelfType a = create(system, restoreName(num), config);
            restoreSetNonOriginal(a);
            restoreSetShuffleIndex(a, num);
            restoreInit(a);
            return a;
        }

        protected String restoreName(long num) {
            return name == null ? ("$" + num) : name + "$" + num;
        }

        protected SelfType create(ActorSystem system, String name, ConfigKelp config) throws Exception {
            return actorType.getConstructor(ActorSystem.class, String.class, ConfigKelp.class)
                    .newInstance(system, name, config);
        }

        protected void restoreSetNonOriginal(SelfType actor) {
            actor.setOriginal(false);
        }

        protected void restoreSetShuffleIndex(SelfType actor, long num) {
            actor.setShuffleIndex((int) num);
        }

        protected void restoreInit(SelfType actor) {
            actor.setInternalState(internalState);
        }
    }

    ////// reducedSize

    public MailboxKelp.ReducedSize getReducedSize() {
        return new MailboxKelp.ReducedSizeDefault(getReduceRuntimeCheckingThreshold(), getReduceRuntimeRemainingBytesToSizeRatio()) {
            @Override
            protected void logReducedSize(long size, long availableOnMemoryMessages, int consuming) {
                if (config.logSplit) {
                    config.log("%s reduceSize: %,d -> %,d", this, size, consuming);
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

    @SuppressWarnings("unchecked")
    public ActorRefShuffleKelp<SelfType> shuffle() {
        ActorKelpSerializable<SelfType> serialized = toSerializable(); //TODO without mailbox
        ActorPlacement place = getPlacement();

        int partitions = getConfig().shufflePartitions;
        int bufferSize = getConfig().shufflePartitions;
        boolean hostIncludePort = getConfig().shuffleHostIncludePort;

        return createShuffle(
                getSystem(),
                ActorRefShuffle.createEntries(
                        IntStream.range(0, partitions)
                            .mapToObj(i -> createAndPlace(place, serialized, i))
                            .collect(Collectors.toList()),
                        bufferSize,
                        ActorRefShuffle.refToHost(hostIncludePort)),
                getKeyExtractors(),
                bufferSize,
                hostIncludePort,
                (Class<SelfType>) getClass());
    }

    protected ActorRefShuffleKelp<SelfType> createShuffle(ActorSystem system, Map<ActorAddress, List<ActorRefShuffle.ShuffleEntry>> entries,
                                                          List<ActorKelpFunctions.KeyExtractor<?, ?>> keyExtractors, int bufferSize, boolean hostIncludePort,
                                                          Class<SelfType> actorType) {
        return new ActorRefShuffleKelp<>(system, entries, keyExtractors, bufferSize, hostIncludePort, actorType);
    }

    @SuppressWarnings("unchecked")
    public List<ActorKelpFunctions.KeyExtractor<?,?>> getKeyExtractors() {
        return getMailboxAsKelp().getEntries().stream()
                .map(HistogramEntry::getProcessor)
                .filter(ActorBehaviorKelp.ActorBehaviorMatchKey.class::isInstance)
                .map(ActorBehaviorKelp.ActorBehaviorMatchKey.class::cast)
                .flatMap(p -> ((List<ActorKelpFunctions.KeyExtractor<?,?>>) p.getKeyExtractors()).stream())
                .collect(Collectors.toList());
    }

    protected ActorRef createAndPlace(ActorPlacement place, ActorKelpSerializable<SelfType> serialized, int i) {
        try {
            Actor a = serialized.restore(system, i, getConfig());
            if (place != null) {
                return place.place(a);
            } else {
                return a;
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class ActorRefShuffleKelp<ActorType extends Actor> extends ActorRefShuffle implements KelpStage<ActorType> {
        public static final long serialVersionUID = 1L;
        protected Class<?> actorType;

        public ActorRefShuffleKelp() {
        }

        public ActorRefShuffleKelp(ActorSystem system, Map<ActorAddress, List<ShuffleEntry>> entries,
                                   List<ActorKelpFunctions.KeyExtractor<?, ?>> keyExtractors, int bufferSize, boolean hostIncludePort,
                                   Class<ActorType> actorType) {
            super(system, entries, keyExtractors, bufferSize, hostIncludePort);
            this.actorType = actorType;
        }

        @Override
        public <NextActorType extends Actor> KelpStage<NextActorType> connects(Class<NextActorType> actorType, ActorRef ref) {
            ref = connectStageInitialActor(ref);
            try {
                connectStageWithoutInit(ref).get();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return toKelpStage(system, actorType, ref);
        }

        @Override
        public void write(Kryo kryo, Output output) {
            super.write(kryo, output);
            kryo.writeClass(output, actorType);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            super.read(kryo, input);
            actorType = kryo.readClass(input).getType();
        }
    }

    @Override
    public <NextActorType extends Actor> KelpStage<NextActorType> connects(Class<NextActorType> actorType, ActorRef ref) {
        ref = ActorRefShuffle.connectStageInitialActor(system, ref);
        if (ref instanceof ActorRefShuffle) {
            ref = ((ActorRefShuffle) ref).use();
        }
        setNextStage(ref);
        return toKelpStage(system, actorType, ref);
    }

    @SuppressWarnings("unchecked")
    public static <NextActorType extends Actor> KelpStage<NextActorType> toKelpStage(ActorSystem system, Class<NextActorType> actorType, ActorRef ref) {
        if (ref instanceof ActorRefShuffleKelp<?>) {
            return (ActorRefShuffleKelp<NextActorType>) ref;
        } else {
            return new KelpStageRefWrapper<>(system, actorType, ref);
        }
    }

    @Override
    public List<ActorRef> getMemberActors() {
        return Collections.emptyList();
    }
}
