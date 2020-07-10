package csl.actor.kelp;

import csl.actor.*;
import csl.actor.cluster.*;
import csl.actor.kelp.KelpRoutingSplit.SplitPath;
import csl.actor.util.ConfigBase;
import csl.actor.util.ResponsiveCalls;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * <h3>selecting constructor</h3>
 * <p>
 * by default, the primary constructor, {@link #ActorKelp(ActorSystem, String, Config, State)}
 *  is used at cloning in {@link ActorKelpSerializable#create(ActorSystem, String, Config, State)}.
 *  The your sub-class should just override the primary constructor and you can define another constructors for creating a router object.
 * </p>
 *
 * <h3>internal state and cloning process</h3>
 * <p>
 *     For typical impl. of internal state, implements
 *      <ol>
 *          <li>the primary constructor</li>
 *          <li>{@link #toSerializableInternalState()} and {@link #initSerializedInternalState(Serializable)}</li>
 *          <li>{@link #initMerged(ActorKelp)}</li>
 *          <li>{@link #initClone(ActorKelp)}</li>
 *      </ol>
 *
 * </p>
 * <p>
 *  a clone is created by {@link #internalCreateClone(ActorRef)}. it uses {@link Object#clone()}.
 *   Note the method cannot be used for remote nodes, but it first creates local sub-splits by the method.
 *   The method has a call of {@link #initClone(ActorKelp)} which is an empty impl..
 *   Your class can supply some init process for the local copy in the method.
 * </p>
 *
 * <p>
 *     The easiest way of customizing states is using {@link #toSerializableInternalState()}
 *      and {@link #initSerializedInternalState(Serializable)}.
 *      You can transfer any kind of {@link Serializable} object by those methods.
 * </p>
 *
 * <p>
 *    An actor is not {@link Serializable}, so it uses {@link ActorKelpSerializable} for remote sending.
 *    It is created by {@link #newSerializableState()}, and it just creates the instance.
 *    Your class can override the method for customizing the serializable class.
 * </p>
 *
 * <p>
 *     Cloning is a different step from splitting and merging.
 *     For merging, {@link #initMerged(ActorKelp)}
 * </p>
 *
 * <h3>using as unit</h3>
 * <p>
 * it can call {@link #setAsUnit()} in the constructor
 * </p>
 *
 * @param <SelfType> the self-type, e.g.
 *                  <code>class T extends ActorKelp&lt;T&gt;</code>
 */
@SuppressWarnings("rawtypes")
public abstract class ActorKelp<SelfType extends ActorKelp<SelfType>> extends ActorDefault
        implements KeyHistogramsPersistable.HistogramTreePersistableConfig, PhaseShift.StageSupported, Cloneable {
    protected Config config;
    protected volatile ActorRef nextStage;

    protected volatile State state;

    public static boolean logSkipTimeout = System.getProperty("csl.actor.logSkipTimeout", "true").equals("true");

    public ActorKelp(ActorSystem system, String name, MailboxKelp mailbox, ActorBehavior behavior, Config config, State state) {
        super(system, name, mailbox, behavior);
        if (config == null) {
            this.config = Config.CONFIG_DEFAULT;
        } else {
            this.config = config;
        }
        this.state = state;
    }

    /**
     * the primary constructor
     * @param system the actor belonged to the system
     * @param name the name of the actor, or null for anonymous
     * @param config a config object, or null for {@link Config#CONFIG_DEFAULT}
     * @param state the internal state
     */
    public ActorKelp(ActorSystem system, String name, Config config, State state) {
        this(system, name, null, null, config, state);
        this.state = state;
        mailbox = initMailbox();
        behavior = initBehavior();
    }

    public ActorKelp(ActorSystem system, String name, Config config) {
        this(system, name, config, (State) null);
        state = initStateRouter();
    }

    public ActorKelp(ActorSystem system, String name) {
        this(system, name, (Config) null);
    }

    public ActorKelp(ActorSystem system, Config config) {
        this(system, null, config);
    }

    public ActorKelp(ActorSystem system) {
        this(system, null, (Config) null);
    }

    //////////////////////// config


    public Config getConfig() {
        return config;
    }

    public boolean routerAutoSplit() {
        return config.routerAutoSplit;
    }

    public boolean routerAutoMerge() {
        return config.routerAutoMerge;
    }

    public int mailboxTreeSize() {
        return config.mailboxTreeSize;
    }

    public long traverseDelayTimeMs() {
        return config.traverseDelayTimeMs;
    }

    protected double pruneLessThanNonZeroLeafRate() {
        return config.pruneLessThanNonZeroLeafRate;
    }

    protected String persistMailboxPath() {
        return config.persistMailboxPath;
    }

    protected boolean persistRuntimeCondition() {
        return config.persistRuntimeCondition;
    }

    protected boolean persist() {
        return config.persist;
    }

    protected long persistMailboxSizeLimit() {
        return config.persistMailboxSizeLimit;
    }

    protected long persistMailboxOnMemorySize() {
        return config.persistMailboxOnMemorySize;
    }

    protected int reduceRuntimeCheckingThreshold() {
        return config.reduceRuntimeCheckingThreshold;
    }

    protected double reduceRuntimeRemainingBytesToSizeRatio() {
        return config.reduceRuntimeRemainingBytesToSizeRatio;
    }

    @Override
    public int histogramPersistHistoryEntrySize() { return config.histogramPersistHistoryEntrySize; }
    @Override
    public int histogramPersistHistoryEntryLimit() { return config.histogramPersistHistoryEntryLimit; }
    @Override
    public long histogramPersistSizeLimit() { return config.histogramPersistSizeLimit; }
    @Override
    public long histogramPersistOnMemorySize() { return config.histogramPersistOnMemorySize; }
    @Override
    public double histogramPersistSizeRatioThreshold() { return config.histogramPersistSizeRatioThreshold; }
    @Override
    public long histogramPersistRandomSeed() { return config.histogramPersistRandomSeed; }


    protected int mailboxThreshold() {
        return config.mailboxThreshold;
    }

    public float lowerBoundThresholdFactor() {
        return config.lowerBoundThresholdFactor;
    }

    public int minSizeOfEachMailboxSplit() {
        return config.minSizeOfEachMailboxSplit;
    }

    public int historyExceededLimit() {
        return (int) (config.historyExceededLimitThresholdFactor * mailboxThreshold());
    }

    public int maxParallelRouting() {
        return (int) Math.min(Integer.MAX_VALUE, (long) (mailboxThreshold() * (double) config.maxParallelRoutingThresholdFactor));
    }

    protected int historyEntrySize() {
        return config.historyEntrySize;
    }

    public float mergeRatioThreshold() {
        return config.mergeRatioThreshold;
    }

    public int historyEntryLimit() {
        return (int) (mailboxThreshold() * config.historyEntryLimitThresholdFactor);
    }

    protected long pruneGreaterThanLeaf() {
        return (long) config.pruneGreaterThanLeafThresholdFactor * mailboxThreshold();
    }

    protected long toLocalWaitMs() {
        return config.toLocalWaitMs;
    }

    public boolean logSplit() {
        return config.logSplit;
    }

    //////////////////////// init

    protected KelpStateRouter initStateRouter() {
        return new KelpStateRouter();
    }

    protected StateUnit initStateUnit(ActorRef router) {
        return new StateUnit(router);
    }

    @Override
    protected Mailbox initMailbox() {
        MailboxPersistable.PersistentFileManager m = getPersistentFile();
        MailboxDefault mailbox = initMailboxDefault(m);
        return new MailboxKelp(mailboxThreshold(), mailboxTreeSize(),
                mailbox, initTreeFactory(mailbox, m));
    }

    protected MailboxDefault initMailboxDefault(MailboxPersistable.PersistentFileManager m) {
        if (initPersistentEnabled()) {
            MailboxPersistable.PersistentConditionMailbox condMailbox;
            if (persistRuntimeCondition()) {
                condMailbox = new MailboxPersistable.PersistentConditionMailboxSampling(persistMailboxSizeLimit(), m.getLogger());
            } else {
                condMailbox = new MailboxPersistable.PersistentConditionMailboxSizeLimit(persistMailboxSizeLimit());
            }
            return new MailboxPersistable(m, condMailbox, persistMailboxOnMemorySize());
        } else {
            return new MailboxDefault();
        }
    }

    protected KeyHistograms initTreeFactory(MailboxDefault mailbox, MailboxPersistable.PersistentFileManager m) {
        if (initPersistentEnabled()) {
            KeyHistogramsPersistable.PersistentConditionHistogram condHist;
            if (persistRuntimeCondition()) {
                if (mailbox instanceof MailboxPersistable) {
                    condHist = new KeyHistogramsPersistable.PersistentConditionHistogramSampling(
                            histogramPersistSizeLimit(), histogramPersistSizeRatioThreshold(), (MailboxPersistable) mailbox, m.getLogger());
                } else {
                    condHist = new KeyHistogramsPersistable.PersistentConditionHistogramSampling(
                            histogramPersistSizeLimit(), histogramPersistSizeRatioThreshold(), m.getLogger());
                }
            } else {
                condHist = new KeyHistogramsPersistable.PersistentConditionHistogramSizeLimit(this);
            }
            return new KeyHistogramsPersistable(this, m, condHist);
        } else {
            return new KeyHistograms(m);
        }
    }

    protected MailboxPersistable.PersistentFileManager getPersistentFile() {
        String path = persistMailboxPath();
        return MailboxPersistable.getPersistentFile(system, () -> path);
    }

    protected boolean initPersistentEnabled() {
        return persist();
    }

    protected Mailbox initMailboxForClone() {
        return getMailboxAsKelp().create();
    }

    /**
     * @param m the merging instance, which will be discarded. So you can close any resources in m in the method.
     */
    protected void initMerged(SelfType m) { }

    protected void initClone(SelfType original) { }

    @Override
    protected ActorBehaviorBuilderKelp behaviorBuilder() {
        return new ActorBehaviorBuilderKelp((ps) -> getMailboxAsKelp().initMessageEntries(ps));
    }

    @SuppressWarnings("unchecked")
    public SelfType setAsUnit() {
        state = initStateUnit(null);
        return (SelfType) this;
    }

    ////////////////////////

    public MailboxKelp getMailboxAsKelp() {
        return (MailboxKelp) mailbox;
    }

    public ActorRef router() {
        if (state instanceof KelpStateRouter) {
            return this;
        } else if (state instanceof StateUnit) {
            return routerOrThis(((StateUnit) state).getRouter());
        } else if (state instanceof StateCanceled) {
            return routerOrThis(((StateCanceled) state).getRouter());
        } else {
            return this;
        }
    }

    private ActorRef routerOrThis(ActorRef router) {
        if (router == null) {
            return this;
        } else {
            return router;
        }
    }

    public boolean hasRemainingProcesses() {
        return isRouterParallelRouting() || getMailboxAsKelp().hasRemainingProcesses();
    }

    public boolean isRouterParallelRouting() {
        return state instanceof KelpStateRouter && !((KelpStateRouter) state).isNonParallelRouting();
    }

    //////////////////////// internal state

    public State getState() {
        return state;
    }

    public interface State {
        long getProcessCount();
        void processMessage(ActorKelp self, Message<?> message);
        boolean processMessagePhase(ActorKelp self, Message<?> message);
    }

    public static class StateUnit implements State, Serializable {
        public static final long serialVersionUID = 1L;
        protected ActorRef router;
        protected long processCount;

        public StateUnit(ActorRef router) {
            this.router = router;
        }

        public ActorRef getRouter() {
            return router;
        }

        @Override
        public long getProcessCount() {
            return processCount;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void processMessage(ActorKelp self, Message<?> message) {
            processCount++;
            self.processMessageBehavior(message);
        }

        @Override
        public boolean processMessagePhase(ActorKelp self, Message<?> message) {
            Object val = message.getData();
            if (val instanceof PhaseShift) {
                ((PhaseShift) val).accept(self, message.getSender());
                return true;
            } else if (val instanceof PhaseShift.PhaseShiftIntermediate) {
                PhaseShift.PhaseShiftIntermediate event = (PhaseShift.PhaseShiftIntermediate) val;
                if (event.getType().equals(PhaseShift.PhaseShiftIntermediateType.PhaseIntermediateFinishLeaf)) {
                    self.processPhaseEnd(event.getKey());
                }
                event.accept(self, phaseTarget(self), message.getSender());
                return true;
            } else if (val instanceof CancelChange) {
                processMessage(self, message);
                return true;
            } else {
                return false;
            }
        }

        ActorRef phaseTarget(ActorKelp self) {
            return router == null ? self : router;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + (router == null ? "" : ("(router=" + router + ")"));
        }
    }

    public static class StateCanceled implements State {
        protected ActorRef router;
        protected long processCount;

        public StateCanceled(ActorRef router, long processCount) {
            this.router = router;
            this.processCount = processCount;
        }

        public ActorRef getRouter() {
            return router;
        }

        @Override
        public long getProcessCount() {
            return processCount;
        }

        @Override
        public void processMessage(ActorKelp self, Message<?> message) {
            processCount++;
            router.tell(message.getData(), message.getSender());
        }

        @Override
        public boolean processMessagePhase(ActorKelp self, Message<?> message) {
            Object val = message.getData();
            if (val instanceof PhaseShift) {
                ((PhaseShift) val).accept(self, message.getSender());
                return true;
            } else if (val instanceof PhaseShift.PhaseShiftIntermediate) {
                ((PhaseShift.PhaseShiftIntermediate) val).accept(self, router, message.getSender());
                return true;
            } else if (val instanceof CancelChange) {
                processMessage(self, message);
                return true;
            } else {
                return false;
            }
        }

    }

    /////////////////////////// process

    @Override
    public boolean processMessageNext() {
        boolean pr = isRouterParallelRouting();
        try {
            if (!pr && getMailboxAsKelp().processHistogram(this)) {
                return true;
            }
            return super.processMessageNext();
        } catch (Throwable th) {
            throw new RuntimeException(String.format("%s: parallelRouting=%s", this, pr), th);
        }
    }

    @Override
    protected void processMessage(Message<?> message) {
        if (isNoRoutingMessage(message)) {
            if (isRouterParallelRouting()) {
                processMessageDelayWhileParallelRouting(message);
            } else {
                if (!processMessagePhase(message)) {
                    processMessageBehavior(message);
                }
            }
        } else {
            state.processMessage(this, message);
        }
    }

    public void processMessageBehavior(Message<?> message) {
        if (getSystem() instanceof ActorSystemCluster) {
            ((ActorSystemCluster) getSystem()).awaits(message,
                    ConfigBase.lazyToString(() -> "processMessageBehavior: " + this));
        }
        processPrune();
        Object data = message.getData();
        if (data instanceof MailboxKelp.TraversalProcess) {
            getMailboxAsKelp()
                    .processTraversal(this,
                            ((MailboxKelp.TraversalProcess) data).entryId,
                            reducedSize());
        } else {
            super.processMessage(message);
        }
    }

    protected void processMessageDelayWhileParallelRouting(Message<?> message) {
        getSystem().send(message);
    }

    protected boolean processMessagePhase(Message<?> message) {
        return getState().processMessagePhase(this, message);
    }

    protected boolean isNoRoutingMessage(Message<?> message) {
        Object data = message.getData();
        return data instanceof MessageNoRouting ||
                data instanceof MailboxKelp.TraversalProcess ||
                isNoRoutingMessagePhase(message) ||
                (data instanceof CallableMessage<?,?> &&
                        !(data instanceof MessageNoRouting.Routing));
    }

    protected boolean isNoRoutingMessagePhase(Message<?> message) {
        Object data = message.getData();
        return data instanceof PhaseShift ||
                data instanceof PhaseShift.PhaseCompleted ||
                data instanceof PhaseShift.PhaseShiftIntermediate;

    }

    @Override
    public ActorRef nextStage() {
        return nextStage;
    }

    public CompletableFuture<CallableMessage.CallableResponseVoid> setNextStage(ActorRef nextStage) {
        return ResponsiveCalls.sendTaskConsumer(this, (a) -> a.nextStage = nextStage);
    }

    public MailboxKelp.ReducedSize reducedSize() {
        return new MailboxKelp.ReducedSizeDefault(reduceRuntimeCheckingThreshold(), reduceRuntimeRemainingBytesToSizeRatio()) {
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


    public void processPrune() {
        getMailboxAsKelp().prune(
                pruneGreaterThanLeaf(),
                pruneLessThanNonZeroLeafRate());
    }

    public void processPhaseEnd(Object phaseKey) {
        getMailboxAsKelp()
                .processPhase(this, phaseKey, reducedSize());
    }

    /////////////////////////// methods for state

    public KelpRoutingSplit internalCreateSplitNode(KelpRoutingSplit.SplitOrMergeContext context,
                                                    KelpRoutingSplit old,
                                                    ActorKelp target, SplitPath path, int height) {
        try {
            target.getMailboxAsKelp().lockRemainingProcesses();

            ActorRef routerRef = router();
            ActorKelp a1 = target.internalCreateClone(routerRef);
            ActorKelp a2 = target.internalCreateClone(routerRef);
            List<Object> splitPoints = target.getMailboxAsKelp()
                    .splitMessageHistogramIntoReplicas(a1.getMailboxAsKelp(), a2.getMailboxAsKelp());
            if (routerRef != target) {
                target.internalCancel();
            }
            return internalCreateSplitNode(context, old, splitPoints, a1, a2, path, height);
        } finally {
            target.getMailboxAsKelp().unlockRemainingProcesses(target);
        }
    }

    public KelpRoutingSplit internalCreateSplitNode(KelpRoutingSplit.SplitOrMergeContext context,
                                                    KelpRoutingSplit old,
                                                    List<Object> splitPoints, ActorKelp a1, ActorKelp a2, SplitPath path, int height) {
        KelpRoutingSplit s1 = internalCreateSplitLeaf(context, old, a1, path.add(true), height);
        KelpRoutingSplit s2 = internalCreateSplitLeaf(context, old, a2, path.add(false), height);

        return newSplitNode(splitPoints, s1, s2, path);
    }

    public KelpRoutingSplit internalCreateSplitLeaf(KelpRoutingSplit.SplitOrMergeContext context,
                                                    KelpRoutingSplit old,
                                                    ActorKelp actor, SplitPath path, int height) {
        if (path.depth() >= height) {
            internalCreateSplitLeafNewName(actor, path);
            ActorRef a = place(actor.getPlacement(), actor);
            if (a == this) {
                return null;
            } else {
                return newSplitLeaf(context, old, a, path);
            }
        } else {
            if (height <= 1 && actor == this) {
                return null;
            } else {
                internalCreateSplitLeafNewName(actor, path);
                return newSplitLeaf(context, old, actor, path).split(context, height);
            }
        }
    }

    public void internalCreateSplitLeafNewName(ActorKelp actor, SplitPath path) {
        String name = actor.name;
        if (name != null) {
            int ni = name.lastIndexOf("#");
            if (ni >= 0) {
                name = name.substring(0, ni);
            }
            name += "#" + path.toBinaryString();
            if (!(actor.state instanceof KelpStateRouter)) { //router
                actor.name = name;
                actor.getSystem().register(actor);
            }
        }
    }

    private KelpRoutingSplit newSplitLeaf(KelpRoutingSplit.SplitOrMergeContext context,
                                          KelpRoutingSplit old,
                                          ActorRef actor, SplitPath path) {
        KelpRoutingSplit s = newSplitLeaf(actor, path);
        context.split(s, old);
        return s;
    }

    public KelpRoutingSplit newSplitNode(List<Object> splitPoints, KelpRoutingSplit s1, KelpRoutingSplit s2, SplitPath path) {
        return new KelpRoutingSplit.RoutingSplitNode(splitPoints, s1, s2, path, historyEntrySize());
    }

    public KelpRoutingSplit.RoutingSplitLeaf newSplitLeaf(ActorRef actor, SplitPath path) {
        return new KelpRoutingSplit.RoutingSplitLeaf(actor, path);
    }


    public Message<?> internalPollForParallelRouting() {
        return mailbox.poll();
    }

    public void internalMerge(SelfType merged) {
        getMailboxAsKelp().lockRemainingProcesses();
        merged.getMailboxAsKelp().lockRemainingProcesses();
        getMailboxAsKelp()
                .merge(merged.getMailboxAsKelp());
        if (this.name != null) {
            getSystem().register(this); //re-register this for this.name == merged.name
        }
        merged.internalCancel();
        try {
            initMerged(merged);
        } finally {
            merged.getMailboxAsKelp().unlockRemainingProcesses(merged);
            getMailboxAsKelp().unlockRemainingProcesses(this);
        }
    }

    @SuppressWarnings("unchecked")
    public ActorKelp internalCreateClone(ActorRef router) {
        try {
            ActorKelp a = (ActorKelp) super.clone();
            //if the actor has the name, it copies the reference to the name,
            // but it does not register the actor
            a.processLock = new AtomicBoolean(false);
            a.mailbox = a.initMailboxForClone();
            a.behavior = a.initBehavior(); //recreate behavior with initMessageEntry by ActorBehaviorBuilderKeyValue
            a.state = a.initStateUnit(router);
            a.initClone(this);
            return a;
        } catch (CloneNotSupportedException ce) {
            throw new RuntimeException(ce);
        }
    }

    public ActorKelp toLocal(ActorRef ref) {
        if (ref instanceof ActorKelp) {
            return (ActorKelp) ref;
        }
        try {
            ActorKelpSerializable state = ResponsiveCalls.sendTask(system, ref,
                    new CallableToLocalSerializable())
                    .get(toLocalWaitMs(), TimeUnit.MILLISECONDS);
            return state.create(system, -1);
        } catch (Throwable ex) {
            errorToLocal(ex, "toLocal", ref);
            return null;
        }
    }

    protected static Map<String, Instant> errorRecords = new HashMap<>();

    protected void errorToLocal(Throwable ex, String info, ActorRef ref) {
        if (logSkipTimeout && ex instanceof TimeoutException) {
            //here, the error means failure of split or merge, and the typical reason is busyness of the target, which can be ignored
            return;
        }
        synchronized (this) {
            Instant last = errorRecords.get(info);
            Instant now = Instant.now();
            if (last == null || Duration.ofSeconds(30).minus(Duration.between(last, now)).isNegative()) {
                errorRecords.put(info, now);
                config.log(ex, "%s: %s", info, ref);
            }
        }
    }

    public static class CallableToLocalSerializable implements CallableMessage<ActorKelp, ActorKelpSerializable> {
        public static final long serialVersionUID = 1L;
        @Override
        public ActorKelpSerializable call(ActorKelp self) {
            self.getMailboxAsKelp().lockRemainingProcesses();
            try {
                return self.toSerializable(-1);
            } finally {
                self.getMailboxAsKelp().terminateAfterSerialized();
                self.internalCancel();
                self.getMailboxAsKelp().unlockRemainingProcesses(self);
            }
        }
    }

    public void internalCancel() { //remaining messages are processed by the canceled state
        state = new StateCanceled(router(), state == null ? 0 : state.getProcessCount());
        this.name = this.name + ".canceled." + Instant.now().getNano();
        getSystem().register(this);
        router().tell(new CancelChange(this, CanceledChangeType.CancelAdded));
    }

    public static class CancelChange implements Serializable, MessageNoRouting {
        public static final long serialVersionUID = 1L;
        protected ActorRef canceledActor;
        protected Object data;

        public CancelChange(ActorRef canceledActor, Object data) {
            this.canceledActor = canceledActor;
            this.data = data;
        }

        public ActorRef getCanceledActor() {
            return canceledActor;
        }

        public Object getData() {
            return data;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + data + ", " + canceledActor + ")";
        }
    }

    public enum CanceledChangeType {
        CancelAdded,
        CancelFinished,
    }

    /////////////////////////// split or merge APIs

    public CompletableFuture<Integer> routerGetMaxHeight() {
        return ResponsiveCalls.<ActorKelp, Integer>sendTask(getSystem(), router(), (a) -> {
            State state = a.getState();
            if (state instanceof KelpStateRouter) {
                return ((KelpStateRouter) state).getMaxHeight(a);
            } else {
                return 1;
            }
        });
    }

    public CompletableFuture<CallableMessage.CallableResponseVoid> routerSplit(int height) {
        return ResponsiveCalls.<ActorKelp>sendTaskConsumer(getSystem(), router(), (a) -> {
            State state = a.state;
            if (state instanceof KelpStateRouter) {
                ((KelpStateRouter) state).split(a, height);
            }
        });
    }

    public CompletableFuture<CallableMessage.CallableResponseVoid> routerMergeInactive() {
        return ResponsiveCalls.<ActorKelp>sendTaskConsumer(getSystem(), router(), (a) -> {
            State state = a.state;
            if (state instanceof KelpStateRouter) {
                ((KelpStateRouter) state).mergeInactive(a);
            }
        });
    }

    public CompletableFuture<CallableMessage.CallableResponseVoid> routerSplitOrMerge(int height) {
        return ResponsiveCalls.<ActorKelp>sendTaskConsumer(getSystem(), router(), (a) -> {
            State state = a.state;
            if (state instanceof KelpStateRouter) {
                ((KelpStateRouter) state).splitOrMerge(a, height);
            }
        });
    }

    protected void afterSplitOrMerge(KelpRoutingSplit.SplitOrMergeContextDefault context) {
        if (logSplit() && context.hasChanges()) {
            String msg = context.getMessage();
            if (context.isMergedToRoot() || context.isSplitFromRoot()) {
                printStatus(msg);
            } else {
                printStatus(msg, context.getNewSplitsSorted());
            }
        }
    }


    /////////////////////////// remote placement and serialization

    public static ActorRef place(ActorPlacement placement, ActorKelp a) {
        if (placement != null) {
            return placement.place(a);
        } else {
            a.getSystem().send(new Message.MessageNone(a));
            return a;
        }
    }

    public ActorPlacement getPlacement() {
        Actor placement = getSystem().resolveActorLocalNamed(
                ActorRefLocalNamed.get(getSystem(), ActorPlacement.PLACEMENT_NAME));
        if (placement instanceof ActorPlacement) {
            return (ActorPlacement) placement;
        } else {
            return null;
        }
    }

    public ActorKelpSerializable toSerializable(long num) {
        return initSerializableState(newSerializableState(), num);
    }

    protected ActorKelpSerializable newSerializableState() {
        return new ActorKelpSerializable();
    }

    protected ActorKelpSerializable initSerializableState(ActorKelpSerializable state, long num) {
        state.actorType = getClass();
        String n = getName();
        if (n == null) {
            n = "$" + num;
        }
        state.name = n;
        state.config = config;
        state.router = router();
        state.nextStage = nextStage;
        MailboxKelp r = getMailboxAsKelp();
        r.serializeTo(state);
        state.internalState = toSerializableInternalState();
        return state;
    }

    protected Serializable toSerializableInternalState() {
        return null;
    }

    protected void initSerializedInternalState(Serializable s) { }

    public String getOutputFileHeader() {
        String n = getName();
        if (n == null) {
            n = getClass().getSimpleName() + "-" + Integer.toHexString(System.identityHashCode(this));
        }
        return "%h-" + ActorPlacement.toOutputFileComponent(true, 30, n);
    }


    public static class ActorKelpSerializable implements Serializable {
        public static final long serialVersionUID = 1L;
        public Class<? extends ActorKelp> actorType;
        public String name;
        public Message<?>[] messages;
        public List<KeyHistograms.HistogramTree> histograms;
        public Config config;
        public ActorRef router;
        public Serializable internalState;
        public ActorRef nextStage;

        public ActorKelp create(ActorSystem system, long num) throws Exception {
            return init(create(system, name(num), config(), state(router)));
        }

        protected String name(long num) {
            return name == null ? ("$" + num) : name;
        }

        protected Config config() {
            return config == null ? Config.CONFIG_DEFAULT : config;
        }

        protected State state(ActorRef router) {
            return new StateUnit(router);
        }

        protected ActorKelp init(ActorKelp a) {
            a.getMailboxAsKelp().deserializeFrom(this);
            a.initSerializedInternalState(internalState);
            a.setNextStage(nextStage);
            return a;
        }

        protected ActorKelp create(ActorSystem system, String name, Config config, State state) throws Exception {
            return actorType.getConstructor(ActorSystem.class, String.class, Config.class, State.class)
                    .newInstance(system, name, config, state);
        }
    }

    /////////////////////////// print status

    public void printStatus() {
        printStatus("");
    }

    public void printStatus(String head) {
        printStatus(config.getLogger(), head);
    }

    public void printStatus(String head, List<KelpRoutingSplit> newSplits) {
        printStatus(config.getLogger(), head, newSplits);
    }

    public void log(String str) {
        config.log("%s", str);
    }

    public void log(String str, Object... args) {
        config.log(str, args);
    }

    public void log(Throwable ex, String str) {
        config.log(ex, "%s", str);
    }

    public void log(Throwable ex, String str, Object... args) {
        config.log(ex, str, args);
    }

    @Override
    public void logPhase(String str, Object... args) {
        config.log(config.logColorPhase, str, args);
    }

    public ConfigBase.FormatAndArgs logMessage(String str, Object... args) {
        return config.logMessage(str, args);
    }

    public void printStatus(ActorSystem.SystemLogger out, String head) {
        ConfigBase.FormatAndArgs fa = toStringStatus(head);
        int color = config.getLogColorDefault();
        out.log(true, color, fa.format, fa.args);
        if (state instanceof KelpStateRouter) {
            KelpStateRouter sr = (KelpStateRouter) state;
            printStatus(sr.getSplit(), out);
        }
    }

    public void printStatus(ActorSystem.SystemLogger out, String head, List<KelpRoutingSplit> newSplits) {
        ConfigBase.FormatAndArgs fa = toStringStatus(head);
        int color = config.getLogColorDefault();
        out.log(true, color, fa.format, fa.args);
        int i = 0;
        for (KelpRoutingSplit s : newSplits) {
            if (s instanceof KelpRoutingSplit.RoutingSplitLeaf) {
                ActorRef r = ((KelpRoutingSplit.RoutingSplitLeaf) s).getActor();
                out.log(true, color, " %d: %s %d:leaf: %s", i, s.getPath(), s.getDepth(), r);
                ++i;
            }
        }
    }

    public ConfigBase.FormatAndArgs toStringStatus(String head) {
        String str = toString();
        if (state instanceof KelpStateRouter) {
            KelpStateRouter sr = (KelpStateRouter) state;
            return logMessage("%s router %s \n" +
                            "   threshold=%,d height=%,d/%,d parallelRouting=%s",
                    head,
                    str,
                    mailboxThreshold(),
                    sr.getHeight(),
                    sr.getMaxHeight(),
                    !sr.isNonParallelRouting());
        } else if (state instanceof StateUnit) {
            return logMessage("%s leaf %s", head, str);
        } else {
            return logMessage("%s %s %s", state, head, str);
        }
    }

    @Override
    public String toStringContents() {
        String nm = super.toStringContents();
        return String.format("%s %s, %s, %s",
                nm.isEmpty() ? "" : (nm + ","),
                getSystem(),
                toStringState(),
                toStringMailboxStatus());
    }

    public String toStringState() {
        if (state instanceof KelpStateRouter) {
            return "router";
        } else if (state instanceof StateUnit) {
            return "leaf";
        } else if (state instanceof StateCanceled) {
            return "canceled";
        } else if (state == null){
            return "null";
        } else {
            return state.getClass().getName();
        }
    }

    public String toStringMailboxStatus() {
        return String.format("queue=%,d %s",
                getMailboxAsKelp().size(),
                getMailboxAsKelp().getEntries().stream()
                        .map(MailboxKelp.HistogramEntry::getTree)
                        .map(t -> String.format("leaf=%,d nonZeroLeaf=%,d valuesInTree=%,d treeHeight=%,d completed=%,d",
                                t.getLeafSize(),
                                t.getLeafSizeNonZero(),
                                t.getTreeSize(),
                                t.getTreeHeight(),
                                t.getCompleted().size()))
                        .collect(Collectors.joining(", ", "[", "]")));
    }

    protected void printStatus(KelpRoutingSplit s, ActorSystem.SystemLogger out) {
        String idt = "  ";
        if (s != null) {
            for (int i = 0; i < s.getDepth(); ++i) {
                idt += "  ";
            }
        }
        int color = config.getLogColorDefault();
        if (s == null) {
            out.log(true, color, "%s null", idt);
        } else if (s instanceof KelpRoutingSplit.RoutingSplitNode) {
            KelpRoutingSplit.RoutingSplitNode sn = (KelpRoutingSplit.RoutingSplitNode) s;
            out.log(true, color, "%s %d:node: %s proced=%,d history=%s", idt, sn.getDepth(), Arrays.stream(sn.getSplitPoints())
                .map(Objects::toString)
                .map(l -> l.length() > 100 ? l.substring(0, 100) + "..." : l)
                .collect(Collectors.joining(", ", "[", "]")),
                sn.getProcessCount(),
                sn.getHistory().toList().stream()
                    .map(h -> String.format("(%,d:%,d)", h.left.get(), h.right.get()))
                    .collect(Collectors.joining(", ", "{", "}")));
            printStatus(sn.getLeft(), out);
            printStatus(sn.getRight(), out);
        } else if (s instanceof KelpRoutingSplit.RoutingSplitLeaf) {
            ActorRef r = ((KelpRoutingSplit.RoutingSplitLeaf) s).getActor();
            out.log(true, color, "%s %d:leaf: proced=%,d %s", idt, s.getDepth(), s.getProcessCount(), r);
        }
    }

}
