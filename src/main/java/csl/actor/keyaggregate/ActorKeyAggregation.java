package csl.actor.keyaggregate;

import csl.actor.*;
import csl.actor.cluster.ActorPlacement;
import csl.actor.cluster.MailboxPersistable;
import csl.actor.cluster.PhaseShift;
import csl.actor.cluster.ResponsiveCalls;
import csl.actor.keyaggregate.KeyAggregationRoutingSplit.SplitPath;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public abstract class ActorKeyAggregation extends ActorDefault
        implements KeyHistogramsPersistable.HistogramTreePersistableConfig, PhaseShift.StageSupported, Cloneable {
    protected Config config;
    protected volatile ActorRef nextStage;

    protected volatile State state;

    public ActorKeyAggregation(ActorSystem system, String name, MailboxKeyAggregation mailbox, ActorBehavior behavior, Config config, State state) {
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
    public ActorKeyAggregation(ActorSystem system, String name, Config config, State state) {
        this(system, name, null, null, config, state);
        this.state = state;
        mailbox = initMailbox();
        behavior = initBehavior();
    }

    public ActorKeyAggregation(ActorSystem system, String name, Config config) {
        this(system, name, config, (State) null);
        state = initStateRouter();
    }

    public ActorKeyAggregation(ActorSystem system, String name) {
        this(system, name, (Config) null);
    }

    public ActorKeyAggregation(ActorSystem system, Config config) {
        this(system, null, config);
    }

    public ActorKeyAggregation(ActorSystem system) {
        this(system, null, (Config) null);
    }

    //////////////////////// config


    public Config getConfig() {
        return config;
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

    protected KeyAggregationStateRouter initStateRouter() {
        return new KeyAggregationStateRouter();
    }

    protected StateUnit initStateUnit(ActorRef router) {
        return new StateUnit(router);
    }

    @Override
    protected Mailbox initMailbox() {
        MailboxPersistable.PersistentFileManager m = getPersistentFile();
        return new MailboxKeyAggregation(mailboxThreshold(), mailboxTreeSize(),
                initMailboxDefault(m),
                initTreeFactory(m));
    }

    protected MailboxDefault initMailboxDefault(MailboxPersistable.PersistentFileManager m) {
        if (m != null) {
            return new MailboxPersistable(m,
                    persistMailboxSizeLimit(), persistMailboxOnMemorySize());
        } else {
            return new MailboxDefault();
        }
    }

    protected KeyHistograms initTreeFactory(MailboxPersistable.PersistentFileManager m) {
        if (m != null) {
            return new KeyHistogramsPersistable(this, m);
        } else {
            return KeyHistograms.DEFAULT;
        }
    }

    protected MailboxPersistable.PersistentFileManager getPersistentFile() {
        String path = persistMailboxPath();
        if (!path.isEmpty()) {
            return MailboxPersistable.getPersistentFile(system, ()->path);
        } else {
            return null;
        }
    }

    protected Mailbox initMailboxForClone() {
        return getMailboxAsKeyAggregation().create();
    }

    protected void initMerged(ActorKeyAggregation m) { }

    protected void initClone(ActorKeyAggregation original) { }

    @Override
    protected ActorBehaviorBuilderKeyAggregation behaviorBuilder() {
        return new ActorBehaviorBuilderKeyAggregation((ps) -> getMailboxAsKeyAggregation().initMessageEntries(ps));
    }

    public ActorKeyAggregation setAsUnit() {
        state = initStateUnit(null);
        return this;
    }

    ////////////////////////

    public MailboxKeyAggregation getMailboxAsKeyAggregation() {
        return (MailboxKeyAggregation) mailbox;
    }

    public ActorRef router() {
        if (state instanceof KeyAggregationStateRouter) {
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
        return isRouterParallelRouting() || getMailboxAsKeyAggregation().hasRemainingProcesses();
    }

    public boolean isRouterParallelRouting() {
        return state instanceof KeyAggregationStateRouter && !((KeyAggregationStateRouter) state).isNonParallelRouting();
    }

    //////////////////////// internal state

    public State getState() {
        return state;
    }

    public interface State {
        void processMessage(ActorKeyAggregation self, Message<?> message);
        boolean processMessagePhase(ActorKeyAggregation self, Message<?> message);
    }

    public static class StateUnit implements State, Serializable {
        protected ActorRef router;

        public StateUnit(ActorRef router) {
            this.router = router;
        }

        public ActorRef getRouter() {
            return router;
        }

        @Override
        public void processMessage(ActorKeyAggregation self, Message<?> message) {
            self.processMessageBehavior(message);
        }

        @Override
        public boolean processMessagePhase(ActorKeyAggregation self, Message<?> message) {
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

        ActorRef phaseTarget(ActorKeyAggregation self) {
            return router == null ? self : router;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + (router == null ? "" : ("(router=" + router + ")"));
        }
    }

    public static class StateCanceled implements State {
        protected ActorRef router;

        public StateCanceled(ActorRef router) {
            this.router = router;
        }

        public ActorRef getRouter() {
            return router;
        }

        @Override
        public void processMessage(ActorKeyAggregation self, Message<?> message) {
            router.tell(message.getData(), message.getSender());
        }

        @Override
        public boolean processMessagePhase(ActorKeyAggregation self, Message<?> message) {
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
        if (getMailboxAsKeyAggregation().processHistogram()) {
            return true;
        }
        return super.processMessageNext();
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
        processPrune();
        Object data = message.getData();
        if (data instanceof MailboxKeyAggregation.TraversalProcess) {
            getMailboxAsKeyAggregation()
                    .processTraversal(this,
                            ((MailboxKeyAggregation.TraversalProcess) data).entryId,
                            this::nextConsumingSize);
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
                data instanceof MailboxKeyAggregation.TraversalProcess ||
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
        return ResponsiveCalls.sendTaskConsumer(this, (a, s) -> a.nextStage = nextStage);
    }


    public int nextConsumingSize(long size) {
        int consuming = (int) Math.min(Integer.MAX_VALUE, size);
        int rrt = reduceRuntimeCheckingThreshold();
        if (consuming > rrt) { //refer free memory size
            Runtime rt = Runtime.getRuntime();
            consuming = (int) Math.min(consuming,
                    Math.max(rrt, (rt.maxMemory() - rt.totalMemory()) * reduceRuntimeRemainingBytesToSizeRatio()));
            if (config.logSplit) {
                config.log(String.format("%s reduceSize: %,d -> %,d", this, size, consuming));
            }
        }
        return consuming;
    }


    public void processPrune() {
        getMailboxAsKeyAggregation().prune(
                pruneGreaterThanLeaf(),
                pruneLessThanNonZeroLeafRate());
    }

    public void processPhaseEnd(Object phaseKey) {
        getMailboxAsKeyAggregation()
                .processPhase(this, phaseKey, this::nextConsumingSize);
    }

    /////////////////////////// methods for state

    public KeyAggregationRoutingSplit internalCreateSplitNode(KeyAggregationRoutingSplit.SplitOrMergeContext context,
                                                              KeyAggregationRoutingSplit old,
                                                              ActorKeyAggregation target, SplitPath path, int height) {
        try {
            target.getMailboxAsKeyAggregation().lockRemainingProcesses();

            ActorRef routerRef = router();
            ActorKeyAggregation a1 = target.internalCreateClone(routerRef);
            ActorKeyAggregation a2 = target.internalCreateClone(routerRef);
            List<Object> splitPoints = target.getMailboxAsKeyAggregation()
                    .splitMessageHistogramIntoReplicas(a1.getMailboxAsKeyAggregation(), a2.getMailboxAsKeyAggregation());
            if (routerRef != target) {
                target.internalCancel();
            }
            return internalCreateSplitNode(context, old, splitPoints, a1, a2, path, height);
        } finally {
            target.getMailboxAsKeyAggregation().unlockRemainingProcesses(target);
        }
    }

    public KeyAggregationRoutingSplit internalCreateSplitNode(KeyAggregationRoutingSplit.SplitOrMergeContext context,
                                                              KeyAggregationRoutingSplit old,
                                                              List<Object> splitPoints, ActorKeyAggregation a1, ActorKeyAggregation a2, SplitPath path, int height) {
        KeyAggregationRoutingSplit s1 = internalCreateSplitLeaf(context, old, a1, path.add(true), height);
        KeyAggregationRoutingSplit s2 = internalCreateSplitLeaf(context, old, a2, path.add(false), height);

        return newSplitNode(splitPoints, s1, s2, path);
    }

    public KeyAggregationRoutingSplit internalCreateSplitLeaf(KeyAggregationRoutingSplit.SplitOrMergeContext context,
                                                              KeyAggregationRoutingSplit old,
                                                              ActorKeyAggregation actor, SplitPath path, int height) {
        if (path.depth() >= height) {
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
                return newSplitLeaf(context, old, actor, path).split(context, height);
            }
        }
    }

    private KeyAggregationRoutingSplit newSplitLeaf(KeyAggregationRoutingSplit.SplitOrMergeContext context,
                                                KeyAggregationRoutingSplit old,
                                                ActorRef actor, SplitPath path) {
        KeyAggregationRoutingSplit s = newSplitLeaf(actor, path);
        context.split(s, old);
        return s;
    }

    public KeyAggregationRoutingSplit newSplitNode(List<Object> splitPoints, KeyAggregationRoutingSplit s1, KeyAggregationRoutingSplit s2, SplitPath path) {
        return new KeyAggregationRoutingSplit.RoutingSplitNode(splitPoints, s1, s2, path, historyEntrySize());
    }

    public KeyAggregationRoutingSplit.RoutingSplitLeaf newSplitLeaf(ActorRef actor, SplitPath path) {
        return new KeyAggregationRoutingSplit.RoutingSplitLeaf(actor, path);
    }


    public Message<?> internalPollForParallelRouting() {
        return mailbox.poll();
    }

    public void internalMerge(ActorKeyAggregation merged) {
        getMailboxAsKeyAggregation().lockRemainingProcesses();
        merged.getMailboxAsKeyAggregation().lockRemainingProcesses();
        getMailboxAsKeyAggregation()
                .merge(merged.getMailboxAsKeyAggregation());
        merged.internalCancel();
        try {
            initMerged(merged);
        } finally {
            merged.getMailboxAsKeyAggregation().unlockRemainingProcesses(merged);
            getMailboxAsKeyAggregation().unlockRemainingProcesses(this);
        }
    }

    public ActorKeyAggregation internalCreateClone(ActorRef router) {
        try {
            ActorKeyAggregation a = (ActorKeyAggregation) super.clone();
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

    public ActorKeyAggregation toLocal(ActorRef ref) {
        if (ref instanceof ActorKeyAggregation) {
            return (ActorKeyAggregation) ref;
        }
        try {
            ActorKeyAggregationSerializable state = ResponsiveCalls.sendTask(system, ref,
                    new CallableToLocalSerializable())
                    .get(toLocalWaitMs(), TimeUnit.MILLISECONDS);
            return state.create(system, -1);
        } catch (Exception ex) {

            ex.printStackTrace();
            return null;
        }
    }

    public static class CallableToLocalSerializable implements CallableMessage<ActorKeyAggregation, ActorKeyAggregationSerializable> {
        @Override
        public ActorKeyAggregationSerializable call(ActorKeyAggregation self, ActorRef sender) {
            self.getMailboxAsKeyAggregation().lockRemainingProcesses();
            try {
                return self.toSerializable(-1);
            } finally {
                self.getMailboxAsKeyAggregation().terminateAfterSerialized();
                self.internalCancel();
                self.getMailboxAsKeyAggregation().unlockRemainingProcesses(self);
            }
        }
    }

    public void internalCancel() { //remaining messages are processed by the canceled state
        state = new StateCanceled(router());
        router().tell(new CancelChange(this, CanceledChangeType.CancelAdded));
    }

    public static class CancelChange implements Serializable, MessageNoRouting {
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
        return ResponsiveCalls.<ActorKeyAggregation, Integer>sendTask(getSystem(), router(), (a, sender) -> {
            State state = a.getState();
            if (state instanceof KeyAggregationStateRouter) {
                return ((KeyAggregationStateRouter) state).getMaxHeight(a);
            } else {
                return 1;
            }
        });
    }

    public CompletableFuture<CallableMessage.CallableResponseVoid> routerSplit(int height) {
        return ResponsiveCalls.<ActorKeyAggregation>sendTaskConsumer(getSystem(), router(), (a, sender) -> {
            State state = a.state;
            if (state instanceof KeyAggregationStateRouter) {
                ((KeyAggregationStateRouter) state).split(a, height);
            }
        });
    }

    public CompletableFuture<CallableMessage.CallableResponseVoid> routerMergeInactive() {
        return ResponsiveCalls.<ActorKeyAggregation>sendTaskConsumer(getSystem(), router(), (a, sender) -> {
            State state = a.state;
            if (state instanceof KeyAggregationStateRouter) {
                ((KeyAggregationStateRouter) state).mergeInactive(a);
            }
        });
    }

    public CompletableFuture<CallableMessage.CallableResponseVoid> routerSplitOrMerge(int height) {
        return ResponsiveCalls.<ActorKeyAggregation>sendTaskConsumer(getSystem(), router(), (a, sender) -> {
            State state = a.state;
            if (state instanceof KeyAggregationStateRouter) {
                ((KeyAggregationStateRouter) state).splitOrMerge(a, height);
            }
        });
    }

    protected void afterSplitOrMerge(KeyAggregationRoutingSplit.SplitOrMergeContextDefault context) {
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

    public static ActorRef place(ActorPlacement placement, ActorKeyAggregation a) {
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

    public ActorKeyAggregationSerializable toSerializable(long num) {
        return initSerializableState(newSerializableState(), num);
    }

    protected ActorKeyAggregationSerializable newSerializableState() {
        return new ActorKeyAggregationSerializable();
    }

    protected ActorKeyAggregationSerializable initSerializableState(ActorKeyAggregationSerializable state, long num) {
        state.actorType = getClass();
        String n = getName();
        int si = n.lastIndexOf("#");
        if (si >= 0) {
            n = n.substring(0, si);
        }
        state.name = String.format("%s#%d", n, num);
        state.config = config;
        state.router = router();
        state.nextStage = nextStage;
        MailboxKeyAggregation r = getMailboxAsKeyAggregation();
        r.serializeTo(state);
        state.internalState = toSerializableInternalState();
        return state;
    }

    protected Serializable toSerializableInternalState() {
        return null;
    }

    protected void initSerializedInternalState(Serializable s) { }

    public static class ActorKeyAggregationSerializable implements Serializable {
        public Class<? extends ActorKeyAggregation> actorType;
        public String name;
        public Message<?>[] messages;
        public List<KeyHistograms.HistogramTree> histograms;
        public Config config;
        public ActorRef router;
        public Serializable internalState;
        public ActorRef nextStage;

        public ActorKeyAggregation create(ActorSystem system, long num) throws Exception {
            return init(create(system, name(num), config(), state(router)));
        }

        protected String name(long num) {
            return (num < 0 ? null : String.format("%s_%d", name, num));
        }

        protected Config config() {
            return config == null ? Config.CONFIG_DEFAULT : config;
        }

        protected State state(ActorRef router) {
            return new StateUnit(router);
        }

        protected ActorKeyAggregation init(ActorKeyAggregation a) {
            a.getMailboxAsKeyAggregation().deserializeFrom(this);
            a.initSerializedInternalState(internalState);
            a.setNextStage(nextStage);
            return a;
        }

        protected ActorKeyAggregation create(ActorSystem system, String name, Config config, State state) throws Exception {
            return actorType.getConstructor(ActorSystem.class, String.class, Config.class, State.class)
                    .newInstance(system, name, config, state);
        }
    }

    /////////////////////////// print status

    public void printStatus() {
        printStatus("");
    }

    public void printStatus(String head) {
        printStatus(config.getLogOut(), head);
    }

    public void printStatus(String head, List<KeyAggregationRoutingSplit> newSplits) {
        printStatus(config.getLogOut(), head, newSplits);
    }

    public void log(String str) {
        config.log(str);
    }

    @Override
    public void logPhase(String str) {
        config.log(config.logColorPhase, str);
    }

    public String logMessage(String str) {
        return config.logMessage(str);
    }

    public void printStatus(PrintWriter out, String head) {
        println(out, toStringStatus(head));
        if (state instanceof KeyAggregationStateRouter) {
            KeyAggregationStateRouter sr = (KeyAggregationStateRouter) state;
            printStatus(sr.getSplit(), out);
        }
    }

    public void printStatus(PrintWriter out, String head, List<KeyAggregationRoutingSplit> newSplits) {
        println(out, toStringStatus(head));
        int i = 0;
        for (KeyAggregationRoutingSplit s : newSplits) {
            if (s instanceof KeyAggregationRoutingSplit.RoutingSplitLeaf) {
                ActorRef r = ((KeyAggregationRoutingSplit.RoutingSplitLeaf) s).getActor();
                println(out, String.format(" %d: %s %d:leaf: %s", i, s.getPath(), s.getDepth(), r));
                ++i;
            }
        }
    }

    public String toStringStatus(String head) {
        String str = toString();
        if (state instanceof KeyAggregationStateRouter) {
            KeyAggregationStateRouter sr = (KeyAggregationStateRouter) state;
            return logMessage(String.format("%s router %s \n" +
                            "   threshold=%,d height=%,d/%,d parallelRouting=%s",
                    head,
                    str,
                    mailboxThreshold(),
                    sr.getHeight(),
                    sr.getMaxHeight(),
                    !sr.isNonParallelRouting()));
        } else if (state instanceof StateUnit) {
            return logMessage(String.format("%s leaf %s", head, str));
        } else {
            return logMessage(String.format("%s %s %s", state, head, str));
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
        if (state instanceof KeyAggregationStateRouter) {
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
                getMailboxAsKeyAggregation().size(),
                getMailboxAsKeyAggregation().getEntries().stream()
                        .map(MailboxKeyAggregation.HistogramEntry::getTree)
                        .map(t -> String.format("leaf=%,d nonZeroLeaf=%,d valuesInTree=%,d treeHeight=%,d completed=%,d",
                                t.getLeafSize(),
                                t.getLeafSizeNonZero(),
                                t.getTreeSize(),
                                t.getTreeHeight(),
                                t.getCompleted().size()))
                        .collect(Collectors.joining(", ", "[", "]")));
    }

    protected void printStatus(KeyAggregationRoutingSplit s, PrintWriter out) {
        String idt = "  ";
        if (s != null) {
            for (int i = 0; i < s.getDepth(); ++i) {
                idt += "  ";
            }
        }
        if (s == null) {
            println(out, String.format("%s null", idt));
        } else if (s instanceof KeyAggregationRoutingSplit.RoutingSplitNode) {
            KeyAggregationRoutingSplit.RoutingSplitNode sn = (KeyAggregationRoutingSplit.RoutingSplitNode) s;
            println(out, String.format("%s %d:node: %s history=%s", idt, sn.getDepth(), sn.getSplitPoints().stream()
                .map(Objects::toString)
                .map(l -> l.length() > 100 ? l.substring(0, 100) + "..." : l)
                .collect(Collectors.joining(", ", "[", "]")),
                sn.getHistory().toList().stream()
                    .map(h -> String.format("(%,d:%,d)", h.left.get(), h.right.get()))
                    .collect(Collectors.joining(", ", "{", "}"))));
            printStatus(sn.getLeft(), out);
            printStatus(sn.getRight(), out);
        } else if (s instanceof KeyAggregationRoutingSplit.RoutingSplitLeaf) {
            ActorRef r = ((KeyAggregationRoutingSplit.RoutingSplitLeaf) s).getActor();
            println(out, String.format("%s %d:leaf: %s", idt, s.getDepth(), r));
        }
    }

    public void println(PrintWriter out, String line) {
        config.println(out, line);
    }

}
