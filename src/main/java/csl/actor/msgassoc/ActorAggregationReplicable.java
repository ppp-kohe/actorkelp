package csl.actor.msgassoc;

import csl.actor.*;
import csl.actor.msgassoc.MailboxAggregationReplicable.MailboxStatus;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class ActorAggregationReplicable extends ActorAggregation implements Cloneable {
    protected volatile State state;

    public ActorAggregationReplicable(ActorSystem system, String name, MailboxAggregationReplicable mailbox, ActorBehavior behavior) {
        super(system, name, mailbox, behavior);
        state = initStateRouter();
    }

    public ActorAggregationReplicable(ActorSystem system, String name, ActorBehavior behavior) {
        super(system, name, behavior);
        state = initStateRouter();
    }

    public ActorAggregationReplicable(ActorSystem system, ActorBehavior behavior) {
        super(system, behavior);
        state = initStateRouter();
    }

    public ActorAggregationReplicable(ActorSystem system, String name, Config config) {
        super(system, name, config);
        state = initStateRouter();
    }

    public ActorAggregationReplicable(ActorSystem system, String name) {
        super(system, name);
        state = initStateRouter();
    }

    public ActorAggregationReplicable(ActorSystem system, Config config) {
        super(system, config);
        state = initStateRouter();
    }

    public ActorAggregationReplicable(ActorSystem system) {
        super(system);
        state = initStateRouter();
    }

    //////////////////////// config


    public Config getConfig() {
        return config;
    }

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

    protected StateSplitRouter initStateRouter() {
        return new StateSplitRouter();
    }

    @Override
    protected void initMailbox() {
        MailboxPersistable.PersistentFileManager m = getPersistentFile();
        mailbox = new MailboxAggregationReplicable(mailboxThreshold(), mailboxTreeSize(),
                initMailboxDefault(m),
                initTreeFactory(m));
    }

    protected void initMailboxForClone() {
        mailbox = getMailboxAsReplicable().create();
    }

    protected void initMerged(ActorAggregationReplicable m) { }

    protected void initClone(ActorAggregationReplicable original) { }

    ////////////////////////

    public MailboxAggregationReplicable getMailboxAsReplicable() {
        return (MailboxAggregationReplicable) mailbox;
    }

    public ActorRef router() {
        if (state instanceof StateSplitRouter) {
            return this;
        } else if (state instanceof StateLeaf) {
            return ((StateLeaf) state).getRouter();
        } else if (state instanceof StateDisabled) {
            return ((StateDisabled) state).getRouter();
        } else {
            return null;
        }
    }

    public boolean hasRemainingProcesses() {
        return isRouterParallelRouting() || getMailboxAsReplicable().hasRemainingProcesses();
    }

    public boolean isRouterParallelRouting() {
        return state instanceof StateSplitRouter && !((StateSplitRouter) state).isNonParallelRouting();
    }

    //////////////////////// internal state

    public State getState() {
        return state;
    }

    public interface State {
        void processMessage(ActorAggregationReplicable self, Message<?> message);
        boolean processMessagePhase(ActorAggregationReplicable self, Message<?> message);
    }

    public static class StateSplitRouter implements State {
        protected volatile Split split;
        protected Random random = new Random();
        protected int height = 0;
        protected int maxHeight = -1;

        protected volatile boolean parallelRouting1;
        protected volatile boolean parallelRouting2;
        protected volatile boolean needClearHistory;
        protected volatile boolean logAfterParallelRouting;

        protected Map<Object, PhaseShift.PhaseEntry> phase = new HashMap<>();
        protected Set<ActorRef> disabled = new HashSet<>();

        public void split(ActorAggregationReplicable self, int height) {
            this.height = height;
            if (split == null) { //root
                split = self.internalCreateSplitLeaf(self, 0, height);
            } else {
                split = split.split(self, height);
            }
            if (self.logSplit()) {
                self.printStatus("after split: height=" + height);
            }
        }

        public void mergeInactive(ActorAggregationReplicable self) {
            split = split.mergeInactive(self);
            split.clearHistory();
            if (self.logSplit()) {
                self.printStatus("after mergeInactive");
            }
        }

        public void splitOrMerge(ActorAggregationReplicable self, int height) {
            this.height = height;
            if (split == null) {
                split = self.internalCreateSplitLeaf(self, 0, height);
            } else {
                split = split.splitOrMerge(self, height);
            }
            if (self.logSplit()) {
                self.printStatus("after splitOrMerge: height=" + height);
            }
        }

        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            MailboxStatus status;
            if (isNonParallelRouting() &&
                    (status = self.getMailboxAsReplicable().getStatus(self.lowerBoundThresholdFactor())).isExcessive()) {

                int maxHeight = getMaxHeight(self);
                MailboxAggregationReplicable m = self.getMailboxAsReplicable();
                int size = m.size();

                if (status.equals(MailboxStatus.Exceeded)) {
                    int h = nextHeight(maxHeight, size, self.minSizeOfEachMailboxSplit());
                    splitAndParallelRouting(self, m, message, h);
                } else if (split != null && status.equals(MailboxStatus.Few) &&
                        split.isHistoryExceeded((int) (self.historyExceededLimit()))) {
                    mergeInactive(self, m, message);
                } else {
                    route(self, m, message, false);
                }
            } else {
                if (needClearHistory) {
                    needClearHistory = false;
                    if (split != null && isNonParallelRouting()) {
                        split.clearHistory();
                    }
                }
                if (logAfterParallelRouting && isNonParallelRouting()) {
                    logAfterParallelRouting = false;
                    if (self.logSplit()) {
                        self.printStatus("after parallelRouting");
                    }
                }
                route(self, self.getMailboxAsReplicable(), message, false);
            }
        }

        public int getMaxHeight() {
            return maxHeight;
        }

        public int getMaxHeight(ActorAggregationReplicable self) {
            if (maxHeight < 0) {
                maxHeight = initMaxHeight(self);
            }
            return maxHeight;
        }

        protected int initMaxHeight(ActorAggregationReplicable self) {
            int th = getTotalThreads(self, self.getPlacement());
            return Math.max(1, (int) (Math.log(th) / Math.log(2)));
        }

        protected int getTotalThreads(ActorAggregationReplicable self, ActorPlacement placement) {
            if (placement instanceof ActorPlacement.PlacemenActor) {
                return ((ActorPlacement.PlacemenActor) placement).getTotalThreads();
            } else {
                return self.getSystem().getThreads();
            }
        }

        public boolean isNonParallelRouting() {
            return !(parallelRouting1 || parallelRouting2);
        }

        public int nextHeight(int maxHeight, int size, int eachSizeOfSplits) {
            int h = 1;
            while ((size >>> h) > eachSizeOfSplits && h < maxHeight) {
                ++h;
            }
            return Math.min(Math.max(height, h), maxHeight);
        }

        protected void splitAndParallelRouting(ActorAggregationReplicable self, MailboxAggregationReplicable m, Message<?> message,
                                               int height) {
            split(self, height);
            route(self, m, message, false);
            startParallelRouting(self);
        }

        public void startParallelRouting(ActorAggregationReplicable self) {
            int max = Math.min(self.getMailboxAsReplicable().size(), self.maxParallelRouting());

            needClearHistory = true;
            logAfterParallelRouting = self.logSplit();
            if (split != null) {
                parallelRouting1 = true;
                parallelRouting2 = true;
                self.getSystem().execute(() -> {
                    try {
                        routeRemaining(self, max);
                    } finally {
                        parallelRouting1 = false;
                    }
                });
                self.getSystem().execute(() -> {
                    try {
                        routeRemaining(self, max);
                    } finally {
                        parallelRouting2 = false;
                    }
                });
            }
        }

        protected void mergeInactive(ActorAggregationReplicable self, MailboxAggregationReplicable m, Message<?> message) {
            mergeInactive(self);
            route(self, m, message, false);
        }

        protected void routeRemaining(ActorAggregationReplicable self, int max) {
            MailboxAggregationReplicable m = self.getMailboxAsReplicable();
            int i = 0;
            List<Message<?>> noRoutingTops = new ArrayList<>(16);
            while (!m.isEmpty() && i < max) {
                Message<?> msg = self.internalPollForParallelRouting();
                if (msg != null) {
                    if (self.isNoRoutingMessage(msg)) {
                        if (noRoutingTops.contains(msg)) {
                            break;
                        } else if (noRoutingTops.size() < 16) {
                            noRoutingTops.add(msg);
                        }
                        self.processMessageDelayWhileParallelRouting(msg);
                    } else {
                        route(self, m, msg, true);
                    }
                }
                ++i;
            }
        }

        protected void route(ActorAggregationReplicable self, MailboxAggregationReplicable m, Message<?> message, boolean fromParallelRouting) {
            if (split == null) {
                if (fromParallelRouting) {
                    self.getSystem().send(message);
                } else {
                    self.processMessageBehavior(message);
                }
            } else {
                MailboxAggregation.HistogramSelection selection = m.selectTable(message.getData());
                Object key = m.extractKey(selection, message);
                split.process(self, this, key, selection, message);
            }
        }

        public Random getRandom() {
            return random;
        }

        /** @return implementation field getter */
        public Split getSplit() {
            return split;
        }

        /** @return implementation field getter */
        public int getHeight() {
            return height;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" +
                    "split=" + split + ", height=" + height
                    + ", parallelRouting1=" + this.parallelRouting1
                    + ", parallelRouting2=" + this.parallelRouting2
                    + ", needClearHistory=" + this.needClearHistory +
                    ')';
        }

        @Override
        public boolean processMessagePhase(ActorAggregationReplicable self, Message<?> message) {
            Object val = message.getData();
            if (val instanceof PhaseShift) {
                processMessagePhaseShift(self, message, (PhaseShift) val);
                return true;
            } else if (val instanceof PhaseShift.PhaseShiftIntermediate) {
                processMessagePhaseShiftIntermediate(self, message, (PhaseShift.PhaseShiftIntermediate) val);
                return true;
            } else if (val instanceof DisabledChange) {
                processMessageDisabledChange(self, message, (DisabledChange) val);
                return true;
            } else {
                return false;
            }
        }

        protected void processMessagePhaseShift(ActorAggregationReplicable self, Message<?> message, PhaseShift ps) {
            self.logPhase("#phase        start: " + ps.getKey() + " : " + self + " : target=" + ps.getTarget());
            PhaseShift.PhaseEntry e = phase.computeIfAbsent(ps.getKey(), PhaseShift.PhaseEntry::new);
            e.setOriginAndSender(ps, message.getSender());
            e.startRouter(self); //router only delivers to disabled actors without traversal
        }

        protected void processMessagePhaseShiftIntermediate(ActorAggregationReplicable self, Message<?> message, PhaseShift.PhaseShiftIntermediate ps) {
            PhaseShift.PhaseEntry finish = phase.computeIfAbsent(ps.getKey(), PhaseShift.PhaseEntry::new);
            if (ps.getActor() == self && ps.getType().equals(PhaseShift.PhaseShiftIntermediateType.PhaseIntermediateFinishLeaf)) {
                self.processPhaseEnd(ps.getKey());
            }
            if (finish.processIntermediate(self, ps)) {
                phase.remove(ps.getKey());
            }
        }

        protected void processMessageDisabledChange(ActorAggregationReplicable self, Message<?> message, DisabledChange changed) {
            Object data = changed.getData();
            ActorRef disabledActor = changed.getDisabledActor();
            if (data.equals(DisabledChangeType.DisabledAdded)) {
                disabled.add(disabledActor);
                disabledActor.tell(new DisabledChange(disabledActor, DisabledChangeType.DisabledFinished), self);
            } else if (data.equals(DisabledChangeType.DisabledFinished)) {
                disabled.remove(disabledActor);
            }
        }

        public Set<ActorRef> getDisabled() {
            return disabled;
        }
    }

    public static class StateLeaf implements State, Serializable {
        protected ActorRef router;

        public StateLeaf(ActorRef router) {
            this.router = router;
        }

        public ActorRef getRouter() {
            return router;
        }

        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            self.processMessageBehavior(message);
        }

        @Override
        public boolean processMessagePhase(ActorAggregationReplicable self, Message<?> message) {
            Object val = message.getData();
            if (val instanceof PhaseShift) {
                ((PhaseShift) val).accept(self, message.getSender());
                return true;
            } else if (val instanceof PhaseShift.PhaseShiftIntermediate) {
                PhaseShift.PhaseShiftIntermediate event = (PhaseShift.PhaseShiftIntermediate) val;
                if (event.getType().equals(PhaseShift.PhaseShiftIntermediateType.PhaseIntermediateFinishLeaf)) {
                    self.processPhaseEnd(event.getKey());
                }
                event.accept(self, router, message.getSender());
                return true;
            } else if (val instanceof DisabledChange) {
                processMessage(self, message);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" +
                    "router=" + router +
                    ')';
        }
    }

    public static class StateDisabled implements State {
        protected ActorRef router;

        public StateDisabled(ActorRef router) {
            this.router = router;
        }

        public ActorRef getRouter() {
            return router;
        }

        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            router.tell(message.getData(), message.getSender());
        }

        @Override
        public boolean processMessagePhase(ActorAggregationReplicable self, Message<?> message) {
            Object val = message.getData();
            if (val instanceof PhaseShift) {
                ((PhaseShift) val).accept(self, message.getSender());
                return true;
            } else if (val instanceof PhaseShift.PhaseShiftIntermediate) {
                ((PhaseShift.PhaseShiftIntermediate) val).accept(self, router, message.getSender());
                return true;
            } else if (val instanceof DisabledChange) {
                processMessage(self, message);
                return true;
            } else {
                return false;
            }
        }

    }

    ////////////////////// Split

    public interface Split {
        void process(ActorAggregationReplicable router, StateSplitRouter stateRouter,
                     Object key, MailboxAggregation.HistogramSelection selection, Message<?> message);
        int getDepth();

        default void clearHistory() {}

        default boolean isHistoryExceeded(int limit) {
            return false;
        }

        Split split(ActorAggregationReplicable router, int height);
        Split mergeInactive(ActorAggregationReplicable router);
        SplitLeaf mergeIntoLeaf(ActorAggregationReplicable router);

        Split splitOrMerge(ActorAggregationReplicable router, int height);

        Split adjustDepth(int dep);

        default <ActorType extends Actor> void accept(ActorType actor, ActorRef sender, ActorVisitor<ActorType> v) {
            v.visitRouter(actor, sender, this);
        }
    }

    public static class SplitLeaf implements Split {
        protected ActorRef actor;
        protected int depth;

        public SplitLeaf(ActorRef actor, int depth) {
            this.actor = actor;
            this.depth = depth;
        }

        @Override
        public void process(ActorAggregationReplicable router, StateSplitRouter stateRouter,
                            Object key, MailboxAggregation.HistogramSelection selection, Message<?> message) {
            actor.tell(message.getData(), message.getSender());
        }

        @Override
        public SplitLeaf adjustDepth(int dep) {
            return newLeaf(actor, depth + dep);
        }

        public SplitLeaf newLeaf(ActorRef actor, int depth) {
            return new SplitLeaf(actor, depth);
        }

        @Override
        public int getDepth() {
            return depth;
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public Split split(ActorAggregationReplicable router, int height) {
            if (depth >= height) {
                return this;
            }
            ActorAggregationReplicable self = router.toLocal(getActor());
            if (self == null) {
                return this;
            }

            try {
                self.getMailboxAsReplicable().lockRemainingProcesses();

                ActorRef routerRef = router.router();
                ActorAggregationReplicable a1 = self.internalCreateClone(routerRef);
                ActorAggregationReplicable a2 = self.internalCreateClone(routerRef);
                List<Object> splitPoints = self.getMailboxAsReplicable()
                        .splitMessageTableIntoReplicas(a1.getMailboxAsReplicable(), a2.getMailboxAsReplicable());
                if (router != self) {
                    self.internalDisable();
                }
                return router.internalCreateSplitNode(splitPoints, a1, a2, depth, height);
            } finally {
                self.getMailboxAsReplicable().unlockRemainingProcesses(self);
            }
        }

        @Override
        public Split mergeInactive(ActorAggregationReplicable router) {
            return this;
        }

        @Override
        public SplitLeaf mergeIntoLeaf(ActorAggregationReplicable router) {
            return this;
        }

        public SplitLeaf merge(ActorAggregationReplicable router, SplitLeaf leaf, int depthAdjust) {
            ActorRef a1 = getActor();
            ActorRef a2 = leaf.getActor();

            if (hasRemainingProcesses(router, a1) || hasRemainingProcesses(router, a2)) {
                return this;
            }

            if (a1 instanceof ActorAggregationReplicable) { //local
                if (a2 instanceof ActorAggregationReplicable) {
                    ((ActorAggregationReplicable) a1).internalMerge((ActorAggregationReplicable) a2);
                    return newLeaf(a1, depth + depthAdjust);
                } else { //remote a2
                    ActorAggregationReplicable l2 = router.toLocal(a2);
                    if (l2 != null) {
                        ((ActorAggregationReplicable) a1).internalMerge(l2);
                    }
                    return newLeaf(a1, depth + depthAdjust);
                }
            } else if (a2 instanceof ActorAggregationReplicable) { //remote a1, local a2
                ActorAggregationReplicable l1 = router.toLocal(a1);
                if (l1 != null) {
                    ((ActorAggregationReplicable) a2).internalMerge(l1);
                }
                return newLeaf(a2, depth + depthAdjust);
            } else { //both remote
                a1.tell(CallableMessage.callableMessageConsumer((self, sender) -> {
                    ActorAggregationReplicable l2 = ((ActorAggregationReplicable) self).toLocal(a2);
                    if (l2 != null) {
                        ((ActorAggregationReplicable) self).internalMerge(l2);
                    }
                }), null);
                return newLeaf(a1, depth + depthAdjust);
            }
        }

        public boolean hasRemainingProcesses(ActorAggregationReplicable router, ActorRef a) {
            try {
                return ResponsiveCalls.sendTask(router.getSystem(), a, (self, sender) ->
                        ((ActorAggregationReplicable) self).hasRemainingProcesses())
                        .get(1, TimeUnit.SECONDS);
            } catch (Exception ex) {
                router.log("#hasRemainingProcesses: busy " + a + " : " + ex);
                return true;
            }
        }

        @Override
        public Split splitOrMerge(ActorAggregationReplicable router, int height) {
            if (depth < height) {
                return split(router, height);
            } else {
                return this;
            }
        }

        @Override
        public <ActorType extends Actor> void accept(ActorType actor, ActorRef sender, ActorVisitor<ActorType> v) {
            v.visitRouterLeaf(actor, sender, this);
        }
    }

    public static class SplitNode implements Split {
        protected List<Object> splitPoints;
        protected Split left;
        protected Split right;
        protected int depth;
        protected RoutingHistory history;

        public SplitNode(List<Object> splitPoints, Split left, Split right, int depth, int historyEntrySize) {
            this.splitPoints = splitPoints;
            this.left = left;
            this.right = right;
            this.depth = depth;
            history = initRoutingHistory(historyEntrySize);
        }

        public SplitNode(List<Object> splitPoints, Split left, Split right, int depth, RoutingHistory history) {
            this.splitPoints = splitPoints;
            this.left = left;
            this.right = right;
            this.depth = depth;
            this.history = history;
        }

        public SplitNode newNode(Split left, Split right, int depth) {
            return new SplitNode(splitPoints, left, right, depth, history);
        }

        protected SplitNode newNodeOrThis(Split left, Split right, int depth) {
            if (left != this.left || right != this.right || this.depth != depth) {
                return newNode(left, right, depth);
            } else {
                return this;
            }
        }

        @Override
        public void process(ActorAggregationReplicable router, StateSplitRouter stateRouter,
                            Object key, MailboxAggregation.HistogramSelection selection, Message<?> message) {
            if (select(router, stateRouter, key, selection, message)) {
                countHistory(true, router);
                left.process(router, stateRouter, key, selection, message);
            } else {
                countHistory(false, router);
                right.process(router, stateRouter, key, selection, message);
            }
        }

        protected void countHistory(boolean left, ActorAggregationReplicable router) {
            if (left) {
                history.left.getAndIncrement();
            } else {
                history.right.getAndIncrement();
            }
            if (history.total() > router.historyEntryLimit()) {
                history = history.next.clear();
            }
        }

        protected boolean select(ActorAggregationReplicable self, StateSplitRouter router,
                                 Object key, MailboxAggregation.HistogramSelection selection, Message<?> message) {
            if (selection == null) {
                return router.getRandom().nextBoolean();
            } else {
                Object point = splitPoints.get(selection.entryId);
                if (point == null) { //the first arriving key becomes splitPoint
                    synchronized (this) {
                        point = splitPoints.get(selection.entryId);
                        if (point == null) {
                            splitPoints.set(selection.entryId, key);
                            point = key;
                        }
                    }
                }
                return self.getMailboxAsReplicable().compare(selection.entryId, key, point);
            }
        }

        @Override
        public SplitNode adjustDepth(int dep) {
            return newNode(left, right, dep + depth);
        }

        @Override
        public int getDepth() {
            return depth;
        }

        public Split getLeft() {
            return left;
        }

        public Split getRight() {
            return right;
        }

        /** @return implementation field getter */
        public List<Object> getSplitPoints() {
            return splitPoints;
        }

        public RoutingHistory getHistory() {
            return history;
        }

        public RoutingHistory initRoutingHistory(int n) {
            RoutingHistory h = new RoutingHistory();
            history = h;
            for (int i = 0; i < n - 1; ++i) {
                h.next = new RoutingHistory();
                h = h.next;
            }
            h.next = history;
            return history;
        }

        @Override
        public void clearHistory() {
            this.history.clearAll();
            left.clearHistory();
            right.clearHistory();
        }

        @Override
        public boolean isHistoryExceeded(int limit) {
            return history.isExceeded(limit);
        }

        @Override
        public Split split(ActorAggregationReplicable router, int height) {
            if (depth < height) {
                return newNodeOrThis(
                        left.split(router, height),
                        right.split(router, height), depth);
            }
            return this;
        }

        @Override
        public Split mergeInactive(ActorAggregationReplicable router) {
            float r = history.ratioAll();
            float limit = router.mergeRatioThreshold();
            if (r > (1 - limit)) { //into left
                return merge(router, true,
                        left.mergeInactive(router),
                        right.mergeIntoLeaf(router));
            } else if (r < limit) { //into right
                return merge(router, false,
                        left.mergeIntoLeaf(router),
                        right.mergeInactive(router));
            } else {
                return newNodeOrThis(
                        left.mergeInactive(router),
                        right.mergeInactive(router), depth);
            }
        }

        protected Split merge(ActorAggregationReplicable router, boolean intoLeft, Split left, Split right) {
            boolean leftIsLeaf = (left instanceof SplitLeaf);
            boolean rightIsLeaf = (right instanceof SplitLeaf);
            if (leftIsLeaf && rightIsLeaf) {
                //merge leaf
                if (intoLeft) {
                    return ((SplitLeaf) left).merge(router, (SplitLeaf) right, -1);
                } else {
                    return ((SplitLeaf) right).merge(router, (SplitLeaf) left, -1);
                }
            } else {
                if (leftIsLeaf) {
                    return ((SplitNode) right).mergeIntoChildLeaf(router, true, (SplitLeaf) left);
                } else if (rightIsLeaf) {
                    return ((SplitNode) left).mergeIntoChildLeaf(router, false, (SplitLeaf) right);
                } else {
                    return newNodeOrThis(left, right, depth);
                }
            }
        }

        public Split mergeIntoChildLeaf(ActorAggregationReplicable router, boolean intoLeft, SplitLeaf merged) {
            Split first = intoLeft ? left : right;
            Split second = intoLeft ? right : left;

            if (first instanceof SplitLeaf) {
                return newNode(intoLeft,
                        ((SplitLeaf) first).merge(router, merged, -1),
                        second.adjustDepth(-1), depth - 1);
            } else {
                return newNode(intoLeft,
                        ((SplitNode) first).mergeIntoChildLeaf(router, intoLeft, merged),
                        second.adjustDepth(-1), depth - 1);
            }
        }

        private SplitNode newNode(boolean infoLeft, Split first, Split second, int depth) {
            return newNode(infoLeft ? first : second, infoLeft ? second : first, depth);
        }

        @Override
        public SplitLeaf mergeIntoLeaf(ActorAggregationReplicable router) {
            return left.mergeIntoLeaf(router)
                    .merge(router, right.mergeIntoLeaf(router), -1);
        }

        @Override
        public Split splitOrMerge(ActorAggregationReplicable router, int height) {
            if (depth < height) {
                return newNodeOrThis(
                        left.splitOrMerge(router, height),
                        right.splitOrMerge(router, height), depth);
            } else {
                return mergeIntoLeaf(router);
            }
        }

        @Override
        public <ActorType extends Actor> void accept(ActorType actor, ActorRef sender, ActorVisitor<ActorType> v) {
            v.visitRouterNode(actor, sender, this);
        }
    }

    public static class RoutingHistory {
        public AtomicInteger left = new AtomicInteger();
        public AtomicInteger right = new AtomicInteger();
        public RoutingHistory next;

        public float ratioAll() {
            RoutingHistory h = this;
            long l = 0;
            long r = 0;
            while (true) {
                l += h.left.get();
                r += h.right.get();
                h = h.next;
                if (h == this) {
                    break;
                }
            }
            long t = l + r;
            if (t == 0) {
                return 0.5f;
            } else {
                return ((float) l) / (float) (t);
            }
        }

        public float ratio() {
            long t = total();
            if (t == 0) {
                return 0.5f;
            } else {
                return ((float) left.get()) / (float) (t);
            }
        }

        public int total() {
            return left.get() + right.get();
        }

        public boolean isExceeded(int limit) {
            RoutingHistory h = this;
            int total = 0;
            while (true) {
                total += h.total();
                if (total > limit) {
                    return true;
                }
                h = h.next;
                if (h == this) {
                    break;
                }
            }
            return false;
        }

        public void clearAll() {
            RoutingHistory h = this;
            while (true) {
                h.clear();
                h = h.next;
                if (h == this) {
                    break;
                }
            }
        }

        public RoutingHistory clear() {
            left.set(0);
            right.set(0);
            return this;
        }

        public List<RoutingHistory> toList() {
            List<RoutingHistory> hs = new ArrayList<>();
            RoutingHistory h = next;
            while (true) {
                hs.add(h);
                if (this == h) {
                    break;
                }
                h = h.next;
            }
            return hs;
        }
    }

    /////////////////////////// process


    @Override
    protected void processMessage(Message<?> message) {
        if (isNoRoutingMessage(message)) {
            if (isRouterParallelRouting()) {
                processMessageDelayWhileParallelRouting(message);
            } else {
                if (!processMessagePhase(message)) {
                    super.processMessage(message);
                }
            }
        } else {
            state.processMessage(this, message);
        }
    }

    protected void processMessageDelayWhileParallelRouting(Message<?> message) {
        getSystem().send(message);
    }

    protected boolean processMessagePhase(Message<?> message) {
        return getState().processMessagePhase(this, message);
    }

    /////////////////////////// routing messages

    protected boolean isNoRoutingMessage(Message<?> message) {
        Object data = message.getData();
        return data instanceof NoRouting ||
                data instanceof MailboxAggregation.TraversalProcess ||
                (data instanceof CallableMessage<?,?> &&
                        !(data instanceof Routing));
    }

    public interface NoRouting { }
    public interface Routing { }

    public interface CallableMessageRouting<A extends Actor,T> extends CallableMessage<A,T>, Routing { }

    public static <A extends Actor,T> CallableMessageRouting<A,T> callableRouting(CallableMessageRouting<A,T> t) {
        return t;
    }

    /////////////////////////// methods for state

    public Split internalCreateSplitNode(List<Object> splitPoints, ActorAggregationReplicable a1, ActorAggregationReplicable a2, int depth, int height) {
        Split s1 = internalCreateSplitLeaf(a1, depth + 1, height);
        Split s2 = internalCreateSplitLeaf(a2, depth + 1, height);

        return newSplitNode(splitPoints, s1, s2, depth);
    }

    public Split internalCreateSplitLeaf(ActorAggregationReplicable actor, int depth, int height) {
        if (depth >= height) {
            ActorRef a = place(actor.getPlacement(), actor);
            if (a == this) {
                return null;
            } else {
                return newSplitLeaf(a, depth);
            }
        } else {
            if (height <= 1 && actor == this) {
                return null;
            } else {
                return newSplitLeaf(actor, depth).split(this, height);
            }
        }
    }


    public Split newSplitNode(List<Object> splitPoints, Split s1, Split s2, int depth) {
        return new SplitNode(splitPoints, s1, s2, depth, historyEntrySize());
    }

    public SplitLeaf newSplitLeaf(ActorRef actor, int depth) {
        return new SplitLeaf(actor, depth);
    }


    public Message<?> internalPollForParallelRouting() {
        return mailbox.poll();
    }

    public void internalMerge(ActorAggregationReplicable merged) {
        getMailboxAsReplicable().lockRemainingProcesses();
        merged.getMailboxAsReplicable().lockRemainingProcesses();
        getMailboxAsReplicable()
                .merge(merged.getMailboxAsReplicable());
        merged.internalDisable();
        try {
            initMerged(merged);
        } finally {
            merged.getMailboxAsReplicable().unlockRemainingProcesses(merged);
            getMailboxAsReplicable().unlockRemainingProcesses(this);
        }
    }

    public ActorAggregationReplicable internalCreateClone(ActorRef router) {
        try {
            ActorAggregationReplicable a = (ActorAggregationReplicable) super.clone();
            //if the actor has the name, it copies the reference to the name,
            // but it does not register the actor
            a.processLock = new AtomicBoolean(false);
            a.initMailboxForClone();
            a.behavior = a.initBehavior(); //recreate behavior with initMessageTable by ActorBehaviorBuilderKeyValue
            a.state = new StateLeaf(router);
            a.initClone(this);
            return a;
        } catch (CloneNotSupportedException ce) {
            throw new RuntimeException(ce);
        }
    }

    public ActorAggregationReplicable toLocal(ActorRef ref) {
        if (ref instanceof ActorAggregationReplicable) {
            return (ActorAggregationReplicable) ref;
        }
        try {
            ActorReplicableSerializableState state = ResponsiveCalls.sendTask(system, ref,
                    new CallableToLocalSerializable())
                    .get(toLocalWaitMs(), TimeUnit.MILLISECONDS);
            return state.create(system, -1);
        } catch (Exception ex) {

            ex.printStackTrace();
            return null;
        }
    }

    public static class CallableToLocalSerializable implements CallableMessage<ActorAggregationReplicable, ActorReplicableSerializableState> {
        @Override
        public ActorReplicableSerializableState call(ActorAggregationReplicable self, ActorRef sender) {
            self.getMailboxAsReplicable().lockRemainingProcesses();
            try {
                return self.toSerializable(-1);
            } finally {
                self.getMailboxAsReplicable().terminateAfterSerialized();
                self.internalDisable();
                self.getMailboxAsReplicable().unlockRemainingProcesses(self);
            }
        }
    }

    public void internalDisable() { //remaining messages are processed by the disabled state
        state = new StateDisabled(router());
        router().tell(new DisabledChange(this, DisabledChangeType.DisabledAdded));
    }

    public static class DisabledChange implements Serializable, NoRouting {
        protected ActorRef disabledActor;
        protected Object data;

        public DisabledChange(ActorRef disabledActor, Object data) {
            this.disabledActor = disabledActor;
            this.data = data;
        }

        public ActorRef getDisabledActor() {
            return disabledActor;
        }

        public Object getData() {
            return data;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + data + ", " + disabledActor + ")";
        }
    }

    public enum DisabledChangeType {
        DisabledAdded,
        DisabledFinished,
    }

    /////////////////////////// split or merge APIs

    public void routerSplit(int height) {
        router().tell(CallableMessage.<ActorAggregationReplicable>callableMessageConsumer((a,sender) -> {
            State state = a.state;
            if (state instanceof StateSplitRouter) {
                ((StateSplitRouter) state).split(a, height);
            }
        }));
    }

    public void routerMergeInactive() {
        router().tell(CallableMessage.<ActorAggregationReplicable>callableMessageConsumer((a,sender) -> {
            State state = a.state;
            if (state instanceof StateSplitRouter) {
                ((StateSplitRouter) state).mergeInactive(a);
            }
        }));
    }

    public void routerSplitOrMerge(int height) {
        router().tell(CallableMessage.<ActorAggregationReplicable>callableMessageConsumer((a,sender) -> {
            State state = a.state;
            if (state instanceof StateSplitRouter) {
                ((StateSplitRouter) state).splitOrMerge(a, height);
            }
        }));
    }

    /////////////////////////// remote placement and serialization

    public static ActorRef place(ActorPlacement placement, ActorAggregationReplicable a) {
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

    public static class PlacemenActorReplicable extends ActorPlacement.PlacemenActor {
        public PlacemenActorReplicable(ActorSystem system, String name) {
            super(system, name);
        }

        public PlacemenActorReplicable(ActorSystem system) {
            super(system);
        }

        public PlacemenActorReplicable(ActorSystem system, String name, PlacementStrategy strategy) {
            super(system, name, strategy);
        }

        public PlacemenActorReplicable(ActorSystem system, PlacementStrategy strategy) {
            super(system, strategy);
        }

        @Override
        protected PlacementStrategy initStrategy() {
            return new PlacementStrategyRoundRobinThreads();
        }

        @Override
        public Serializable toSerializable(Actor a, long num) {
            if (a instanceof ActorAggregationReplicable) {
                return ((ActorAggregationReplicable) a).toSerializable(num);
            } else {
                return null;
            }
        }

        @Override
        public Actor fromSerializable(Serializable s, long num) {
            if (s instanceof ActorReplicableSerializableState) {
                try {
                    Actor a = ((ActorReplicableSerializableState) s).create(getSystem(), num);
                    getSystem().send(new Message.MessageNone(a));
                    return a;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    return null;
                }
            } else {
                return null;
            }
        }

        @Override
        protected ActorRef placeLocal(Actor a) {
            a.getSystem().send(new Message.MessageNone(a));
            return a;
        }
    }

    public ActorReplicableSerializableState toSerializable(long num) {
        return initSerializableState(newSerializableState(), num);
    }

    protected ActorReplicableSerializableState newSerializableState() {
        return new ActorReplicableSerializableState();
    }

    protected ActorReplicableSerializableState initSerializableState(ActorReplicableSerializableState state, long num) {
        state.actorType = getClass();
        state.name = String.format("%s#%d", getName(), num);
        state.config = config;
        MailboxAggregationReplicable r = getMailboxAsReplicable();
        r.serializeTo(state);
        return state;
    }

    public static class ActorReplicableSerializableState implements Serializable {
        public Class<? extends ActorAggregationReplicable> actorType;
        public String name;
        public Message<?>[] messages;
        public List<KeyHistograms.HistogramTree> tables;
        public Config config;

        public ActorAggregationReplicable create(ActorSystem system, long num) throws Exception {
            return init(create(system, name(num), config()));
        }

        protected String name(long num) {
            return (num < 0 ? null : String.format("%s_%d", name, num));
        }

        protected Config config() {
            return config == null ? Config.CONFIG_DEFAULT : config;
        }

        protected ActorAggregationReplicable init(ActorAggregationReplicable a) {
            a.state = new StateLeaf(a.router());
            a.getMailboxAsReplicable().deserializeFrom(this);
            return a;
        }

        protected ActorAggregationReplicable create(ActorSystem system, String name, Config config) throws Exception {
            return actorType.getConstructor(ActorSystem.class, String.class, Config.class).newInstance(system, name, config);
        }
    }

    /////////////////////////// print status

    public void printStatus() {
        printStatus("");
    }

    public void printStatus(String head) {
        printStatus(config.getLogOut(), head);
    }

    public void log(String str) {
        config.log(str);
    }

    public void logPhase(String str) {
        config.log(config.logColorPhase, str);
    }

    public String logMessage(String str) {
        return config.logMessage(str);
    }

    public void printStatus(PrintWriter out, String head) {
        String str = toString();
        if (state instanceof StateSplitRouter) {
            StateSplitRouter sr = (StateSplitRouter) state;
            println(out, logMessage(String.format("%s router %s \n" +
                            "   threshold=%,d height=%,d/%,d parallelRouting=%s",
                    head,
                    str,
                    mailboxThreshold(),
                    sr.getHeight(),
                    sr.getMaxHeight(),
                    !sr.isNonParallelRouting())));
            printStatus(sr.getSplit(), out);
        } else if (state instanceof StateLeaf) {
            println(out, logMessage(String.format("%s leaf %s", head, str)));
        } else {
            println(out, logMessage(String.format("%s %s %s", state, head, str)));
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
        if (state instanceof StateSplitRouter) {
            return "router";
        } else if (state instanceof StateLeaf) {
            return "leaf";
        } else if (state instanceof StateDisabled) {
            return "disabled";
        } else if (state == null){
            return "null";
        } else {
            return state.getClass().getName();
        }
    }

    public String toStringMailboxStatus() {
        return String.format("queue=%,d %s",
                getMailboxAsReplicable().size(),
                getMailboxAsReplicable().getTableEntries().stream()
                        .map(MailboxAggregation.HistogramEntry::getTree)
                        .map(t -> String.format("leaf=%,d nonZeroLeaf=%,d valuesInTree=%,d treeHeight=%,d completed=%,d",
                                t.getLeafSize(),
                                t.getLeafSizeNonZero(),
                                t.getTreeSize(),
                                t.getTreeHeight(),
                                t.getCompleted().size()))
                        .collect(Collectors.joining(", ", "[", "]")));
    }

    protected void printStatus(Split s, PrintWriter out) {
        String idt = "  ";
        if (s != null) {
            for (int i = 0; i < s.getDepth(); ++i) {
                idt += "  ";
            }
        }
        if (s == null) {
            println(out, String.format("%s null", idt));
        } else if (s instanceof SplitNode) {
            SplitNode sn = (SplitNode) s;
            println(out, String.format("%s %d:node: %s history=%s", idt, sn.getDepth(), sn.getSplitPoints().stream()
                .map(Objects::toString)
                .map(l -> l.length() > 100 ? l.substring(0, 100) + "..." : l)
                .collect(Collectors.joining(", ", "[", "]")),
                sn.getHistory().toList().stream()
                    .map(h -> String.format("(%,d:%,d)", h.left.get(), h.right.get()))
                    .collect(Collectors.joining(", ", "{", "}"))));
            printStatus(sn.getLeft(), out);
            printStatus(sn.getRight(), out);
        } else if (s instanceof SplitLeaf) {
            ActorRef r = ((SplitLeaf) s).getActor();
            println(out, String.format("%s %d:leaf: %s", idt, s.getDepth(), r));
        }
    }

    public void println(PrintWriter out, String line) {
        config.println(out, line);
    }

}
