package csl.actor.msgassoc;

import csl.actor.*;
import csl.actor.msgassoc.MailboxAggregationReplicable.MailboxStatus;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class ActorAggregationReplicable extends ActorAggregation implements Cloneable {
    protected State state;
    protected volatile int maxHeight;

    public ActorAggregationReplicable(ActorSystem system, String name, MailboxAggregationReplicable mailbox, ActorBehavior behavior) {
        super(system, name, mailbox, behavior);
        state = new StateSplitRouter();
        maxHeight = initMaxHeight();
    }

    public ActorAggregationReplicable(ActorSystem system, String name, ActorBehavior behavior) {
        super(system, name, behavior);
        state = new StateSplitRouter();
        maxHeight = initMaxHeight();
    }

    public ActorAggregationReplicable(ActorSystem system, ActorBehavior behavior) {
        super(system, behavior);
        state = new StateSplitRouter();
        maxHeight = initMaxHeight();
    }

    public ActorAggregationReplicable(ActorSystem system, String name, Config config) {
        super(system, name, config);
        state = new StateSplitRouter();
        maxHeight = initMaxHeight();
    }

    public ActorAggregationReplicable(ActorSystem system, String name) {
        super(system, name);
        state = new StateSplitRouter();
        maxHeight = initMaxHeight();
    }

    public ActorAggregationReplicable(ActorSystem system, Config config) {
        super(system, config);
        state = new StateSplitRouter();
        maxHeight = initMaxHeight();
    }

    public ActorAggregationReplicable(ActorSystem system) {
        super(system);
        state = new StateSplitRouter();
        maxHeight = initMaxHeight();
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

    ////////////////////////

    protected int initMaxHeight() {
        int th = getTotalThreads(this, getPlacement());
        return Math.max(1, (int) (Math.log(th) / Math.log(2)));
    }

    protected int getTotalThreads(ActorAggregationReplicable self, ActorPlacement placement) {
        if (placement instanceof ActorPlacement.PlacemenActor) {
            return ((ActorPlacement.PlacemenActor) placement).getTotalThreads();
        } else {
            return self.getSystem().getThreads();
        }
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

    public MailboxAggregationReplicable getMailboxAsReplicable() {
        return (MailboxAggregationReplicable) mailbox;
    }

    public int getMaxHeight() {
        return maxHeight;
    }

    public static ActorRef place(ActorPlacement placement, ActorAggregationReplicable a) {
        if (placement != null) {
            return placement.place(a);
        } else {
            a.getSystem().send(new Message.MessageNone(a));
            return a;
        }
    }

    public interface State {
        void processMessage(ActorAggregationReplicable self, Message<?> message);
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


    public static class StateSplitRouter implements State {
        protected Split split;
        protected Random random = new Random();
        protected int height = 0;

        protected volatile boolean parallelRouting1;
        protected volatile boolean parallelRouting2;
        protected boolean needClearHistory;

        public void split(ActorAggregationReplicable self, int height) {
            this.height = height;
            if (split == null) { //root
                split = self.createSplitLeaf(self, 0, height);
            } else {
                split = split.split(self, height);
            }
            if (self.logSplit()) {
                self.log("after split: height=" + height);
                self.printStatus();
            }
        }

        public void mergeInactive(ActorAggregationReplicable self) {
            split = split.mergeInactive(self);
            split.clearHistory();
            if (self.logSplit()) {
                self.log("after mergeInactive");
                self.printStatus();
            }
        }

        public void splitOrMerge(ActorAggregationReplicable self, int height) {
            this.height = height;
            if (split == null) {
                split = self.createSplitLeaf(self, 0, height);
            } else {
                split = split.splitOrMerge(self, height);
            }
            if (self.logSplit()) {
                self.log("after splitOrMerge: height=" + height);
                self.printStatus();
            }
        }

        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            MailboxStatus status;
            if (isNonParallelRouting() &&
                    (status = self.getMailboxAsReplicable().getStatus(self.lowerBoundThresholdFactor())).isExcessive()) {

                int maxHeight = self.getMaxHeight();
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
                        if (self.logSplit()) {
                            self.log("after parallelRouting");
                            self.printStatus();
                        }
                    }
                }
                route(self, self.getMailboxAsReplicable(), message, false);
            }
        }


        protected boolean isNonParallelRouting() {
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
            while (!m.isEmpty() && i < max) {
                Message<?> msg = self.pollForParallelRouting();
                if (msg != null) {
                    route(self, m, msg, true);
                }
                ++i;
            }
        }

        protected void route(ActorAggregationReplicable self, MailboxAggregationReplicable m, Message<?> message, boolean fromParallelRouting) {
            if (split == null) {
                if (fromParallelRouting) {
                    self.tell(message.getData(), message.getSender());
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
    }

    public static class StateLeaf implements State, Serializable {
        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            self.processMessageBehavior(message);
        }
    }

    public Split createSplitNode(List<Object> splitPoints, ActorAggregationReplicable a1, ActorAggregationReplicable a2, int depth, int height) {
        Split s1 = createSplitLeaf(a1, depth + 1, height);
        Split s2 = createSplitLeaf(a2, depth + 1, height);

        return newSplitNode(splitPoints, s1, s2, depth);
    }

    public Split createSplitLeaf(ActorAggregationReplicable actor, int depth, int height) {
        if (depth >= height) {
            ActorRef a = place(actor.getPlacement(), actor);
            if (a == this) {
                return null;
            } else {
                return newSplitLeaf(a, depth, this);
            }
        } else {
            if (height <= 1 && actor == this) {
                return null;
            } else {
                return newSplitLeaf(actor, depth, this).split(this, height);
            }
        }
    }


    protected Split newSplitNode(List<Object> splitPoints, Split s1, Split s2, int depth) {
        return new SplitNode(splitPoints, s1, s2, depth, historyEntrySize());
    }

    protected Split newSplitLeaf(ActorRef actor, int depth, ActorRef router) {
        return new SplitLeaf(actor, depth, router);
    }


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
    }

    public static class SplitLeaf implements Split {
        protected ActorRef actor;
        protected int depth;
        protected ActorRef router;

        public SplitLeaf(ActorRef actor, int depth, ActorRef router) {
            this.actor = actor;
            this.depth = depth;
            this.router = router;
        }

        @Override
        public void process(ActorAggregationReplicable router, StateSplitRouter stateRouter,
                            Object key, MailboxAggregation.HistogramSelection selection, Message<?> message) {
            actor.tell(message.getData(), message.getSender());
        }

        @Override
        public SplitLeaf adjustDepth(int dep) {
            this.depth += dep;
            return this;
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

            ActorAggregationReplicable a1 = self.createClone();
            ActorAggregationReplicable a2 = self.createClone();
            a1.state = new StateLeaf();
            a2.state = new StateLeaf();
            List<Object> splitPoints = self.getMailboxAsReplicable()
                    .splitMessageTableIntoReplicas(a1.getMailboxAsReplicable(), a2.getMailboxAsReplicable());
            return router.createSplitNode(splitPoints, a1, a2, depth, height);
        }

        @Override
        public Split mergeInactive(ActorAggregationReplicable router) {
            return this;
        }

        @Override
        public SplitLeaf mergeIntoLeaf(ActorAggregationReplicable router) {
            return this;
        }

        public SplitLeaf merge(ActorAggregationReplicable router, SplitLeaf leaf) {
            ActorRef a1 = getActor();
            ActorRef a2 = leaf.getActor();

            if (a1 instanceof ActorAggregationReplicable) { //local
                if (a2 instanceof ActorAggregationReplicable) {
                    ((ActorAggregationReplicable) a1).merge((ActorAggregationReplicable) a2);
                } else { //remote a2
                    ActorAggregationReplicable l2 = router.toLocal(a2);
                    if (l2 != null) {
                        ((ActorAggregationReplicable) a1).merge(l2);
                    }
                }
            } else if (a2 instanceof ActorAggregationReplicable) { //remote a1, local a2
                ActorAggregationReplicable l1 = router.toLocal(a1);
                if (l1 != null) {
                    ((ActorAggregationReplicable) a2).merge(l1);
                }
                actor = a2;
            } else { //both remote
                a1.tell(CallableMessage.callableMessageConsumer((self, sender) -> {
                    ActorAggregationReplicable l2 = ((ActorAggregationReplicable) self).toLocal(a2);
                    if (l2 != null) {
                        ((ActorAggregationReplicable) self).merge(l2);
                    }
                }), null);
            }

            return this;
        }

        @Override
        public Split splitOrMerge(ActorAggregationReplicable router, int height) {
            if (depth < height) {
                return split(router, height);
            } else {
                return this;
            }
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
            this.depth += dep;
            return this;
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

        /** @return implementation field getter */
        public RoutingHistory getHistory() {
            return history;
        }

        public RoutingHistory initRoutingHistory(int n) {
            RoutingHistory h = new RoutingHistory();
            history = h;
            for (int i = 0; i < n; ++i) {
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
                left = left.split(router, height);
                right = right.split(router, height);
            }
            return this;
        }

        @Override
        public Split mergeInactive(ActorAggregationReplicable router) {
            float r = history.ratioAll();
            float limit = router.mergeRatioThreshold();
            if (r > (1 - limit)) { //into left
                return merge(router, true, left.mergeInactive(router), right.mergeIntoLeaf(router));
            } else if (r < limit) { //into right
                return merge(router, false, left.mergeIntoLeaf(router), right.mergeInactive(router));
            } else {
                this.left = left.mergeInactive(router);
                this.right = right.mergeInactive(router);
                return this;
            }
        }

        protected Split merge(ActorAggregationReplicable router, boolean intoLeft, Split left, Split right) {
            boolean leftIsLeaf = (left instanceof SplitLeaf);
            boolean rightIsLeaf = (right instanceof SplitLeaf);
            if (leftIsLeaf && rightIsLeaf) {
                //merge leaf
                if (intoLeft) {
                    return ((SplitLeaf) left).merge(router, (SplitLeaf) right).adjustDepth(-1);
                } else {
                    return ((SplitLeaf) right).merge(router, (SplitLeaf) left).adjustDepth(-1);
                }
            } else {
                this.left = left;
                this.right = right;
                if (leftIsLeaf) {
                    if (((SplitNode) right).mergeIntoChildLeaf(router, true, (SplitLeaf) left)) {
                        return right.adjustDepth(-1);
                    } else {
                        return this;
                    }
                } else if (rightIsLeaf) {
                    if (((SplitNode) left).mergeIntoChildLeaf(router, false, (SplitLeaf) right)) {
                        return left.adjustDepth(-1);
                    } else {
                        return this;
                    }
                }
                return this;
            }
        }

        public boolean mergeIntoChildLeaf(ActorAggregationReplicable router, boolean intoLeft, SplitLeaf merged) {
            Split first = intoLeft ? left : right;
            Split second = intoLeft ? right : left;

            if (first instanceof SplitLeaf) {
                ((SplitLeaf) first).merge(router, merged);
                return true;
            } else {
                if (((SplitNode) first).mergeIntoChildLeaf(router, intoLeft, merged)) {
                    return true;
                } else {
                    if (second instanceof SplitLeaf) {
                        ((SplitLeaf) second).merge(router, merged);
                        return true;
                    } else {
                        return ((SplitNode) second).mergeIntoChildLeaf(router, intoLeft, merged);
                    }
                }
            }
        }

        @Override
        public SplitLeaf mergeIntoLeaf(ActorAggregationReplicable router) {
            return left.mergeIntoLeaf(router)
                    .merge(router, right.mergeIntoLeaf(router)).adjustDepth(-1);
        }

        @Override
        public Split splitOrMerge(ActorAggregationReplicable router, int height) {
            if (depth < height) {
                left = left.splitOrMerge(router, height);
                right = right.splitOrMerge(router, height);
                return this;
            } else {
                return mergeIntoLeaf(router);
            }
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
            RoutingHistory h = this;
            while (true) {
                hs.add(h);
                h = h.next;
                if (h == this) {
                    break;
                }
            }
            return hs;
        }
    }

    ///////////////////////////


    @Override
    protected void processMessage(Message<?> message) {
        if (isNoRoutingMessage(message)) {
            super.processMessage(message);
        } else {
            state.processMessage(this, message);
        }
    }

    protected boolean isNoRoutingMessage(Message<?> message) {
        return message.getData() instanceof NoRouting ||
                (message.getData() instanceof CallableMessage<?> &&
                        !(message.getData() instanceof Routing));
    }

    public interface NoRouting { }
    public interface Routing { }

    public interface CallableMessageRouting<T> extends CallableMessage<T>, Routing { }

    public static <T> CallableMessageRouting<T> callableRouting(CallableMessageRouting<T> t) {
        return t;
    }

    public Message<?> pollForParallelRouting() {
        return mailbox.poll();
    }

    public ActorAggregationReplicable createClone() {
        try {
            ActorAggregationReplicable a = (ActorAggregationReplicable) super.clone();
            //if the actor has the name, it copies the reference to the name,
            // but it does not register the actor
            a.processLock = new AtomicBoolean(false);
            a.initMailboxForClone();
            a.behavior = a.initBehavior(); //recreate behavior with initMessageTable by ActorBehaviorBuilderKeyValue
            a.initClone(this);
            return a;
        } catch (CloneNotSupportedException ce) {
            throw new RuntimeException(ce);
        }
    }

    protected void initClone(ActorAggregationReplicable original) { }

    public State getState() {
        return state;
    }

    public ActorAggregationReplicable toLocal(ActorRef ref) {
        if (ref instanceof ActorAggregationReplicable) {
            return (ActorAggregationReplicable) ref;
        }
        try {
            ActorReplicableSerializableState state = ResponsiveCalls.sendCallable(system, ref,
                    new CallableToLocalSerializable())
                    .get(toLocalWaitMs(), TimeUnit.MILLISECONDS);
            return state.create(system, -1);
        } catch (Exception ex) {

            ex.printStackTrace();
            return null;
        }
    }

    public static class CallableToLocalSerializable implements CallableMessage<ActorReplicableSerializableState> {
        @Override
        public ActorReplicableSerializableState call(Actor self, ActorRef sender) {
            return ((ActorAggregationReplicable) self).toSerializable(-1);
        }
    }

    public void merge(ActorAggregationReplicable merged) {
        getMailboxAsReplicable()
                .merge(merged.getMailboxAsReplicable());
        initMerged(merged);
    }

    protected void initMerged(ActorAggregationReplicable m) { }

    public void routerSplit(int height) {
        State s = state;
        if (s instanceof StateSplitRouter) {
            ((StateSplitRouter) s).split(this, height);
        }
    }

    public void routerMergeInactive() {
        State s = state;
        if (s instanceof StateSplitRouter) {
            ((StateSplitRouter) s).mergeInactive(this);
        }
    }

    public void routerSplitOrMerge(int height) {
        State s = state;
        if (s instanceof StateSplitRouter) {
            ((StateSplitRouter) s).splitOrMerge(this, height);
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
            a.state = new StateLeaf();
            a.getMailboxAsReplicable().deserializeFrom(this);
            return a;
        }

        protected ActorAggregationReplicable create(ActorSystem system, String name, Config config) throws Exception {
            return actorType.getConstructor(ActorSystem.class, String.class, Config.class).newInstance(system, name, config);
        }
    }

    public void printStatus() {
        printStatus(config.getLogOut());
    }

    public void log(String str) {
        config.log(str);
    }

    public void printStatus(PrintWriter out) {
        String str = toString();
        if (state instanceof StateSplitRouter) {
            StateSplitRouter sr = (StateSplitRouter) state;
            println(out, String.format(" router %s \n" +
                            "   threshold=%,d height=%,d/%,d parallelRouting=%s",
                    str,
                    mailboxThreshold(),
                    sr.getHeight(),
                    getMaxHeight(),
                    !sr.isNonParallelRouting()));
            printStatus(sr.getSplit(), out);
        } else if (state instanceof StateLeaf) {
            println(out, String.format(" leaf %s", str));
        } else {
            println(out, String.format(" %s %s", state, str));
        }
    }

    @Override
    public String toString() {
        return String.format("%s: %s",
                super.toString(),
                toStringMailboxStatus());
    }

    public String toStringMailboxStatus() {
        return String.format("queue=%,d %s",
                getMailboxAsReplicable().size(),
                getMailboxAsReplicable().getTableEntries().stream()
                        .map(MailboxAggregation.HistogramEntry::getTree)
                        .map(t -> String.format("leaf=%,d valuesInTree=%,d treeHeight=%,d",
                                t.getLeafSize(),
                                t.getTreeSize(),
                                t.getTreeHeight()))
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
            println(out, String.format("%s %d:node: %s ", idt, sn.getDepth(), sn.getSplitPoints().stream()
                .map(Objects::toString)
                .map(l -> l.length() > 100 ? l.substring(0, 100) + "..." : l)
                .collect(Collectors.joining(", ", "[", "]"))));
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
