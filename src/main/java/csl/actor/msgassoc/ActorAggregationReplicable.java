package csl.actor.msgassoc;

import csl.actor.*;
import csl.actor.msgassoc.MailboxAggregationReplicable.MailboxStatus;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ActorAggregationReplicable extends ActorAggregation implements Cloneable {
    protected State state;
    protected volatile int maxHeight;

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

    public ActorAggregationReplicable(ActorSystem system, String name) {
        super(system, name);
        state = new StateSplitRouter();
        maxHeight = initMaxHeight();
    }

    public ActorAggregationReplicable(ActorSystem system) {
        super(system);
        state = new StateSplitRouter();
        maxHeight = initMaxHeight();
    }

    protected int initMaxHeight() {
        int th = getTotalThreads(this, getPlacement());
        return Math.max(1, (int) (Math.log(th) / Math.log(2)));
    }

    protected int getTotalThreads(ActorAggregationReplicable self, ActorPlacement placement) {
        if (placement instanceof ActorPlacement.PlacemenActor) {
            return ((ActorPlacement.PlacemenActor) placement).getTotalThreads();
        } else {
            return ActorPlacement.getLocalThreads(self.getSystem());
        }
    }

    @Override
    protected void initMailbox() {
        mailbox = new MailboxAggregationReplicable();
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
        /*
        @Deprecated
        default int getDepth() {
            return 0;
        }*/
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
    /*
    @Deprecated
    public static class StateDefault implements State {
        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            if (isOverThreshold(self)) {
                split(self, message);
            } else {
                self.behavior.process(self, message);
            }
        }

        protected boolean isOverThreshold(ActorAggregationReplicable self) {
            if (getDepth() < self.getMaxHeight() && self.getMailboxAsReplicable().isOverThreshold()) {
                return true;
            } else {
                return false;
            }
        }

        public void split(ActorAggregationReplicable self, Message<?> message) {
            int nextDep = getDepth() + 1;
            ActorRef router = getRouter(self);
            ActorAggregationReplicable a1 = self.createClone();
            ActorAggregationReplicable a2 = self.createClone();
            a1.state = new StateReplica(router, nextDep);
            a2.state = new StateReplica(router, nextDep);
            List<Object> splitPoints = self.getMailboxAsReplicable().splitMessageTableIntoReplicas(a1, a2);

            ActorPlacement placement = self.getPlacement();

            StateRouter r = newStateRouter(self,
                    place(placement, a1),
                    place(placement, a2), splitPoints);
            self.state = r;
            r.route(self, message);
        }

        protected ActorRef getRouter(ActorAggregationReplicable self) {
            return self;
        }

        protected StateRouter newStateRouter(ActorAggregationReplicable self, ActorRef a1, ActorRef a2,
                                             List<Object> splitPoints) {
            return new StateRouter(self, a1, a2, splitPoints, getDepth());
        }

        @Override
        public int getDepth() {
            return 0;
        }
    }

    @Deprecated
    public static class StateRouter implements State {
        protected List<MailboxAggregationReplicable.SplitTreeRoot> splits;
        protected Random random = new Random();
        protected int depth;

        protected RouterUpdate update = new RouterUpdate();

        public StateRouter(ActorAggregationReplicable self, ActorRef a1, ActorRef a2, List<Object> splitPoints, int depth) {
            MailboxAggregationReplicable mailbox = self.getMailboxAsReplicable();
            splits = mailbox.createSplits(a1, a2, random, splitPoints, depth);
            this.depth = depth;
        }

        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            if (message.getData() instanceof RequestUpdateSplitsPrepare) {
                update.forecast((RequestUpdateSplitsPrepare) message.getData());
            } else if (message.getData() instanceof RequestUpdateSplits) {
                update.request((RequestUpdateSplits) message.getData())
                        .forEachRemaining(r -> updatePoints(self, r));
                //updatePoints(self, (RequestUpdateSplits) message.getData());
            } else {
                route(self, message);
            }
        }

        public void updatePoints(ActorAggregationReplicable self, RequestUpdateSplits request) {
            List<Object> splitPoints = request.getNewSplitPoints();
            ActorRef left = request.getLeft();
            ActorRef right = request.getRight();
            for (int i = 0, size = splits.size(); i < size; ++i) {
                splits.get(i).updatePoint(splitPoints.get(i), left, right);
            }
        }

        public void route(ActorAggregationReplicable self, Message<?> message) {
            MailboxAggregation.HistogramSelection target = self.getMailboxAsReplicable().selectTable(message.getData());
            if (target == null) {
                splits.get(random.nextInt(splits.size())).sendNonKey(message);
            } else {
                splits.get(target.entryId).send(message, target.position);
            }
        }

        /** @return implementation field getter *//*
        public Random getRandom() {
            return random;
        }

        /** @return implementation field getter *//*
        public List<MailboxAggregationReplicable.SplitTreeRoot> getSplits() {
            return splits;
        }

        @Override
        public int getDepth() {
            return depth;
        }

        /** @return implementation field getter *//*
        public RouterUpdate getUpdate() {
            return update;
        }
    }

    @Deprecated
    public static class RouterUpdate {
        protected TreeMap<Integer,RouterUpdateEntry> depthToEntry;
        protected int minDepth;
        protected int pendingSize;

        public RouterUpdate() {
            depthToEntry = new TreeMap<>();
            minDepth = Integer.MAX_VALUE;
            pendingSize = 0;
        }

        public void forecast(RequestUpdateSplitsPrepare p) {
            depthToEntry.computeIfAbsent(p.getDepth(), RouterUpdateEntry::new)
                .count++;
            minDepth = Math.min(minDepth, p.getDepth());
        }

        public Iterator<RequestUpdateSplits> request(RequestUpdateSplits r) {
            if (minDepth >= r.getDepth()) {
                List<RequestUpdateSplits> list = new ArrayList<>(pendingSize + 1);
                list.add(r);
                int[] nextMinDepth = new int[] {Integer.MAX_VALUE};
                depthToEntry.entrySet()
                        .removeIf(e -> e.getValue().collect(list, nextMinDepth));
                minDepth = nextMinDepth[0];
                pendingSize = 0;
                return list.iterator();
            } else {
                ++pendingSize;
                depthToEntry.get(r.getDepth())
                        .pending.add(r);
                return Collections.emptyIterator();
            }
        }

        public long forecastCount() {
            return depthToEntry.values().stream()
                    .mapToInt(e -> e.count)
                    .sum();
        }

        /** @return implementation field getter *//*
        public TreeMap<Integer, RouterUpdateEntry> getDepthToEntry() {
            return depthToEntry;
        }

        /** @return implementation field getter *//*
        public int getMinDepth() {
            return minDepth;
        }

        /** @return implementation field getter *//*
        public int getPendingSize() {
            return pendingSize;
        }
    }

    @Deprecated
    public static class RouterUpdateEntry {
        public int depth;
        public int count;
        public List<RequestUpdateSplits> pending = new ArrayList<>(3);

        public RouterUpdateEntry(int depth) {
            this.depth = depth;
        }

        public boolean collect(List<RequestUpdateSplits> target, int[] nonZero) {
            target.addAll(pending);
            count -= pending.size();
            pending.clear();
            if (count <= 0) {
                return true;
            } else {
                nonZero[0] = Math.min(nonZero[0], depth);
                return false;
            }
        }
    }

    @Deprecated
    public static class StateReplica extends StateDefault implements State {
        protected ActorRef router;
        protected int depth;

        public StateReplica(ActorRef router, int depth) {
            this.router = router;
            this.depth = depth;
        }

        public ActorRef getRouter() {
            return router;
        }

        @Override
        public void split(ActorAggregationReplicable self, Message<?> message) {
            super.split(self, message);
            router.tell(new RequestUpdateSplitsPrepare(self.state.getDepth()), self);
        }

        protected ActorRef getRouter(ActorAggregationReplicable self) {
            return router;
        }

        @Override
        protected StateRouter newStateRouter(ActorAggregationReplicable self, ActorRef a1, ActorRef a2, List<Object> splitPoints) {
            return new StateRouterTemporary(self, a1, a2, router, splitPoints, getDepth());
        }

        @Override
        public int getDepth() {
            return depth;
        }
    }

    @Deprecated
    public static class StateRouterTemporary extends StateRouter {
        protected ActorRef router;
        protected ActorRef left;
        protected ActorRef right;
        protected boolean requested;
        public StateRouterTemporary(ActorAggregationReplicable self, ActorRef a1, ActorRef a2, ActorRef router, List<Object> splitPoints, int depth) {
            super(self, a1, a2, splitPoints, depth);
            this.router = router;
            this.left = a1;
            this.right = a2;
            requested = false;
        }

        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            super.processMessage(self, message);
            processFurtherMessages(self);

            if (!requested && self.getMailboxAsReplicable().isEmpty()) {
                router.tell(new RequestUpdateSplits(toSplitPoints(), left, right, getDepth()), self);
                requested = true;
            }
        }

        protected void processFurtherMessages(ActorAggregationReplicable self) {
            //here we can manually process further messages on self for quickly delivering messages
            MailboxAggregationReplicable mailbox = self.getMailboxAsReplicable();
            int processLimit = Math.max(10, mailbox.getThreshold() / 10);
            for (int i = 0; !mailbox.isEmpty() && i < processLimit; ++i) {
                super.processMessage(self, mailbox.poll());
            }
        }

        protected List<Object> toSplitPoints() {
            return splits.stream()
                    .map(MailboxAggregationReplicable.SplitTreeRoot::getSplitPoint)
                    .collect(Collectors.toList());
        }

        /** @return implementation field getter *//*
        public boolean isRequested() {
            return requested;
        }

        /** @return implementation field getter *//*
        public ActorRef getRouter() {
            return router;
        }

        /** @return implementation field getter *//*
        public ActorRef getLeft() {
            return left;
        }

        /** @return implementation field getter *//*
        public ActorRef getRight() {
            return right;
        }
    }


    @Deprecated
    public static class RequestUpdateSplitsPrepare implements Serializable {
        protected int depth;

        public RequestUpdateSplitsPrepare(int depth) {
            this.depth = depth;
        }

        public int getDepth() {
            return depth;
        }
    }

    @Deprecated
    public static class RequestUpdateSplits implements Serializable {
        protected List<Object> newSplitPoints;
        protected ActorRef left;
        protected ActorRef right;
        protected int depth;

        public RequestUpdateSplits(List<Object> newSplitPoints, ActorRef left, ActorRef right, int depth) {
            this.newSplitPoints = newSplitPoints;
            this.left = left;
            this.right = right;
            this.depth = depth;
        }

        public List<Object> getNewSplitPoints() {
            return newSplitPoints;
        }

        public ActorRef getLeft() {
            return left;
        }

        public ActorRef getRight() {
            return right;
        }

        public int getDepth() {
            return depth;
        }
    }
*/
    ///////////////////////////

    public static class StateSplitRouter implements State {
        protected Split split;
        protected Random random = new Random();
        protected int height = 0;

        protected volatile boolean parallelRouting1;
        protected volatile boolean parallelRouting2;
        protected boolean needClearHistory;

        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            MailboxStatus status;
            if (isNonParallelRouting() &&
                    (status = self.getMailboxAsReplicable().getStatus()).isExcessive()) {

                int maxHeight = self.getMaxHeight();
                MailboxAggregationReplicable m = self.getMailboxAsReplicable();
                int size = m.size();
                int threshold = m.getThreshold();

                if (status.equals(MailboxStatus.Exceeded) &&
                        height < maxHeight) {
                    setNextHeight(maxHeight, size, threshold);
                    System.err.println(String.format("!!! size=%,d thr=%,d h=%,d maxH=%,d  status=%s leaf=%,d sizeInTbl=%,d",
                            size, threshold, height, maxHeight, status,
                            self.getMailboxAsReplicable().getTable(0).getLeafSize(),
                            self.getMailboxAsReplicable().getTable(0).getRoot().size()));

                    splitAndParallelRouting(self, m, message);
                } else if (split != null && status.equals(MailboxStatus.Few) &&
                        split.isHistoryExceeded(threshold / 3)) {
                    mergeInactive(self, m, message);
                } else {
                    route(self, m, message);
                }
            } else {
                if (needClearHistory) {
                    needClearHistory = false;
                    if (split != null && isNonParallelRouting()) {
                        split.clearHistory();
                        System.err.println("!!! after boost");
                        print(self.getSystem(), split);
                    }
                }
                route(self, self.getMailboxAsReplicable(), message);
            }
        }

        private void print(ActorSystem system, Split s) {
            String idt = "  ";
            for (int i = 0; i < s.getDepth(); ++i) {
                idt += " - ";
            }
            if (s instanceof SplitNode) {
                SplitNode sn = (SplitNode) s;
                System.err.println(String.format("%snode: %s ", idt, sn.getSplitPoints()));
                print(system, sn.getLeft());
                print(system, sn.getRight());
            } else if (s instanceof SplitLeaf) {
                ActorAggregationReplicable r = toLocal(system, ((SplitLeaf) s).getActor());
                if (r == null) {
                    System.err.println(String.format("%sleaf: ??? %s", idt, ((SplitLeaf) s).getActor()));
                } else {
                    KeyHistograms.HistogramNode root = r.getMailboxAsReplicable().getTable(0).getRoot();
                    System.err.println(String.format("%sleaf: size=%,d leaf=%,d sizeInTbl=%,d", idt,
                            r.getMailboxAsReplicable().size(),
                            r.getMailboxAsReplicable().getTable(0).getLeafSize(),
                            (root == null ? 0 : root.size())));
                }
            }
        }

        protected boolean isNonParallelRouting() {
            return !(parallelRouting1 || parallelRouting2);
        }

        protected void setNextHeight(int maxHeight, int size, int threshold) {
            int h = 1;
            while ((size >>> h) > threshold) {
                ++h;
            }

            this.height = Math.min(Math.max(height, h), maxHeight);
        }

        protected void splitAndParallelRouting(ActorAggregationReplicable self, MailboxAggregationReplicable m, Message<?> message) {
            //TODO special considering height=0
            if (split == null) { //root
                split = SplitLeaf.create(self, 0, height);
            } else {
                split = split.split(self.getSystem(), height);
            }
            print(self.getSystem(), split);
            route(self, m, message);

            needClearHistory = true;
            if (split != null) {
                parallelRouting1 = true;
                parallelRouting2 = true;
                self.getSystem().execute(() -> {
                    try {
                        routeRemaining(self);
                    } finally {
                        parallelRouting1 = false;
                    }
                });
                self.getSystem().execute(() -> {
                    try {
                        routeRemaining(self);
                    } finally {
                        parallelRouting2 = false;
                    }
                });
            }
        }

        protected void mergeInactive(ActorAggregationReplicable self, MailboxAggregationReplicable m, Message<?> message) {
            //TODO split = split.mergeInactive(self.getSystem());
            split.clearHistory();
            route(self, m, message);
        }

        protected void routeRemaining(ActorAggregationReplicable self) {
            MailboxAggregationReplicable m = self.getMailboxAsReplicable();
            while (!m.isEmpty()) {
                Message<?> msg = self.pollForParallelRouting();
                if (msg != null) {
                    route(self, m, msg);
                }
            }
        }

        protected void route(ActorAggregationReplicable self, MailboxAggregationReplicable m, Message<?> message) {
            if (split == null) {
                self.getBehavior().process(self, message);
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
            self.getBehavior().process(self, message);
        }
    }

    public interface Split {
        void process(ActorAggregationReplicable self, StateSplitRouter router,
                     Object key, MailboxAggregation.HistogramSelection selection, Message<?> message);
        int getDepth();

        default void clearHistory() {}

        default boolean isHistoryExceeded(int limit) {
            return false;
        }

        Split split(ActorSystem system, int height);
        Split mergeInactive(ActorSystem system);
        SplitLeaf mergeIntoLeaf(ActorSystem system);
    }

    public static class SplitLeaf implements Split {
        protected ActorRef actor;
        protected int depth;

        public SplitLeaf(ActorRef actor, int depth) {
            this.actor = actor;
            this.depth = depth;
        }

        @Override
        public void process(ActorAggregationReplicable self, StateSplitRouter router,
                            Object key, MailboxAggregation.HistogramSelection selection, Message<?> message) {
            actor.tell(message.getData(), message.getSender());
        }

        @Override
        public int getDepth() {
            return depth;
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public Split split(ActorSystem system, int height) {
            if (depth >= height) {
                return this;
            }
            ActorAggregationReplicable self = toLocal(system, getActor());
            if (self == null) {
                return this;
            }

            ActorAggregationReplicable a1 = self.createClone();
            ActorAggregationReplicable a2 = self.createClone();
            a1.state = new StateLeaf();
            a2.state = new StateLeaf();
            List<Object> splitPoints = self.getMailboxAsReplicable().splitMessageTableIntoReplicas(a1, a2);
            Split s1 = create(a1, depth + 1, height);
            Split s2 = create(a2, depth + 1, height);

            return new SplitNode(splitPoints, s1, s2, depth);
        }

        public static Split create(ActorAggregationReplicable actor, int depth, int height) {
            if (depth >= height) {
                return new SplitLeaf(place(actor.getPlacement(), actor), depth);
            } else {
                return new SplitLeaf(actor, depth).split(actor.getSystem(), height);
            }
        }

        @Override
        public Split mergeInactive(ActorSystem system) {
            return this;
        }

        @Override
        public SplitLeaf mergeIntoLeaf(ActorSystem system) {
            return this;
        }

        public SplitLeaf merge(ActorSystem system, SplitLeaf leaf) {
            ActorRef a1 = getActor();
            ActorRef a2 = leaf.getActor();

            if (a1 instanceof ActorAggregationReplicable) { //local
                if (a2 instanceof ActorAggregationReplicable) {
                    ((ActorAggregationReplicable) a1).merge((ActorAggregationReplicable) a2);
                } else { //remote a2
                    ActorAggregationReplicable l2 = toLocal(system, a2);
                    if (l2 != null) {
                        ((ActorAggregationReplicable) a1).merge(l2);
                    }
                }
            } else if (a2 instanceof ActorAggregationReplicable) { //remote a1, local a2
                ActorAggregationReplicable l1 = toLocal(system, a1);
                if (l1 != null) {
                    ((ActorAggregationReplicable) a2).merge(l1);
                }
                actor = a2;
            } else { //both remote
                a1.tell(CallableMessage.callableMessageConsumer((self, sender) -> {
                    ActorAggregationReplicable l2 = toLocal(self.getSystem(), a2);
                    if (l2 != null) {
                        ((ActorAggregationReplicable) self).merge(l2);
                    }
                }), null);
            }

            return this;
        }
    }

    public static class SplitNode implements Split {
        protected List<Object> splitPoints;
        protected Split left;
        protected Split right;
        protected int depth;
        protected RoutingHistory history;

        public SplitNode(List<Object> splitPoints, Split left, Split right, int depth) {
            this.splitPoints = splitPoints;
            this.left = left;
            this.right = right;
            this.depth = depth;
            history = initRoutingHistory();
        }

        @Override
        public void process(ActorAggregationReplicable self, StateSplitRouter router,
                            Object key, MailboxAggregation.HistogramSelection selection, Message<?> message) {
            if (select(self, router, key, selection, message)) {
                countHistory(true, self);
                left.process(self, router, key, selection, message);
            } else {
                countHistory(false, self);
                right.process(self, router, key, selection, message);
            }
        }

        protected void countHistory(boolean left, ActorAggregationReplicable self) {
            if (left) {
                history.left.getAndIncrement();
            } else {
                history.right.getAndIncrement();
            }
            if (history.total() > self.getMailboxAsReplicable().getThreshold() / 10) {
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

        public RoutingHistory initRoutingHistory() {
            RoutingHistory h = new RoutingHistory();
            history = h;
            for (int i = 0; i < 10; ++i) {
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
        public Split split(ActorSystem system, int height) {
            if (depth < height) {
                left = left.split(system, height);
                right = right.split(system, height);
            }
            return this;
        }

        @Override
        public Split mergeInactive(ActorSystem system) {
            float r = history.ratioAll();
            if (r > 0.8f) { //into left
                return merge(system, true, left.mergeInactive(system), right.mergeIntoLeaf(system));
            } else if (r < 0.2f) { //into right
                return merge(system, false, left.mergeIntoLeaf(system), right.mergeInactive(system));
            } else {
                this.left = left.mergeInactive(system);
                this.right = right.mergeInactive(system);
                return this;
            }
        }

        protected Split merge(ActorSystem system, boolean intoLeft, Split left, Split right) {
            boolean leftIsLeaf = (left instanceof SplitLeaf);
            boolean rightIsLeaf = (right instanceof SplitLeaf);
            if (leftIsLeaf && rightIsLeaf) {
                //merge leaf
                if (intoLeft) {
                    return ((SplitLeaf) left).merge(system, (SplitLeaf) right);
                } else {
                    return ((SplitLeaf) right).merge(system, (SplitLeaf) left);
                }
            } else {
                this.left = left;
                this.right = right;
                if (leftIsLeaf) {
                    if (((SplitNode) right).mergeIntoChildLeaf(system, true, (SplitLeaf) left)) {
                        return right;
                    } else {
                        return this;
                    }
                } else if (rightIsLeaf) {
                    if (((SplitNode) left).mergeIntoChildLeaf(system, false, (SplitLeaf) right)) {
                        return left;
                    } else {
                        return this;
                    }
                }
                return this;
            }
        }

        public boolean mergeIntoChildLeaf(ActorSystem system, boolean intoLeft, SplitLeaf merged) {
            Split first = intoLeft ? left : right;
            Split second = intoLeft ? right : left;

            if (first instanceof SplitLeaf) {
                ((SplitLeaf) first).merge(system, merged);
                return true;
            } else {
                if (((SplitNode) first).mergeIntoChildLeaf(system, intoLeft, merged)) {
                    return true;
                } else {
                    if (second instanceof SplitLeaf) {
                        ((SplitLeaf) second).merge(system, merged);
                        return true;
                    } else {
                        return ((SplitNode) second).mergeIntoChildLeaf(system, intoLeft, merged);
                    }
                }
            }
        }

        @Override
        public SplitLeaf mergeIntoLeaf(ActorSystem system) {
            return left.mergeIntoLeaf(system)
                    .merge(system, right.mergeIntoLeaf(system));
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

    public static ActorAggregationReplicable toLocal(ActorSystem system, ActorRef ref) {
        if (ref instanceof ActorAggregationReplicable) {
            return (ActorAggregationReplicable) ref;
        }
        try {
            ActorReplicableSerializableState state = ResponsiveCalls.<ActorReplicableSerializableState>send(system, ref,
                    CallableMessage.callableMessage((s, sender) ->
                            ((ActorAggregationReplicable) s).toSerializable(0)))
                    .get(2, TimeUnit.SECONDS);

            return fromSerializable(system, state, 0);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public void merge(ActorAggregationReplicable merged) {
        getMailboxAsReplicable()
                .merge(merged.getMailboxAsReplicable());
        initMerged(merged);
    }

    protected void initMerged(ActorAggregationReplicable m) { }

    public static class PlacemenActorReplicable extends ActorPlacement.PlacemenActor {
        public PlacemenActorReplicable(ActorSystem system, String name) {
            super(system, name);
        }

        public PlacemenActorReplicable(ActorSystem system) {
            super(system);
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
                Actor a = ActorAggregationReplicable.fromSerializable(getSystem(), (ActorReplicableSerializableState) s, num);
                getSystem().send(new Message.MessageNone(a));
                return a;
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
        ActorReplicableSerializableState state = newSerializableState();
        state.actorType = getClass();
        state.name = String.format("%s#%d", getName(), num);

        /*
        State s = getState();
        state.depth = s.getDepth();
        if (s instanceof StateReplica) { //a replica always has StateReplica
            state.router = ((StateReplica) s).getRouter();
        }*/

        MailboxAggregationReplicable r = getMailboxAsReplicable();
        r.serializeTo(state);
        return state;
    }

    protected ActorReplicableSerializableState newSerializableState() {
        return new ActorReplicableSerializableState();
    }

    public static ActorAggregationReplicable fromSerializable(ActorSystem system, ActorReplicableSerializableState state, long num) {
        try {
            ActorAggregationReplicable r = state.actorType.getConstructor(ActorSystem.class, String.class)
                .newInstance(system, String.format("%s_%d", state.name, num));
            //if (state.router == null) {
                r.state = new StateLeaf();
            /*} else {
                r.state = new StateReplica(state.router, state.depth);
            }*/

            r.getMailboxAsReplicable().deserializeFrom(state);
            return r;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public static class ActorReplicableSerializableState implements Serializable {
        public Class<? extends ActorAggregationReplicable> actorType;
        public String name;
        public ActorRef router;
        public Message<?>[] messages;
        public List<KeyHistograms.HistogramTree> tables;
        public int threshold;
        public int depth;
    }
}
