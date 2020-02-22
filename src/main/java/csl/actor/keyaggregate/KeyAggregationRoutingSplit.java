package csl.actor.keyaggregate;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.CallableMessage;
import csl.actor.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public interface KeyAggregationRoutingSplit {
    void process(ActorKeyAggregation router, KeyAggregationStateRouter stateRouter,
                 Object key, MailboxKeyAggregation.HistogramSelection selection, Message<?> message);
    int getDepth();

    default void clearHistory() {}

    default boolean isHistoryExceeded(int limit) {
        return false;
    }

    KeyAggregationRoutingSplit split(ActorKeyAggregation router, int height);
    KeyAggregationRoutingSplit mergeInactive(ActorKeyAggregation router);
    RoutingSplitLeaf mergeIntoLeaf(ActorKeyAggregation router);

    KeyAggregationRoutingSplit splitOrMerge(ActorKeyAggregation router, int height);

    KeyAggregationRoutingSplit adjustDepth(int dep);

    default <ActorType extends Actor> void accept(ActorType actor, ActorRef sender, ActorKeyAggregationVisitor<ActorType> v) {
        v.visitRouter(actor, sender, this);
    }


    class RoutingSplitLeaf implements KeyAggregationRoutingSplit {
        protected ActorRef actor;
        protected int depth;

        public RoutingSplitLeaf(ActorRef actor, int depth) {
            this.actor = actor;
            this.depth = depth;
        }

        @Override
        public void process(ActorKeyAggregation router, KeyAggregationStateRouter stateRouter,
                            Object key, MailboxKeyAggregation.HistogramSelection selection, Message<?> message) {
            actor.tell(message.getData(), message.getSender());
        }

        @Override
        public RoutingSplitLeaf adjustDepth(int dep) {
            return newLeaf(actor, depth + dep);
        }

        public RoutingSplitLeaf newLeaf(ActorRef actor, int depth) {
            return new RoutingSplitLeaf(actor, depth);
        }

        @Override
        public int getDepth() {
            return depth;
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public KeyAggregationRoutingSplit split(ActorKeyAggregation router, int height) {
            if (depth >= height) {
                return this;
            }
            ActorKeyAggregation self = router.toLocal(getActor());
            if (self == null) {
                return this;
            }
            //return split(router, height, self);
            return router.internalCreateSplitNode(self, depth, height);
        }

        @Override
        public KeyAggregationRoutingSplit mergeInactive(ActorKeyAggregation router) {
            return this;
        }

        @Override
        public RoutingSplitLeaf mergeIntoLeaf(ActorKeyAggregation router) {
            return this;
        }

        public RoutingSplitLeaf merge(ActorKeyAggregation router, RoutingSplitLeaf leaf, int depthAdjust) {
            ActorRef a1 = getActor();
            ActorRef a2 = leaf.getActor();

            if (hasRemainingProcesses(router, a1) || hasRemainingProcesses(router, a2)) {
                return this;
            }

            if (a1 instanceof ActorKeyAggregation) { //local
                if (a2 instanceof ActorKeyAggregation) {
                    ((ActorKeyAggregation) a1).internalMerge((ActorKeyAggregation) a2);
                    return newLeaf(a1, depth + depthAdjust);
                } else { //remote a2
                    ActorKeyAggregation l2 = router.toLocal(a2);
                    if (l2 != null) {
                        ((ActorKeyAggregation) a1).internalMerge(l2);
                    }
                    return newLeaf(a1, depth + depthAdjust);
                }
            } else if (a2 instanceof ActorKeyAggregation) { //remote a1, local a2
                ActorKeyAggregation l1 = router.toLocal(a1);
                if (l1 != null) {
                    ((ActorKeyAggregation) a2).internalMerge(l1);
                }
                return newLeaf(a2, depth + depthAdjust);
            } else { //both remote
                a1.tell(CallableMessage.callableMessageConsumer((self, sender) -> {
                    ActorKeyAggregation l2 = ((ActorKeyAggregation) self).toLocal(a2);
                    if (l2 != null) {
                        ((ActorKeyAggregation) self).internalMerge(l2);
                    }
                }), null);
                return newLeaf(a1, depth + depthAdjust);
            }
        }

        public boolean hasRemainingProcesses(ActorKeyAggregation router, ActorRef a) {
            try {
                return ResponsiveCalls.sendTask(router.getSystem(), a, (self, sender) ->
                        ((ActorKeyAggregation) self).hasRemainingProcesses())
                        .get(1, TimeUnit.SECONDS);
            } catch (Exception ex) {
                router.log("#hasRemainingProcesses: busy " + a + " : " + ex);
                return true;
            }
        }

        @Override
        public KeyAggregationRoutingSplit splitOrMerge(ActorKeyAggregation router, int height) {
            if (depth < height) {
                return split(router, height);
            } else {
                return this;
            }
        }

        @Override
        public <ActorType extends Actor> void accept(ActorType actor, ActorRef sender, ActorKeyAggregationVisitor<ActorType> v) {
            v.visitRouterLeaf(actor, sender, this);
        }
    }

    class RoutingSplitNode implements KeyAggregationRoutingSplit {
        protected List<Object> splitPoints;
        protected KeyAggregationRoutingSplit left;
        protected KeyAggregationRoutingSplit right;
        protected int depth;
        protected RoutingHistory history;

        public RoutingSplitNode(List<Object> splitPoints, KeyAggregationRoutingSplit left, KeyAggregationRoutingSplit right, int depth, int historyEntrySize) {
            this.splitPoints = splitPoints;
            this.left = left;
            this.right = right;
            this.depth = depth;
            history = initRoutingHistory(historyEntrySize);
        }

        public RoutingSplitNode(List<Object> splitPoints, KeyAggregationRoutingSplit left, KeyAggregationRoutingSplit right, int depth, RoutingHistory history) {
            this.splitPoints = splitPoints;
            this.left = left;
            this.right = right;
            this.depth = depth;
            this.history = history;
        }

        public RoutingSplitNode newNode(KeyAggregationRoutingSplit left, KeyAggregationRoutingSplit right, int depth) {
            return new RoutingSplitNode(splitPoints, left, right, depth, history);
        }

        protected RoutingSplitNode newNodeOrThis(KeyAggregationRoutingSplit left, KeyAggregationRoutingSplit right, int depth) {
            if (left != this.left || right != this.right || this.depth != depth) {
                return newNode(left, right, depth);
            } else {
                return this;
            }
        }

        @Override
        public void process(ActorKeyAggregation router, KeyAggregationStateRouter stateRouter,
                            Object key, MailboxKeyAggregation.HistogramSelection selection, Message<?> message) {
            if (select(router, stateRouter, key, selection, message)) {
                countHistory(true, router);
                left.process(router, stateRouter, key, selection, message);
            } else {
                countHistory(false, router);
                right.process(router, stateRouter, key, selection, message);
            }
        }

        protected void countHistory(boolean left, ActorKeyAggregation router) {
            if (left) {
                history.left.getAndIncrement();
            } else {
                history.right.getAndIncrement();
            }
            if (history.total() > router.historyEntryLimit()) {
                history = history.next.clear();
            }
        }

        protected boolean select(ActorKeyAggregation self, KeyAggregationStateRouter router,
                                 Object key, MailboxKeyAggregation.HistogramSelection selection, Message<?> message) {
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
                return self.getMailboxAsKeyAggregation().compare(selection.entryId, key, point);
            }
        }

        @Override
        public RoutingSplitNode adjustDepth(int dep) {
            return newNode(left, right, dep + depth);
        }

        @Override
        public int getDepth() {
            return depth;
        }

        public KeyAggregationRoutingSplit getLeft() {
            return left;
        }

        public KeyAggregationRoutingSplit getRight() {
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
        public KeyAggregationRoutingSplit split(ActorKeyAggregation router, int height) {
            if (depth < height) {
                return newNodeOrThis(
                        left.split(router, height),
                        right.split(router, height), depth);
            }
            return this;
        }

        @Override
        public KeyAggregationRoutingSplit mergeInactive(ActorKeyAggregation router) {
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

        protected KeyAggregationRoutingSplit merge(ActorKeyAggregation router, boolean intoLeft, KeyAggregationRoutingSplit left, KeyAggregationRoutingSplit right) {
            boolean leftIsLeaf = (left instanceof RoutingSplitLeaf);
            boolean rightIsLeaf = (right instanceof RoutingSplitLeaf);
            if (leftIsLeaf && rightIsLeaf) {
                //merge leaf
                if (intoLeft) {
                    return ((RoutingSplitLeaf) left).merge(router, (RoutingSplitLeaf) right, -1);
                } else {
                    return ((RoutingSplitLeaf) right).merge(router, (RoutingSplitLeaf) left, -1);
                }
            } else {
                if (leftIsLeaf) {
                    return ((RoutingSplitNode) right).mergeIntoChildLeaf(router, true, (RoutingSplitLeaf) left);
                } else if (rightIsLeaf) {
                    return ((RoutingSplitNode) left).mergeIntoChildLeaf(router, false, (RoutingSplitLeaf) right);
                } else {
                    return newNodeOrThis(left, right, depth);
                }
            }
        }

        public KeyAggregationRoutingSplit mergeIntoChildLeaf(ActorKeyAggregation router, boolean intoLeft, RoutingSplitLeaf merged) {
            KeyAggregationRoutingSplit first = intoLeft ? left : right;
            KeyAggregationRoutingSplit second = intoLeft ? right : left;

            if (first instanceof RoutingSplitLeaf) {
                return newNode(intoLeft,
                        ((RoutingSplitLeaf) first).merge(router, merged, -1),
                        second.adjustDepth(-1), depth - 1);
            } else {
                return newNode(intoLeft,
                        ((RoutingSplitNode) first).mergeIntoChildLeaf(router, intoLeft, merged),
                        second.adjustDepth(-1), depth - 1);
            }
        }

        private RoutingSplitNode newNode(boolean infoLeft, KeyAggregationRoutingSplit first, KeyAggregationRoutingSplit second, int depth) {
            return newNode(infoLeft ? first : second, infoLeft ? second : first, depth);
        }

        @Override
        public RoutingSplitLeaf mergeIntoLeaf(ActorKeyAggregation router) {
            return left.mergeIntoLeaf(router)
                    .merge(router, right.mergeIntoLeaf(router), -1);
        }

        @Override
        public KeyAggregationRoutingSplit splitOrMerge(ActorKeyAggregation router, int height) {
            if (depth < height) {
                return newNodeOrThis(
                        left.splitOrMerge(router, height),
                        right.splitOrMerge(router, height), depth);
            } else {
                return mergeIntoLeaf(router);
            }
        }

        @Override
        public <ActorType extends Actor> void accept(ActorType actor, ActorRef sender, ActorKeyAggregationVisitor<ActorType> v) {
            v.visitRouterNode(actor, sender, this);
        }
    }

    class RoutingHistory {
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
}
