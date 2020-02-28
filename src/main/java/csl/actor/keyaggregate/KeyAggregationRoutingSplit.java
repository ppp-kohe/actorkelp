package csl.actor.keyaggregate;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.CallableMessage;
import csl.actor.Message;
import csl.actor.cluster.ResponsiveCalls;

import java.util.*;
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

    KeyAggregationRoutingSplit split(SplitOrMergeContext context, int height);
    KeyAggregationRoutingSplit mergeInactive(SplitOrMergeContext context);
    KeyAggregationRoutingSplit mergeIntoLeaf(SplitOrMergeContext context);

    KeyAggregationRoutingSplit splitOrMerge(SplitOrMergeContext context, int height);

    KeyAggregationRoutingSplit adjustPath(SplitPath newPath);

    default <ActorType extends Actor> void accept(ActorType actor, ActorRef sender, KeyAggregationVisitor<ActorType> v) {
        v.visitRouter(actor, sender, this);
    }

    SplitPath getPath();

    interface SplitOrMergeContext {
        ActorKeyAggregation router();
        void merged(KeyAggregationRoutingSplit newMerged, KeyAggregationRoutingSplit oldNode, KeyAggregationRoutingSplit oldAnother);
        void split(KeyAggregationRoutingSplit newSplit, KeyAggregationRoutingSplit oldNode);
    }

    class SplitOrMergeContextDefault implements SplitOrMergeContext {
        protected String message;
        protected ActorKeyAggregation router;
        protected Set<KeyAggregationRoutingSplit> newSplits = new HashSet<>();
        protected boolean mergedToRoot;
        protected boolean splitFromRoot;

        public SplitOrMergeContextDefault(String message, ActorKeyAggregation router) {
            this.message = message;
            this.router = router;
        }

        public String getMessage() {
            return message;
        }

        public boolean hasChanges() {
            return !newSplits.isEmpty() || mergedToRoot;
        }

        public Set<KeyAggregationRoutingSplit> getNewSplits() {
            return newSplits;
        }

        public List<KeyAggregationRoutingSplit> getNewSplitsSorted() {
            List<KeyAggregationRoutingSplit> s = new ArrayList<>(newSplits);
            s.sort(Comparator.comparing(KeyAggregationRoutingSplit::getPath));
            return s;
        }

        public boolean isMergedToRoot() {
            return mergedToRoot;
        }

        public boolean isSplitFromRoot() {
            return splitFromRoot;
        }

        @Override
        public ActorKeyAggregation router() {
            return router;
        }

        @Override
        public void merged(KeyAggregationRoutingSplit newMerged, KeyAggregationRoutingSplit oldNode, KeyAggregationRoutingSplit oldAnother) {
            if (oldNode != null) {
                newSplits.remove(oldNode);
            }
            if (oldAnother != null) {
                newSplits.remove(oldAnother);
            }
            if (newMerged != null) {
                newSplits.add(newMerged);
            } else {
                mergedToRoot = true;
            }
        }

        @Override
        public void split(KeyAggregationRoutingSplit newSplit, KeyAggregationRoutingSplit oldNode) {
            if (oldNode != null) {
                newSplits.remove(oldNode);
            } else {
                splitFromRoot = true;
            }
            if (newSplit != null) {
                newSplits.add(newSplit);
            }
        }
    }

    class RoutingSplitLeaf implements KeyAggregationRoutingSplit {
        protected ActorRef actor;
        protected SplitPath path;

        public RoutingSplitLeaf(ActorRef actor, SplitPath path) {
            this.actor = actor;
            this.path = path;
        }

        @Override
        public void process(ActorKeyAggregation router, KeyAggregationStateRouter stateRouter,
                            Object key, MailboxKeyAggregation.HistogramSelection selection, Message<?> message) {
            actor.tell(message.getData(), message.getSender());
        }

        @Override
        public RoutingSplitLeaf adjustPath(SplitPath newPath) {
            return newLeaf(actor, newPath);
        }

        public RoutingSplitLeaf newLeaf(ActorRef actor, SplitPath path) {
            return new RoutingSplitLeaf(actor, path);
        }

        @Override
        public int getDepth() {
            return path.depth();
        }

        @Override
        public SplitPath getPath() {
            return path;
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public KeyAggregationRoutingSplit split(SplitOrMergeContext context, int height) {
            if (getDepth() >= height) {
                return this;
            }
            ActorKeyAggregation self = context.router().toLocal(getActor());
            if (self == null) {
                return this;
            }
            //return split(router, height, self);
            return context.router().internalCreateSplitNode(context, this, self, path, height);
        }

        @Override
        public KeyAggregationRoutingSplit mergeInactive(SplitOrMergeContext context) {
            return this;
        }

        @Override
        public RoutingSplitLeaf mergeIntoLeaf(SplitOrMergeContext context) {
            return this;
        }

        public RoutingSplitLeaf merge(SplitOrMergeContext context, RoutingSplitLeaf leaf, SplitPath path) {
            ActorRef a1 = getActor();
            ActorRef a2 = leaf.getActor();

            if (hasRemainingProcesses(context.router(), a1) || hasRemainingProcesses(context.router(), a2)) {
                return null; //failure
            }
            try {
                if (a1 instanceof ActorKeyAggregation) { //local
                    if (a2 instanceof ActorKeyAggregation) {
                        ((ActorKeyAggregation) a1).internalMerge((ActorKeyAggregation) a2);
                        return newMergedLeaf(context, a1, path, leaf);
                    } else { //remote a2
                        ActorKeyAggregation l2 = context.router().toLocal(a2);
                        if (l2 != null) {
                            ((ActorKeyAggregation) a1).internalMerge(l2);
                        } else {
                            return null;
                        }
                        return newMergedLeaf(context, a1, path, leaf);
                    }
                } else if (a2 instanceof ActorKeyAggregation) { //remote a1, local a2
                    ActorKeyAggregation l1 = context.router().toLocal(a1);
                    if (l1 != null) {
                        ((ActorKeyAggregation) a2).internalMerge(l1);
                    } else {
                        return null;
                    }
                    return newMergedLeaf(context, a2, path, leaf);
                } else { //both remote
                    Boolean b = ResponsiveCalls.sendTask(context.router().getSystem(), a1, (self, sender) -> {
                        ActorKeyAggregation l2 = ((ActorKeyAggregation) self).toLocal(a2);
                        if (l2 != null) {
                            ((ActorKeyAggregation) self).internalMerge(l2);
                            return true;
                        } else {
                            return false;
                        }
                    }).get(context.router().toLocalWaitMs() * 2L, TimeUnit.MILLISECONDS);
                    if (b) {
                        return newMergedLeaf(context, a1, path, leaf);
                    } else {
                        return null;
                    }
                }
            } catch (Throwable ex) {
                context.router().errorToLocal(ex, "merge", a1);
                return null;
            }
        }

        private RoutingSplitLeaf newMergedLeaf(SplitOrMergeContext context, ActorRef a, SplitPath path, RoutingSplitLeaf leaf) {
            RoutingSplitLeaf l = newLeaf(a, path);
            context.merged(l, this, leaf);
            return l;
        }

        public boolean hasRemainingProcesses(ActorKeyAggregation router, ActorRef a) {
            try {
                return ResponsiveCalls.sendTask(router.getSystem(), a, (self, sender) ->
                        ((ActorKeyAggregation) self).hasRemainingProcesses())
                        .get(router.toLocalWaitMs(), TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                router.errorToLocal(ex, "hasRemainingProcess", a);
                return true;
            }
        }

        @Override
        public KeyAggregationRoutingSplit splitOrMerge(SplitOrMergeContext context, int height) {
            if (getDepth() < height) {
                return split(context, height);
            } else {
                return this;
            }
        }

        @Override
        public <ActorType extends Actor> void accept(ActorType actor, ActorRef sender, KeyAggregationVisitor<ActorType> v) {
            v.visitRouterLeaf(actor, sender, this);
        }
    }

    class RoutingSplitNode implements KeyAggregationRoutingSplit {
        protected List<Object> splitPoints;
        protected KeyAggregationRoutingSplit left;
        protected KeyAggregationRoutingSplit right;
        protected SplitPath path;
        protected RoutingHistory history;

        public RoutingSplitNode(List<Object> splitPoints, KeyAggregationRoutingSplit left, KeyAggregationRoutingSplit right, SplitPath path, int historyEntrySize) {
            this.splitPoints = splitPoints;
            this.left = left;
            this.right = right;
            this.path = path;
            history = initRoutingHistory(historyEntrySize);
        }

        public RoutingSplitNode(List<Object> splitPoints, KeyAggregationRoutingSplit left, KeyAggregationRoutingSplit right, SplitPath path, RoutingHistory history) {
            this.splitPoints = splitPoints;
            this.left = left;
            this.right = right;
            this.path = path;
            this.history = history;
        }

        public RoutingSplitNode newNode(KeyAggregationRoutingSplit left, KeyAggregationRoutingSplit right, SplitPath path) {
            return new RoutingSplitNode(splitPoints, left, right, path, history);
        }

        protected RoutingSplitNode newNodeOrThis(KeyAggregationRoutingSplit left, KeyAggregationRoutingSplit right, SplitPath path) {
            if (left != this.left || right != this.right || this.path != path) {
                return newNode(left, right, path);
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
        public RoutingSplitNode adjustPath(SplitPath newPath) {
            return newNode(left, right, newPath);
        }

        @Override
        public int getDepth() {
            return path.depth();
        }

        @Override
        public SplitPath getPath() {
            return path;
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
        public KeyAggregationRoutingSplit split(SplitOrMergeContext context, int height) {
            if (getDepth() < height) {
                return newNodeOrThis(
                        left.split(context, height),
                        right.split(context, height), path);
            }
            return this;
        }

        @Override
        public KeyAggregationRoutingSplit mergeInactive(SplitOrMergeContext context) {
            float r = history.ratioAll();
            float limit = context.router().mergeRatioThreshold();
            if (r > (1 - limit)) { //into left
                return afterMerge(context, true,
                        left.mergeInactive(context),
                        right.mergeIntoLeaf(context));
            } else if (r < limit) { //into right
                return afterMerge(context, false,
                        left.mergeIntoLeaf(context),
                        right.mergeInactive(context));
            } else {
                return newNodeOrThis(
                        left.mergeInactive(context),
                        right.mergeInactive(context), path);
            }
        }

        protected KeyAggregationRoutingSplit afterMerge(SplitOrMergeContext context, boolean intoLeft, KeyAggregationRoutingSplit left, KeyAggregationRoutingSplit right) {
            boolean leftIsLeaf = (left instanceof RoutingSplitLeaf);
            boolean rightIsLeaf = (right instanceof RoutingSplitLeaf);
            if (leftIsLeaf && rightIsLeaf) {
                //merge leaf
                if (intoLeft) {
                    return newNodeOrMerged(left, right, ((RoutingSplitLeaf) left).merge(context, (RoutingSplitLeaf) right, path));
                } else {
                    return newNodeOrMerged(left, right, ((RoutingSplitLeaf) right).merge(context, (RoutingSplitLeaf) left, path));
                }
            } else {
                if (leftIsLeaf) {
                    return newNodeOrMerged(left, right, ((RoutingSplitNode) right).mergeIntoChildLeaf(context, true, (RoutingSplitLeaf) left, path));
                } else if (rightIsLeaf) {
                    return newNodeOrMerged(left, right, ((RoutingSplitNode) left).mergeIntoChildLeaf(context, false, (RoutingSplitLeaf) right, path));
                } else {
                    return newNodeOrThis(left, right, path);
                }
            }
        }

        private KeyAggregationRoutingSplit newNodeOrMerged(KeyAggregationRoutingSplit left, KeyAggregationRoutingSplit right, KeyAggregationRoutingSplit merged) {
            return merged == null ? newNodeOrThis(left, right, path) : merged;
        }

        public KeyAggregationRoutingSplit mergeIntoChildLeaf(SplitOrMergeContext context, boolean intoLeft, RoutingSplitLeaf merged, SplitPath path) {
            KeyAggregationRoutingSplit first = intoLeft ? left : right;
            KeyAggregationRoutingSplit second = intoLeft ? right : left;

            KeyAggregationRoutingSplit newFirst;
            if (first instanceof RoutingSplitLeaf) {
                newFirst = ((RoutingSplitLeaf) first).merge(context, merged, path.add(intoLeft));
            } else {
                newFirst = ((RoutingSplitNode) first).mergeIntoChildLeaf(context, intoLeft, merged, path.add(intoLeft));
            }

            return newFirst == null ?
                    null :
                    newNode(intoLeft,
                        newFirst,
                        second.adjustPath(path.add(!intoLeft)), path);
        }

        private RoutingSplitNode newNode(boolean infoLeft, KeyAggregationRoutingSplit first, KeyAggregationRoutingSplit second, SplitPath path) {
            return newNode(infoLeft ? first : second, infoLeft ? second : first, path);
        }

        @Override
        public KeyAggregationRoutingSplit mergeIntoLeaf(SplitOrMergeContext context) {
            KeyAggregationRoutingSplit mergedLeft = left.mergeIntoLeaf(context);
            KeyAggregationRoutingSplit mergedRight = right.mergeIntoLeaf(context);
            if (mergedLeft instanceof RoutingSplitLeaf && mergedRight instanceof RoutingSplitLeaf) {
                return newNodeOrMerged(mergedLeft, mergedRight, ((RoutingSplitLeaf) mergedLeft).merge(context, (RoutingSplitLeaf) mergedRight, path));
            } else {
                return newNodeOrThis(mergedLeft, mergedRight, path);
            }
        }

        @Override
        public KeyAggregationRoutingSplit splitOrMerge(SplitOrMergeContext context, int height) {
            if (getDepth() < height) {
                return newNodeOrThis(
                        left.splitOrMerge(context, height),
                        right.splitOrMerge(context, height), path);
            } else {
                return mergeIntoLeaf(context);
            }
        }

        @Override
        public <ActorType extends Actor> void accept(ActorType actor, ActorRef sender, KeyAggregationVisitor<ActorType> v) {
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


    class SplitPath implements Comparable<SplitPath> {
        private byte length;
        private byte[] path;

        public SplitPath() {
            path = new byte[1];
        }

        public SplitPath(byte length, byte[] path) {
            this.length = length;
            this.path = path;
        }

        public SplitPath add(boolean left) {
            int bitPos = length % 8;
            int bytePos = length / 8;
            byte[] p;
            p = Arrays.copyOf(path, bytePos + 1);
            int byteMask = 1;
            int byteAdded = 1 << (7 - bitPos);

            switch (bitPos) {
                case 0: byteMask = 0b0000_0000; break;
                case 1: byteMask = 0b1000_0000; break;
                case 2: byteMask = 0b1100_0000; break;
                case 3: byteMask = 0b1110_0000; break;
                case 4: byteMask = 0b1111_0000; break;
                case 5: byteMask = 0b1111_1000; break;
                case 6: byteMask = 0b1111_1100; break;
                case 7: byteMask = 0b1111_1110; break;
            }

            p[bytePos] = (byte) ((p[bytePos] & byteMask) | (left ? byteAdded : 0));
            return new SplitPath((byte) (length + 1), p);
        }

        public boolean[] toFlags() {
            boolean[] flags = new boolean[length];
            for (int i = 0; i < length; ++i) {
                flags[i] = get(i);
            }
            return flags;
        }

        public boolean get(int i) {
            int bytePos = i / 8;
            int bitPos = i % 8;
            int b = ((int) path[bytePos]) << bitPos;
            return ((b & 0b1000_0000) == 0b1000_0000);
        }

        public boolean getLast() {
            return get(depth() - 1);
        }

        public int depth() {
            return length;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append(getClass().getSimpleName());
            buf.append("(");
            boolean first = true;
            for (boolean b : toFlags()) {
                if (first) {
                    first = false;
                } else {
                    buf.append("-");
                }
                buf.append(b ? "L" : "R");
            }
            buf.append(")");
            return buf.toString();
        }

        @Override
        public int compareTo(SplitPath o) {
            int d1 = depth();
            int d2 = o.depth();
            int ps = Math.min(d1, d2);
            for (int i = 0; i < ps; ++i) {
                int n = Boolean.compare(get(i), o.get(i));
                if (n != 0) {
                    return n;
                }
            }
            return Integer.compare(d1, d2);
        }
    }
}
