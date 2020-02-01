package csl.actor.msgassoc;

import csl.actor.ActorRef;
import csl.actor.Message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class MailboxAggregationReplicable extends MailboxAggregation {
    protected int threshold;
    private volatile int size;

    public MailboxAggregationReplicable() {
        this(1000);
    }

    public MailboxAggregationReplicable(int threshold) {
        this.threshold = threshold;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

    public boolean isOverThreshold() {
        return size > threshold && hasSufficientPoints();
    }

    public MailboxStatus getStatus() {
        int s = size;
        int t = threshold;
        if (s > t) {
            if (hasSufficientPoints()) {
                return MailboxStatus.Exceeded;
            } else {
                return MailboxStatus.Unready;
            }
        } else if (s < t / 2) {
            return MailboxStatus.Few;
        } else {
            return MailboxStatus.Reasonable;
        }
    }

    public enum MailboxStatus {
        Exceeded {
            @Override
            public boolean isExcessive() {
                return true;
            }
        },
        Reasonable,
        Few {
            @Override
            public boolean isExcessive() {
                return true;
            }
        },
        Unready;

        public boolean isExcessive() {
            return false;
        }
    }


    public int getThreshold() {
        return threshold;
    }


    @Override
    public MailboxAggregationReplicable create() {
        MailboxAggregationReplicable r = (MailboxAggregationReplicable) super.create();
        r.size = 0;
        return r;
    }


    public int size() {
        return size;
    }

    @Override
    public void offer(Message<?> message) {
        ++size; //queue.size() is slow. the volatile field is used here. it is sufficient just for checking over the threshold
        if (size < 0) { //overflow
            size = Integer.MAX_VALUE;
        }
        super.offer(message);
    }

    @Override
    public Message<?> poll() {
        Message<?> m = super.poll();
        if (m != null) {
            size = Math.max(size - 1, 0);
        }
        return m;
    }

    public boolean hasSufficientPoints() {
        for (HistogramEntry h : tables) {
            if (h.getTree().hasSufficientPoints()) {
                return true;
            }
        }
        return false;
    }

    public List<Object> splitMessageTableIntoReplicas(ActorAggregationReplicable a1, ActorAggregationReplicable a2) {
        return splitMessageTableIntoReplicas(a1.getMailboxAsReplicable(), a2.getMailboxAsReplicable());
    }

    public List<Object> splitMessageTableIntoReplicas(MailboxAggregationReplicable m1, MailboxAggregationReplicable m2) {
        int size = tables.length;
        List<Object> splitPoints = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            KeyHistograms.HistogramTree rt = tables[i].getTree();
            KeyHistograms.HistogramTree lt = rt.split();
            m1.tables[i].setTree(lt);
            m2.tables[i].setTree(rt);
            tables[i] = tables[i].create();
            splitPoints.add(rt.splitPointAsRightHandSide(lt));
        }
        return splitPoints;
    }

    public Object extractKey(HistogramSelection selection, Message<?> message) {
        HistogramProcessor p = tables[selection.entryId].getProcessor();
        return p.extractKeyFromValue(message.getData(), selection.position);
    }

    @SuppressWarnings("unchecked")
    public boolean compare(int entryId, Object key, Object point) {
        HistogramProcessor p = tables[entryId].getProcessor();
        return ((KeyHistograms.KeyComparator<Object>) p.getKeyComparator()).compare(key, point) < 0;
    }

    public void merge(MailboxAggregationReplicable m) {
        size += m.size;
        queue.addAll(m.queue); //it does not change target of each message
        if (size < 0) { //overflow
            size = Integer.MAX_VALUE;
        }

        for (int i = 0, l = tables.length; i < l; ++i) {
            HistogramEntry e1 = tables[i];
            HistogramEntry e2 = m.tables[i];
            e1.getTree().merge(e2.getTree());
        }
    }
/*
    @Deprecated
    public List<SplitTreeRoot> createSplits(ActorRef a1, ActorRef a2, Random random, List<Object> splitPoints, int depth) {
        List<SplitTreeRoot> splits = new ArrayList<>(tables.length);
        int i = 0;
        for (HistogramEntry e : tables) {
            splits.add(new SplitTreeRoot(new SplitTree(splitPoints.get(i), new SplitActor(a1, depth + 1), new SplitActor(a2, depth + 1), depth), e.getProcessor(), random));
            ++i;
        }
        return splits;
    }

    @Deprecated
    public static class SplitTreeRoot {
        protected Split split;
        protected HistogramProcessor processor;
        protected Random random;

        public SplitTreeRoot(Split split, HistogramProcessor processor, Random random) {
            this.split = split;
            this.processor = processor;
            this.random = random;
        }

        public Split getSplit() {
            return split;
        }

        /** @return implementation field getter *//*
        public HistogramProcessor getProcessor() {
            return processor;
        }

        /** @return implementation field getter *//*
        public Random getRandom() {
            return random;
        }

        public Object getSplitPoint() {
            if (split instanceof SplitTree) {
                return ((SplitTree) split).getPoint();
            } else {
                return null; //never
            }
        }

        public void updatePoint(Object newSplitPoint, ActorRef left, ActorRef right) {
            split = split.updatePoint(newSplitPoint, left, right, processor);
        }

        public void send(Message<?> message, Object position) {
            Object key = processor.extractKeyFromValue(message.getData(), position);
            split.send(message, key, processor);
        }

        public void sendNonKey(Message<?> message) {
            split.sendNonKey(message, random);
        }
    }

    @Deprecated
    public interface Split {
        Split updatePoint(Object newSplitPoint, ActorRef left, ActorRef right, HistogramProcessor processor);
        void send(Message<?> message, Object key, HistogramProcessor processor);
        void sendNonKey(Message<?> message, Random random);

        int getDepth();
    }

    @Deprecated
    public static class SplitActor implements Split {
        protected ActorRef actorRef;
        protected int depth;

        public SplitActor(ActorRef actorRef, int depth) {
            this.actorRef = actorRef;
            this.depth = depth;
        }

        /** @return implementation field getter *//*
        public ActorRef getActorRef() {
            return actorRef;
        }

        @Override
        public int getDepth() {
            return depth;
        }

        @Override
        public Split updatePoint(Object newSplitPoint, ActorRef left, ActorRef right, HistogramProcessor processor) {
            return new SplitTree(newSplitPoint, new SplitActor(left, depth + 1), new SplitActor(right, depth + 1), depth);
        }

        @Override
        public void send(Message<?> message, Object key, HistogramProcessor processor) {
            actorRef.tell(message.getData(), message.getSender());
        }

        @Override
        public void sendNonKey(Message<?> message, Random random) {
            actorRef.tell(message.getData(), message.getSender());
        }
    }

    @Deprecated
    public static class SplitTree implements Split {
        protected Object point;
        protected Split left;
        protected Split right;
        protected int depth;

        public SplitTree(Object point, Split left, Split right, int depth) {
            this.point = point;
            this.left = left;
            this.right = right;
            this.depth = depth;
        }

        public Object getPoint() {
            return point;
        }

        /** @return implementation field getter *//*
        public Split getLeft() {
            return left;
        }

        /** @return implementation field getter *//*
        public Split getRight() {
            return right;
        }

        @Override
        public int getDepth() {
            return depth;
        }

        @Override
        public Split updatePoint(Object newSplitPoint, ActorRef la, ActorRef ra, HistogramProcessor processor) {
            if (compareKeyToPoint(newSplitPoint, processor)) {
                left = left.updatePoint(newSplitPoint, la, ra, processor);
            } else {
                right = right.updatePoint(newSplitPoint, la, ra, processor);
            }
            return this;
        }

        @Override
        public void send(Message<?> message, Object key, HistogramProcessor processor) {
            if (compareKeyToPoint(key, processor)) {
                left.send(message, key, processor);
            } else {
                right.send(message, key, processor);
            }
        }

        @SuppressWarnings("unchecked")
        protected boolean compareKeyToPoint(Object key, HistogramProcessor processor) {
            //the split point is currently included in the right hand side by the implementation of HistogramTree.split
            return ((KeyHistograms.KeyComparator<Object>) processor.getKeyComparator()).compare(key, point) < 0;
        }

        @Override
        public void sendNonKey(Message<?> message, Random random) {
            if (random.nextBoolean()) {
                left.sendNonKey(message, random);
            } else {
                right.sendNonKey(message, random);
            }
        }
    }
*/
    public void serializeTo(ActorAggregationReplicable.ActorReplicableSerializableState state) {
        state.messages = queue.toArray(new Message[0]);
        state.threshold = threshold;
        state.tables = Arrays.stream(tables)
                .map(HistogramEntry::getTree)
                .collect(Collectors.toList());
    }

    public void deserializeFrom(ActorAggregationReplicable.ActorReplicableSerializableState state) {
        queue.addAll(Arrays.asList(state.messages));
        threshold = state.threshold;
        int i = 0;
        for (KeyHistograms.HistogramTree t : state.tables) {
            tables[i].setTree(t);
            ++i;
        }
    }

}
