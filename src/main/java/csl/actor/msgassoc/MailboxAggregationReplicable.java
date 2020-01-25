package csl.actor.msgassoc;

import csl.actor.ActorRef;
import csl.actor.Message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class MailboxAggregationReplicable extends MailboxAggregation {
    protected int threshold = 1000;

    public boolean isOverThreshold() {
        return queue.size() > threshold && hasMultiplePoints();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int getThreshold() {
        return threshold;
    }

    public boolean hasMultiplePoints() {
        for (HistogramEntry h : tables) {
            if (h.getTree().hasMultiplePoints()) {
                return true;
            }
        }
        return false;
    }

    public List<Object> splitMessageTableIntoReplicas(ActorAggregationReplicable a1, ActorAggregationReplicable a2) {
        MailboxAggregationReplicable m1 = a1.getMailboxAsReplicable();
        MailboxAggregationReplicable m2 = a2.getMailboxAsReplicable();

        int size = tables.length;
        List<Object> splitPoints = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            KeyHistograms.HistogramTree rt = tables[i].getTree();
            KeyHistograms.HistogramTree lt = rt.split();
            m1.tables[i].setTree(lt);
            m2.tables[i].setTree(rt);
            splitPoints.add(rt.splitPointAsRightHandSide(lt));
        }
        return splitPoints;
    }

    public List<SplitTreeRoot> createSplits(ActorRef a1, ActorRef a2, Random random, List<Object> splitPoints) {
        List<SplitTreeRoot> splits = new ArrayList<>(tables.length);
        int i = 0;
        for (HistogramEntry e : tables) {
            splits.add(new SplitTreeRoot(new SplitTree(splitPoints.get(i), new SplitActor(a1), new SplitActor(a2)), e.getProcessor(), random));
            ++i;
        }
        return splits;
    }

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

        /** @return implementation field getter */
        public HistogramProcessor getProcessor() {
            return processor;
        }

        /** @return implementation field getter */
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

    public interface Split {
        Split updatePoint(Object newSplitPoint, ActorRef left, ActorRef right, HistogramProcessor processor);
        void send(Message<?> message, Object key, HistogramProcessor processor);
        void sendNonKey(Message<?> message, Random random);
    }

    public static class SplitActor implements Split {
        protected ActorRef actorRef;

        public SplitActor(ActorRef actorRef) {
            this.actorRef = actorRef;
        }

        /** @return implementation field getter */
        public ActorRef getActorRef() {
            return actorRef;
        }

        @Override
        public Split updatePoint(Object newSplitPoint, ActorRef left, ActorRef right, HistogramProcessor processor) {
            return new SplitTree(newSplitPoint, new SplitActor(left), new SplitActor(right));
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

    public static class SplitTree implements Split {
        protected Object point;
        protected Split left;
        protected Split right;

        public SplitTree(Object point, Split left, Split right) {
            this.point = point;
            this.left = left;
            this.right = right;
        }

        public Object getPoint() {
            return point;
        }

        /** @return implementation field getter */
        public Split getLeft() {
            return left;
        }

        /** @return implementation field getter */
        public Split getRight() {
            return right;
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
