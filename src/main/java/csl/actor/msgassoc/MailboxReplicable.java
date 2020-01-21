package csl.actor.msgassoc;

import csl.actor.ActorRef;
import csl.actor.Message;

import java.io.Serializable;
import java.util.*;

public class MailboxReplicable extends MailboxAggregation {
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
        for (EntryTable e : entries) {
            if (e.hasMultiplePoints()) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected EntryTable createTable(KeyHistograms.Histogram histogram) {
        return new EntryTableReplicable(histogram);
    }

    public void splitMessageTableIntoReplicas(ActorReplicable a1, ActorReplicable a2) {
        MailboxReplicable m1 = a1.getMailboxAsReplicable();
        MailboxReplicable m2 = a2.getMailboxAsReplicable();

        int size = entries.length;
        for (int i = 0; i < size; ++i) {
            ((EntryTableReplicable) entries[i]).splitInto(m1.entries[i], m2.entries[i]);
        }
    }

    public List<SplitTreeRoot> createSplits(ActorRef a1, ActorRef a2, Random random) {
        List<SplitTreeRoot> splits = new ArrayList<>(entries.length);
        for (EntryTable e : entries) {
            splits.add(((EntryTableReplicable) e).createSplitRoot(a1, a2, random));
        }
        return splits;
    }

    public static class EntryTableReplicable extends EntryTable implements Serializable {

        public EntryTableReplicable(KeyHistograms.Histogram histogram) {
            super(histogram);
        }

        @Override
        public EntryTable create() {
            return new EntryTableReplicable(histogram.create());
        }

        public void splitInto(EntryTable e1, EntryTable e2) {
            Comparable<?> div = findSplitPoint();
            for (Map.Entry<Object, List<Object>> e : table.entrySet()) {
                Object key = e.getKey();
                int r = histogram.compareToSplitPoint(key, div);
                if (r < 0) { //key < div
                    e1.put(key, e.getValue());
                } else {
                    e2.put(key, e.getValue());
                }
            }
        }

        public Comparable<?> findSplitPoint() {
            return histogram.findSplitPoint();
        }

        public SplitTreeRoot createSplitRoot(ActorRef a1, ActorRef a2, Random random) {
            return new SplitTreeRoot(createSplit(a1, a2), histogram, random);
        }

        public Split createSplit(ActorRef a1, ActorRef a2) {
            return new SplitTree(findSplitPoint(),
                    new SplitActor(a1),
                    new SplitActor(a2));
        }
    }

    public static class SplitTreeRoot {
        protected Split split;
        protected KeyHistograms.Histogram histogram;
        protected Random random;

        public SplitTreeRoot(Split split, KeyHistograms.Histogram histogram, Random random) {
            this.split = split;
            this.histogram = histogram;
            this.random = random;
        }

        public Comparable<?> getSplitPoint() {
            if (split instanceof SplitTree) {
                return ((SplitTree) split).getPoint();
            } else {
                return null; //never
            }
        }

        public void updatePoint(Comparable<?> newSplitPoint, ActorRef left, ActorRef right) {
            split = split.updatePoint(newSplitPoint, left, right, histogram);
        }

        public void send(Message<?> message) {
            split.send(message, histogram);
        }

        public void sendNonKey(Message<?> message) {
            split.sendNonKey(message, random);
        }
    }

    public interface Split {
        Split updatePoint(Comparable<?> newSplitPoint, ActorRef left, ActorRef right, KeyHistograms.Histogram histogram);
        void send(Message<?> message, KeyHistograms.Histogram histogram);
        void sendNonKey(Message<?> message, Random random);
    }

    public static class SplitActor implements Split {
        protected ActorRef actorRef;

        public SplitActor(ActorRef actorRef) {
            this.actorRef = actorRef;
        }

        @Override
        public Split updatePoint(Comparable<?> newSplitPoint, ActorRef left, ActorRef right, KeyHistograms.Histogram histogram) {
            return new SplitTree(newSplitPoint, new SplitActor(left), new SplitActor(right));
        }

        @Override
        public void send(Message<?> message, KeyHistograms.Histogram histogram) {
            actorRef.tell(message.getData(), message.getSender());
        }

        @Override
        public void sendNonKey(Message<?> message, Random random) {
            actorRef.tell(message.getData(), message.getSender());
        }
    }

    public static class SplitTree implements Split {
        protected Comparable<?> point;
        protected Split left;
        protected Split right;

        public SplitTree(Comparable<?> point, Split left, Split right) {
            this.point = point;
            this.left = left;
            this.right = right;
        }

        public Comparable<?> getPoint() {
            return point;
        }

        @Override
        public Split updatePoint(Comparable<?> newSplitPoint, ActorRef la, ActorRef ra, KeyHistograms.Histogram histogram) {
            if (histogram.compareSplitPoints(newSplitPoint, point) < 0) {
                left = left.updatePoint(newSplitPoint, la, ra, histogram);
            } else {
                right = right.updatePoint(newSplitPoint, la, ra, histogram);
            }
            return this;
        }

        @Override
        public void send(Message<?> message, KeyHistograms.Histogram histogram) {
            if (histogram.compareToSplitPoint(message.getData(), point) < 0) {
                left.send(message, histogram);
            } else {
                right.send(message, histogram);
            }
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

    public void serializeTo(ActorReplicable.ActorReplicableSerializableState state) {
        state.messages = queue.toArray(new Message[0]);
        state.entries = entries;
        state.threshold = threshold;
    }

    public void deserializeFrom(ActorReplicable.ActorReplicableSerializableState state) {
        queue.addAll(Arrays.asList(state.messages));
        entries = state.entries;
        threshold = state.threshold;
    }
}
