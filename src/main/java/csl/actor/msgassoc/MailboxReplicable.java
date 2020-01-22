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

    //TODO remove
    @Deprecated @Override
    protected EntryTable createTable(KeyHistograms.Histogram histogram) {
        return new EntryTableReplicable(histogram);
    }

    public List<Object> splitMessageTableIntoReplicas(ActorReplicable a1, ActorReplicable a2) {
        MailboxReplicable m1 = a1.getMailboxAsReplicable();
        MailboxReplicable m2 = a2.getMailboxAsReplicable();

        int size = tables.length;
        List<Object> splitPoints = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            KeyHistograms.HistogramTree lt = tables[i];
            KeyHistograms.HistogramTree rt = lt.split();
            m1.tables[i] = lt;
            m2.tables[i] = rt;
            splitPoints.add(lt.splitPoint(rt));
        }

        //TODO remove
        size = entries.length;
        for (int i = 0; i < size; ++i) {
            ((EntryTableReplicable) entries[i]).splitInto(m1.entries[i], m2.entries[i]);
        }

        return splitPoints;
    }

    public List<SplitTreeRoot> createSplits(ActorRef a1, ActorRef a2, Random random, List<Object> splitPoints) {
        List<SplitTreeRoot> splits = new ArrayList<>(tables.length);
        int i = 0;
        for (KeyHistograms.HistogramTree t : tables) {
            splits.add(new SplitTreeRoot(new SplitTree(splitPoints.get(0), new SplitActor(a1), new SplitActor(a2)), null, t, random));
        }

        //TODO remove
        for (EntryTable e : entries) {
            //splits.add(((EntryTableReplicable) e).createSplitRoot(a1, a2, random, splitPoints.get(i)));
            ++i;
        }
        return splits;
    }

    //TODO remove
    @Deprecated public static class EntryTableReplicable extends EntryTable implements Serializable {

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

        public SplitTreeRoot createSplitRoot(ActorRef a1, ActorRef a2, Random random, Object splitPoint) {
            return new SplitTreeRoot(createSplit(splitPoint, a1, a2), histogram, null, random);
        }

        public Split createSplit(Object splitPoint, ActorRef a1, ActorRef a2) {
            return new SplitTree(splitPoint,
                    new SplitActor(a1),
                    new SplitActor(a2));
        }
    }

    public static class SplitTreeRoot {
        protected Split split;
        @Deprecated protected KeyHistograms.Histogram histogram; //TODO remove
        protected KeyHistograms.HistogramTree histogramTree;
        protected Random random;

        public SplitTreeRoot(Split split, KeyHistograms.Histogram histogram, KeyHistograms.HistogramTree histogramTree, Random random) {
            this.split = split;
            this.histogram = histogram;
            this.histogramTree = histogramTree;
            this.random = random;
        }

        public Object getSplitPoint() {
            if (split instanceof SplitTree) {
                return ((SplitTree) split).getPoint();
            } else {
                return null; //never
            }
        }

        public void updatePoint(Object newSplitPoint, ActorRef left, ActorRef right) {
            split = split.updatePoint(newSplitPoint, left, right, histogram, histogramTree);
        }

        public void send(Message<?> message) {
            split.send(message, histogram, histogramTree);
        }

        public void sendNonKey(Message<?> message) {
            split.sendNonKey(message, random);
        }
    }

    public interface Split {
        Split updatePoint(Object newSplitPoint, ActorRef left, ActorRef right, KeyHistograms.Histogram histogram, KeyHistograms.HistogramTree histogramTree);
        void send(Message<?> message, KeyHistograms.Histogram histogram, KeyHistograms.HistogramTree histogramTree);
        void sendNonKey(Message<?> message, Random random);
    }

    public static class SplitActor implements Split {
        protected ActorRef actorRef;

        public SplitActor(ActorRef actorRef) {
            this.actorRef = actorRef;
        }

        @Override
        public Split updatePoint(Object newSplitPoint, ActorRef left, ActorRef right, KeyHistograms.Histogram histogram, KeyHistograms.HistogramTree histogramTree) {
            return new SplitTree(newSplitPoint, new SplitActor(left), new SplitActor(right));
        }

        @Override
        public void send(Message<?> message, KeyHistograms.Histogram histogram, KeyHistograms.HistogramTree histogramTree) {
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

        @Override
        public Split updatePoint(Object newSplitPoint, ActorRef la, ActorRef ra, KeyHistograms.Histogram histogram, KeyHistograms.HistogramTree histogramTree) {

            //TODO remove
            //if (histogram.compareSplitPoints(newSplitPoint, point) < 0) {
            if (histogramTree.compareSplitPoints(newSplitPoint, point)) {
                left = left.updatePoint(newSplitPoint, la, ra, histogram, histogramTree);
            } else {
                right = right.updatePoint(newSplitPoint, la, ra, histogram, histogramTree);
            }
            return this;
        }

        @Override
        public void send(Message<?> message, KeyHistograms.Histogram histogram, KeyHistograms.HistogramTree histogramTree) {
            //TODO how to compare value with point?
            if (histogram.compareToSplitPoint(message.getData(), (Comparable) point) < 0) { //TODO remove
                left.send(message, histogram, histogramTree);
            } else {
                right.send(message, histogram, histogramTree);
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
        state.tables = tables;
        //TODO remove
        state.entries = entries;
        state.threshold = threshold;
    }

    public void deserializeFrom(ActorReplicable.ActorReplicableSerializableState state) {
        queue.addAll(Arrays.asList(state.messages));
        tables = state.tables;
        //TODO remove
        entries = state.entries;
        threshold = state.threshold;
    }
}
