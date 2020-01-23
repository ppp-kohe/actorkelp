package csl.actor.msgassoc;

import csl.actor.ActorRef;
import csl.actor.Message;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

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
        for (HistogramEntry h : tables) {
            if (h.getTree().hasMultiplePoints()) {
                return true;
            }
        }

        //TODO remove
        for (EntryTable e : entries) {
            if (e.hasMultiplePoints()) {
                return true;
            }
        }
        return false;
    }

    public List<Object> splitMessageTableIntoReplicas(ActorReplicable a1, ActorReplicable a2) {
        MailboxReplicable m1 = a1.getMailboxAsReplicable();
        MailboxReplicable m2 = a2.getMailboxAsReplicable();

        int size = tables.length;
        List<Object> splitPoints = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            KeyHistograms.HistogramTree lt = tables[i].getTree();
            KeyHistograms.HistogramTree rt = lt.split();
            m1.tables[i].setTree(lt);
            m2.tables[i].setTree(rt);
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
        for (HistogramEntry e : tables) {
            splits.add(new SplitTreeRoot(new SplitTree(splitPoints.get(0), new SplitActor(a1), new SplitActor(a2)), null, e.getProcessor(), random));
        }

        //TODO remove
        for (EntryTable e : entries) {
            //splits.add(((EntryTableReplicable) e).createSplitRoot(a1, a2, random, splitPoints.get(i)));
            ++i;
        }
        return splits;
    }

    public static class SplitTreeRoot {
        @Deprecated protected KeyHistograms.Histogram histogram; //TODO remove

        protected Split split;
        protected HistogramProcessor processor;
        protected Random random;

        public SplitTreeRoot(Split split, KeyHistograms.Histogram histogram, HistogramProcessor processor, Random random) {
            this.split = split;
            this.histogram = histogram;
            this.processor = processor;
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
            split = split.updatePoint(newSplitPoint, left, right, histogram, processor);
        }

        public void send(Message<?> message, Object position) {
            Object key = processor.extractKeyFromValue(message.getData(), position);
            split.send(message, key, histogram, processor);
        }

        public void sendNonKey(Message<?> message) {
            split.sendNonKey(message, random);
        }
    }

    public interface Split {
        Split updatePoint(Object newSplitPoint, ActorRef left, ActorRef right, KeyHistograms.Histogram histogram, HistogramProcessor processor);
        void send(Message<?> message, Object key, KeyHistograms.Histogram histogram, HistogramProcessor processor);
        void sendNonKey(Message<?> message, Random random);
    }

    public static class SplitActor implements Split {
        protected ActorRef actorRef;

        public SplitActor(ActorRef actorRef) {
            this.actorRef = actorRef;
        }

        @Override
        public Split updatePoint(Object newSplitPoint, ActorRef left, ActorRef right, KeyHistograms.Histogram histogram, HistogramProcessor processor) {
            return new SplitTree(newSplitPoint, new SplitActor(left), new SplitActor(right));
        }

        @Override
        public void send(Message<?> message, Object key, KeyHistograms.Histogram histogram, HistogramProcessor processor) {
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
        public Split updatePoint(Object newSplitPoint, ActorRef la, ActorRef ra, KeyHistograms.Histogram histogram, HistogramProcessor processor) {
            //TODO remove
            //if (histogram.compareSplitPoints(newSplitPoint, point) < 0) {
            if (compareKeyToPoint(newSplitPoint, processor)) {
                left = left.updatePoint(newSplitPoint, la, ra, histogram, processor);
            } else {
                right = right.updatePoint(newSplitPoint, la, ra, histogram, processor);
            }
            return this;
        }

        @Override
        public void send(Message<?> message, Object key, KeyHistograms.Histogram histogram, HistogramProcessor processor) {
            //if (histogram.compareToSplitPoint(message.getData(), (Comparable) point) < 0) { //TODO remove
            if (compareKeyToPoint(key, processor)) {
                left.send(message, key, histogram, processor);
            } else {
                right.send(message, key, histogram, processor);
            }
        }

        @SuppressWarnings("unchecked")
        protected boolean compareKeyToPoint(Object key, HistogramProcessor processor) {
            return ((KeyHistograms.KeyComparator<Object>) processor.getKeyComparator()).compare(key, point) <= 0;
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
        state.threshold = threshold;
        state.tables = Arrays.stream(tables)
                .map(HistogramEntry::getTree)
                .collect(Collectors.toList());

        //TODO remove
        state.entries = entries;
    }

    public void deserializeFrom(ActorReplicable.ActorReplicableSerializableState state) {
        queue.addAll(Arrays.asList(state.messages));
        threshold = state.threshold;
        int i = 0;
        for (KeyHistograms.HistogramTree t : state.tables) {
            tables[i].setTree(t);
            ++i;
        }

        //TODO remove
        entries = state.entries;
    }


    //TODO remove
    @Deprecated @Override
    protected EntryTable createTable(KeyHistograms.Histogram histogram) {
        return new EntryTableReplicable(histogram);
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

}
