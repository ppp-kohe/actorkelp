package csl.actor.msgassoc;

import csl.actor.ActorRef;
import csl.actor.MailboxDefault;
import csl.actor.Message;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class MailboxReplicable extends MailboxDefault implements Cloneable {
    protected int threshold = 1000;
    protected EntryTable[] entries; //consider performance
    protected HistogramSelector histogramSelector;

    protected Set<ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey<?>> activeAssociations = new HashSet<>();

    public MailboxReplicable create() {
        try {
            MailboxReplicable m = (MailboxReplicable) super.clone();
            int size = entries.length;
            m.entries = new EntryTable[size];
            for (int i = 0; i < size; ++i) {
                m.entries[i] = entries[i].create();
            }
            return m;
        } catch (CloneNotSupportedException cne) {
            throw new RuntimeException(cne);
        }
    }

    public boolean isOverThreshold() {
        return queue.size() > threshold;
    }

    public HistogramSelector getHistogramSelector() {
        return histogramSelector;
    }

    @FunctionalInterface
    public interface HistogramSelector {
        int select(Object value);
    }

    public void initMessageTable(List<Supplier<KeyHistograms.Histogram>> histogramFactories, HistogramSelector histogramSelector) {
        this.histogramSelector = histogramSelector;
        int size = histogramFactories.size();
        entries = new EntryTable[size];
        for (int i = 0; i < size; ++i) {
            entries[i] = new EntryTable(histogramFactories.get(i).get());
        }
    }

    public void putMessageTable(int entryId, Object key, Object value) {
        entries[entryId].put(key, value);
    }

    public void splitMessageTableIntoReplicas(ActorReplicable a1, ActorReplicable a2) {
        MailboxReplicable m1 = a1.getMailboxAsReplicable();
        MailboxReplicable m2 = a2.getMailboxAsReplicable();

        int size = entries.length;
        for (int i = 0; i < size; ++i) {
            entries[i].splitInto(m1.entries[i], m2.entries[i]);
        }
    }

    public List<Split> createSplits(ActorReplicable a1, ActorReplicable a2) {
        List<Split> splits = new ArrayList<>(entries.length);
        for (EntryTable e : entries) {
            splits.add(e.createSplit(a1, a2));
        }
        return splits;
    }

    public List<Comparable<?>> createSplitPoints() {
        List<Comparable<?>> splitPoints = new ArrayList<>(entries.length);
        for (EntryTable e : entries) {
            splitPoints.add(e.findSplitPoint());
        }
        return splitPoints;
    }

    public void addActiveAssociation(ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey<?> assoc) {
        activeAssociations.add(assoc); //TODO deactivation : count down ?
    }

    public boolean processTable() {
        for (ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey<?> a : activeAssociations) {
            if (a.processTable(this)) {
                return true;
            }
        }
        return false;
    }

    public Object[] takeFromTable(int entryId, ValueInTableMatcher matcher) {
        return entries[entryId].take(matcher);
    }

    public interface ValueInTableMatcher {
        int valueSizeInTable();
        boolean matchValueInTable(int index, Object value);
    }

    public static class EntryTable {
        protected Map<Object, List<Object>> table;
        protected KeyHistograms.Histogram histogram;

        public EntryTable(KeyHistograms.Histogram histogram) {
            table = new HashMap<>();
            this.histogram = histogram;
        }

        public EntryTable create() {
            return new EntryTable(histogram.create());
        }

        public void put(Object key, Object value) {
            table.computeIfAbsent(key, _k -> new ArrayList<>())
                    .add(value);

            histogram.put(key);
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

        public Split createSplit(ActorReplicable a1, ActorReplicable a2) {
            return new SplitTree(findSplitPoint(),
                    new SplitActor(a1),
                    new SplitActor(a2));
        }

        public Object[] take(ValueInTableMatcher matcher) {
            for (List<Object> storeList : table.values()) {
                int dataSize = matcher.valueSizeInTable();
                Object[] values = new Object[dataSize];
                int[] removeIndices = new int[dataSize];

                int found = 0;
                for (int j = 0; j < dataSize; ++j) {
                    for (int listIndex = 0, storeSize = storeList.size();
                         listIndex < storeSize;
                         listIndex++) {
                        Object o = storeList.get(listIndex);
                        if (values[j] == null && matcher.matchValueInTable(j, o)) {
                            values[j] = o;
                            removeIndices[j] = listIndex;
                            ++found;
                            break;
                        }
                    }
                }

                if (found == dataSize) {
                    IntStream.range(0, dataSize)
                            .forEach(i -> storeList.remove(removeIndices[dataSize - i - 1]));
                    return values;
                }
            }
            return null;
        }
    }

    public interface Split {
        Split updatePoint(Comparable<?> newSplitPoint, ActorRef actorRef);
        void send(Message<?> message);
        void sendNonKey(Message<?> message);
    }

    public static class SplitActor implements Split {
        protected ActorRef actorRef;

        public SplitActor(ActorRef actorRef) {
            this.actorRef = actorRef;
        }

        @Override
        public Split updatePoint(Comparable<?> newSplitPoint, ActorRef actorRef) {
            return new SplitTree(newSplitPoint, this, new SplitActor(actorRef));
        }

        @Override
        public void send(Message<?> message) {
            actorRef.tell(message.getData(), message.getSender());
        }

        @Override
        public void sendNonKey(Message<?> message) {
            send(message);
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

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public Split updatePoint(Comparable<?> newSplitPoint, ActorRef actorRef) {
            if (((Comparable) point).compareTo(newSplitPoint) > 0) {
                left = left.updatePoint(newSplitPoint, actorRef);
            } else {
                right = right.updatePoint(newSplitPoint, actorRef);
            }
            return this;
        }

        @Override
        public void send(Message<?> message) {
            //TODO compare by histogram?

        }

        @Override
        public void sendNonKey(Message<?> message) {
            //TODO
            left.send(message);
        }
    }
}
