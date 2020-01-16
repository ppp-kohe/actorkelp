package csl.actor.msgassoc;

import csl.actor.ActorRef;
import csl.actor.MailboxDefault;
import csl.actor.Message;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

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

    public List<SplitTreeRoot> createSplits(ActorReplicable a1, ActorReplicable a2, Random random) {
        List<SplitTreeRoot> splits = new ArrayList<>(entries.length);
        for (EntryTable e : entries) {
            splits.add(e.createSplitRoot(a1, a2, random));
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

    public boolean processWithTakingFromTable(int entryId, ValueInTableMatcher matcher, BiConsumer<Object, List<Object>> handler) {
        return entries[entryId].processWithTaking(matcher, (k,vs) -> {
            if (vs == null) {
                return false;
            } else {
                handler.accept(k, Arrays.asList(vs));
                return true;
            }
        });
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

        public SplitTreeRoot createSplitRoot(ActorReplicable a1, ActorReplicable a2, Random random) {
            return new SplitTreeRoot(createSplit(a1, a2), histogram, random);
        }

        public Split createSplit(ActorReplicable a1, ActorReplicable a2) {
            return new SplitTree(findSplitPoint(),
                    new SplitActor(a1),
                    new SplitActor(a2));
        }

        public Object[] take(ValueInTableMatcher matcher) {
            return processWithTaking(matcher, (k,vs) -> vs);
        }

        public <Ret> Ret processWithTaking(ValueInTableMatcher matcher, BiFunction<Object, Object[], Ret> handler) {
            for (Map.Entry<Object,List<Object>> e : table.entrySet()) {
                List<Object> storeList = e.getValue();
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
                    for (int i = dataSize - 1; i >= 0; --i) {
                        storeList.remove(removeIndices[i]);
                    }
                    return handler.apply(e.getKey(), values);
                }
            }
            return handler.apply(null, null);
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

        public void updatePoint(Comparable<?> newSplitPoint, ActorRef actorRef) {
            split = split.updatePoint(newSplitPoint, actorRef);
        }

        public void send(Message<?> message) {
            split.send(message, histogram);
        }

        public void sendNonKey(Message<?> message) {
            split.sendNonKey(message, random);
        }
    }

    public interface Split {
        Split updatePoint(Comparable<?> newSplitPoint, ActorRef actorRef);
        void send(Message<?> message, KeyHistograms.Histogram histogram);
        void sendNonKey(Message<?> message, Random random);
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

}
