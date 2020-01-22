package csl.actor.msgassoc;

import csl.actor.MailboxDefault;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class MailboxAggregation extends MailboxDefault {
    @Deprecated  protected EntryTable[] entries; //consider performance //TODO remove
    protected HistogramSelector histogramSelector;

    protected Set<ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey<?>> activeAssociations = new HashSet<>();

    protected KeyHistograms.HistogramTree[] tables;

    public MailboxAggregation create() {
        try {
            MailboxAggregation m = (MailboxAggregation) super.clone();
            int size = entries.length;
            //TODO remove
            m.entries = new EntryTable[size];
            for (int i = 0; i < size; ++i) {
                m.entries[i] = entries[i].create();
            }

            m.tables = new KeyHistograms.HistogramTree[size];
            for (int i = 0; i < size; ++i) {
                m.tables[i] = tables[i].create();
            }
            return m;
        } catch (CloneNotSupportedException cne) {
            throw new RuntimeException(cne);
        }
    }

    public HistogramSelector getHistogramSelector() {
        return histogramSelector;
    }


    @FunctionalInterface
    public interface HistogramSelector {
        int select(Object value);
    }

    public void initMessageTable(List<Supplier<KeyHistograms.Histogram>> histogramFactories, List<KeyHistograms.KeyComparator<?>> keyComparators, HistogramSelector histogramSelector) {
        this.histogramSelector = histogramSelector;
        int size = histogramFactories.size();
        tables = new KeyHistograms.HistogramTree[size];
        for (int i = 0; i < size; ++i) {
            tables[i] = new KeyHistograms.HistogramTree(keyComparators.get(i));
        }

        //TODO remove
        entries = new EntryTable[size];
        for (int i = 0; i < size; ++i) {
            entries[i] = createTable(histogramFactories.get(i).get());
        }
    }

    //TODO remove
    @Deprecated protected EntryTable createTable(KeyHistograms.Histogram histogram) {
        return new EntryTable(histogram);
    }

    //TODO remove
    @Deprecated public void putMessageTable(int entryId, Object key, Object value) {
        entries[entryId].put(key, value);
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

    public KeyHistograms.HistogramTree getTable(int entryId) {
        return tables[entryId];
    }

    //TODO remove
    @Deprecated  public Object[] takeFromTable(int entryId, ValueInTableMatcher matcher) {
        return entries[entryId].take(matcher);
    }

    //TODO remove
    @Deprecated  public boolean processWithTakingFromTable(int entryId, ValueInTableMatcher matcher, BiConsumer<Object, List<Object>> handler) {
        return entries[entryId].processWithTaking(matcher, (k,vs) -> {
            if (vs == null) {
                return false;
            } else {
                handler.accept(k, Arrays.asList(vs));
                return true;
            }
        });
    }

    @Deprecated public interface ValueInTableMatcher {
        int valueSizeInTable();
        boolean matchValueInTable(int index, Object value);
    }

    //TODO remove
    @Deprecated  public static class EntryTable {
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

        public Object[] take(ValueInTableMatcher matcher) {
            return processWithTaking(matcher, (k,vs) -> vs);
        }

        public <Ret> Ret processWithTaking(ValueInTableMatcher matcher, BiFunction<Object, Object[], Ret> handler) {
            for (Iterator<Map.Entry<Object,List<Object>>> ei = table.entrySet().iterator();
                 ei.hasNext();) {
                Map.Entry<Object,List<Object>> e = ei.next();
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
                    if (storeList.isEmpty()) {
                        ei.remove();
                    }
                    return handler.apply(e.getKey(), values);
                }
            }
            return handler.apply(null, null);
        }

        public boolean hasMultiplePoints() {
            return histogram.hasMultiplePoints();
        }
    }
}
