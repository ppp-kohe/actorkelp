package csl.actor.msgassoc;

import csl.actor.MailboxDefault;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class MailboxAggregation extends MailboxDefault {
    protected HistogramEntry[] tables;

    public MailboxAggregation create() {
        try {
            MailboxAggregation m = (MailboxAggregation) super.clone();
            int size = entries.length;
            //TODO remove
            m.entries = new EntryTable[size];
            for (int i = 0; i < size; ++i) {
                m.entries[i] = entries[i].create();
            }

            m.tables = new HistogramEntry[size];
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



    public interface HistogramProcessor {
        KeyHistograms.KeyComparator<?> getKeyComparator();
        boolean processTable(MailboxAggregation m);
        Object selectFromValue(Object value);
        Object extractKeyFromValue(Object value, Object position);
    }


    public static class HistogramEntry {
        HistogramProcessor processor;
        KeyHistograms.HistogramTree tree;

        public HistogramEntry(HistogramProcessor p) {
            this.processor = p;
            tree = new KeyHistograms.HistogramTree(p.getKeyComparator());
        }

        public HistogramEntry create() {
            return new HistogramEntry(processor);
        }

        public KeyHistograms.HistogramTree getTree() {
            return tree;
        }

        public void setTree(KeyHistograms.HistogramTree tree) {
            this.tree = tree;
        }

        public boolean processTable(MailboxAggregation m) {
            return processor.processTable(m);
        }

        public HistogramProcessor getProcessor() {
            return processor;
        }
    }

    public void initMessageTable(List<Supplier<KeyHistograms.Histogram>> histogramFactories, List<HistogramProcessor> processors, HistogramSelector histogramSelector) {
        int size = processors.size();
        tables = new HistogramEntry[size];
        for (int i = 0; i < size; ++i) {
            tables[i] = new HistogramEntry(processors.get(i));
        }

        //TODO remove
        this.histogramSelector = histogramSelector;
        entries = new EntryTable[size];
        for (int i = 0; i < size; ++i) {
            entries[i] = createTable(histogramFactories.get(i).get());
        }
    }

    public boolean processTable() {
        //TODO remove
        for (ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey<?> a : activeAssociations) {
            if (a.processTable(this)) {
                return true;
            }
        }
        for (HistogramEntry e : tables) {
            if (e.processTable(this)) {
                return true;
            }
        }
        return false;
    }

    public KeyHistograms.HistogramTree getTable(int entryId) {
        return tables[entryId].getTree();
    }

    public HistogramSelection selectTable(Object value) {
        HistogramSelection s = new HistogramSelection();
        int i = 0;
        for (HistogramEntry e : tables) {
            Object position = e.getProcessor().selectFromValue(value);
            if (position != null) {
                s.entryId = i;
                s.position = position;
                return s;
            }
            ++i;
        }
        return null;
    }

    public static class HistogramSelection {
        public int entryId;
        public Object position;

        public HistogramSelection() { }

        public HistogramSelection(int entryId, Object position) {
            this.entryId = entryId;
            this.position = position;
        }
    }

    @Deprecated  protected EntryTable[] entries; //consider performance //TODO remove
    @Deprecated protected HistogramSelector histogramSelector;

    @Deprecated  protected Set<ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey<?>> activeAssociations = new HashSet<>();


    //TODO remove
    @Deprecated @FunctionalInterface
    public interface HistogramSelector {
        int select(Object value);
    }

    //TODO remove
    @Deprecated protected EntryTable createTable(KeyHistograms.Histogram histogram) {
        return new EntryTable(histogram);
    }

    //TODO remove
    @Deprecated public void putMessageTable(int entryId, Object key, Object value) {
        entries[entryId].put(key, value);
    }

    //TODO remove
    @Deprecated
    public void addActiveAssociation(ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey<?> assoc) {
        activeAssociations.add(assoc); //TODO deactivation : count down ?
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

    //TODO remove
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
