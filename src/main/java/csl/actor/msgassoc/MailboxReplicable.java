package csl.actor.msgassoc;

import csl.actor.MailboxDefault;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class MailboxReplicable extends MailboxDefault {
    protected int threshold = 1000;
    protected EntryTable[] entries; //consider performance

    public MailboxReplicable create() {
        MailboxReplicable m = new MailboxReplicable();
        int size = entries.length;
        m.entries = new EntryTable[size];
        for (int i = 0; i < size; ++i) {
            m.entries[i] = entries[i].create();
        }
        return m;
    }

    public boolean isOverThreshold() {
        return queue.size() > threshold;
    }

    public Map<Class<?>, Function<?,?>> createClassFunctionMap() {
        return null; //TODO
    }

    public Object createDividingPoints() {
        //TODO
        return null;
    }

    public void initTable(List<Supplier<KeyHistograms.Histogram>> histogramFactories) {
        int size = histogramFactories.size();
        entries = new EntryTable[size];
        for (int i = 0; i < size; ++i) {
            entries[i] = new EntryTable(histogramFactories.get(i).get());
        }
    }

    public void putMessageTable(int entryId, Object key, Object value) {
        entries[entryId].put(key, value);
    }

    public void splitMessageTableToReplicas(ActorReplicable a1, ActorReplicable a2) {
        MailboxReplicable m1 = a1.getMailboxAsReplicable();
        MailboxReplicable m2 = a2.getMailboxAsReplicable();

        int size = entries.length;
        for (int i = 0; i < size; ++i) {
            entries[i].split(m1.entries[i], m2.entries[i]);
        }
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
            table.computeIfAbsent(key, _k -> new LinkedList<>())
                    .add(value);
            histogram.put(key);
        }

        public void split(EntryTable e1, EntryTable e2) {
            Comparable<?> div = histogram.findDividingPoint();
            for (Map.Entry<Object, List<Object>> e : table.entrySet()) {
                Object key = e.getKey();
                int r = histogram.compareToDividingPoint(key, div);
                if (r < 0) { //key < div
                    e1.put(key, e.getValue());
                } else {
                    e2.put(key, e.getValue());
                }
            }
        }
    }
}
