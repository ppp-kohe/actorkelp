package csl.actor.msgassoc;

import csl.actor.MailboxDefault;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MailboxAggregation extends MailboxDefault implements Cloneable {
    protected int treeSize;
    protected HistogramEntry[] tables;

    public MailboxAggregation() {
        this(32);
    }

    public MailboxAggregation(int treeSize) {
        this.treeSize = treeSize;
    }

    public MailboxAggregation create() {
        try {
            MailboxAggregation m = (MailboxAggregation) super.clone();
            m.queue = new ConcurrentLinkedQueue<>();
            int size = tables.length;
            m.tables = new HistogramEntry[size];
            for (int i = 0; i < size; ++i) {
                m.tables[i] = tables[i].create();
            }
            return m;
        } catch (CloneNotSupportedException cne) {
            throw new RuntimeException(cne);
        }
    }

    public interface HistogramProcessor {
        KeyHistograms.KeyComparator<?> getKeyComparator();
        boolean processTable(MailboxAggregation m);
        Object selectFromValue(Object value);
        Object extractKeyFromValue(Object value, Object position);
    }


    public static class HistogramEntry {
        protected HistogramProcessor processor;
        protected KeyHistograms.HistogramTree tree;

        public HistogramEntry(HistogramProcessor p, int treeLimit) {
            this.processor = p;
            tree = new KeyHistograms.HistogramTree(p.getKeyComparator(), treeLimit);
        }

        public HistogramEntry create() {
            return new HistogramEntry(processor, tree.getTreeLimit());
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

    public void initMessageTable(List<HistogramProcessor> processors) {
        int size = processors.size();
        tables = new HistogramEntry[size];
        for (int i = 0; i < size; ++i) {
            tables[i] = new HistogramEntry(processors.get(i), getInitTreeSize(i));
        }
    }

    protected int getInitTreeSize(int i) {
        return treeSize;
    }

    public boolean processTable() {
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

    public int getTableSize() {
        return tables.length;
    }

    /** @return implementation field getter */
    public List<HistogramEntry> getTableEntries() {
        return Arrays.asList(tables);
    }

    public int prune(long greaterThanLeafSize, double lessThanNonZeroLeafRate) {
        int pruneCount = 0;
        for (HistogramEntry e : tables) {
            KeyHistograms.HistogramTree t = e.getTree();
            if (t.getLeafSize() > greaterThanLeafSize && t.getLeafSizeNonZeroRate() < lessThanNonZeroLeafRate) {
                t.prune();
                pruneCount++;
            }
        }
        return pruneCount;
    }

    public HistogramSelection selectTable(Object value) {
        int i = 0;
        for (HistogramEntry e : tables) {
            Object position = e.getProcessor().selectFromValue(value);
            if (position != null) {
                return new HistogramSelection(i, position);
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
}
