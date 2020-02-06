package csl.actor.msgassoc;

import csl.actor.Actor;
import csl.actor.MailboxDefault;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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

        default void processTraversal(Actor self, KeyHistograms.HistogramNodeLeaf leaf) {}
    }


    public static class HistogramEntry {
        protected int entryId;
        protected HistogramProcessor processor;
        protected KeyHistograms.HistogramTree tree;
        protected volatile ScheduledFuture<?> scheduledProcess;
        protected Instant nextSchedule = Instant.now();

        public HistogramEntry(int entryId, HistogramProcessor p, int treeLimit) {
            this.entryId = entryId;
            this.processor = p;
            tree = new KeyHistograms.HistogramTree(p.getKeyComparator(), treeLimit);
        }

        public HistogramEntry create() {
            return new HistogramEntry(entryId, processor, tree.getTreeLimit());
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

        public synchronized void updateScheduledProcess(Actor self) {
            ScheduledFuture<?> p = scheduledProcess;
            nextSchedule = Instant.now().plus(1, ChronoUnit.SECONDS); //TODO custom delay
            if (p == null || p.isCancelled() || p.isDone()) {
                scheduledProcess = self.getSystem().getScheduledExecutor()
                        .schedule(() -> processScheduled(self), 1, TimeUnit.SECONDS); //TODO custom delay
            }
        }

        public void processScheduled(Actor self) {
            synchronized (this) {
                Duration remaining = Duration.between(Instant.now(), nextSchedule);
                if (remaining.isNegative()) {
                    //do the job
                    scheduledProcess = null;
                    self.tell(new TraversalProcess(entryId), null);
                } else {
                    scheduledProcess.cancel(false);
                    scheduledProcess = self.getSystem().getScheduledExecutor()
                            .schedule(() -> processScheduled(self), remaining.toMillis() + 1L, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    public static class TraversalProcess {
        protected int entryId;

        public TraversalProcess(int entryId) {
            this.entryId = entryId;
        }
    }

    public void initMessageTable(List<HistogramProcessor> processors) {
        int size = processors.size();
        tables = new HistogramEntry[size];
        for (int i = 0; i < size; ++i) {
            tables[i] = new HistogramEntry(i, processors.get(i), getInitTreeSize(i));
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

    public void processTraversal(Actor self, int entryId) {
        processTraversal(self, entryId, getTable(entryId).getRoot());
    }

    protected void processTraversal(Actor self, int entryId, KeyHistograms.HistogramNode node) {
        if (node.size() > 0) {
            if (node instanceof KeyHistograms.HistogramNodeTree) {
                ((KeyHistograms.HistogramNodeTree) node).getChildren()
                        .forEach(c -> processTraversal(self, entryId, c));
            } else if (node instanceof KeyHistograms.HistogramNodeLeaf) {
                tables[entryId].getProcessor().processTraversal(self, (KeyHistograms.HistogramNodeLeaf) node);
            }
        }
    }
}
