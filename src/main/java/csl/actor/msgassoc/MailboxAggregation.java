package csl.actor.msgassoc;

import csl.actor.Actor;
import csl.actor.Mailbox;
import csl.actor.MailboxDefault;
import csl.actor.Message;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MailboxAggregation implements Mailbox, Cloneable {
    protected MailboxDefault mailbox;
    protected int treeSize;
    protected HistogramEntry[] tables;
    protected KeyHistograms treeFactory;
    protected volatile boolean remainingProcessesLock = false;

    public MailboxAggregation() {
        this(32);
    }

    public MailboxAggregation(int treeSize) {
        this(treeSize, new MailboxDefault());
    }

    public MailboxAggregation(int treeSize, MailboxDefault mailbox) {
        this(treeSize, mailbox, new KeyHistograms());
    }

    public MailboxAggregation(int treeSize, MailboxDefault mailbox, KeyHistograms treeFactory) {
        this.mailbox = mailbox;
        this.treeSize = treeSize;
        this.treeFactory = treeFactory;
    }

    @Override
    public void offer(Message<?> message) {
        mailbox.offer(message);
    }

    @Override
    public Message<?> poll() {
        if (remainingProcessesLock) {
            return null;
        } else {
            return mailbox.poll();
        }
    }

    @Override
    public boolean isEmpty() {
        return mailbox.isEmpty();
    }

    public boolean hasRemainingProcesses() {
        return !isEmpty() && Arrays.stream(tables)
                .anyMatch(HistogramEntry::hasRemainingProcesses);
    }

    public void lockRemainingProcesses() {
        if (!remainingProcessesLock) {
            remainingProcessesLock = true;
            for (HistogramEntry e : tables) {
                e.lockRemainingProcess();
            }
        }
    }

    public void unlockRemainingProcesses(Actor self) {
        if (remainingProcessesLock) {
            for (HistogramEntry e : tables) {
                e.unlockRemainingProcess(self);
            }
            remainingProcessesLock = false;
        }
        self.getSystem().send(new Message.MessageNone(self));
    }

    public void terminateAfterSerialized() {
        mailbox.getQueue().clear();
        for (HistogramEntry e : tables) {
            e.terminateRemainingProcess();
        }
    }

    @Override
    public MailboxAggregation create() {
        try {
            MailboxAggregation m = (MailboxAggregation) super.clone();
            m.remainingProcessesLock = false;
            m.mailbox = mailbox.create();
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

        default boolean needToProcessTraversal(Actor self, KeyHistograms.HistogramTree tree) {
            return false;
        }

        default boolean needToProcessPhase(Actor self, Object phaseKey, KeyHistograms.HistogramTree tree) {
            return false;
        }

        default void processTraversal(Actor self, ReducedSize reducedSize, KeyHistograms.HistogramNodeLeaf leaf) {}
        default void processPhase(Actor self, Object phaseKey, ReducedSize reducedSize, KeyHistograms.HistogramNodeLeaf leaf) {}
    }

    public interface ReducedSize {
        int nextReducedSize(long size);
    }

    public static class HistogramEntry {
        protected int entryId;
        protected HistogramProcessor processor;
        protected KeyHistograms.HistogramTree tree;
        protected volatile ScheduledFuture<?> scheduledProcess;
        protected Instant nextSchedule = Instant.now();
        protected Instant scheduleBackup = Instant.now();
        protected volatile boolean remainingProcessesLock;

        public HistogramEntry(int entryId, HistogramProcessor p, KeyHistograms.HistogramTree tree) {
            this.entryId = entryId;
            this.processor = p;
            this.tree = tree;
        }

        public HistogramEntry create() {
            return new HistogramEntry(entryId, processor, tree.createTree(null));
        }

        public KeyHistograms.HistogramTree getTree() {
            return tree;
        }

        public void setTree(KeyHistograms.HistogramTree tree) {
            this.tree = tree;
        }

        public boolean processTable(MailboxAggregation m) {
            if (remainingProcessesLock) {
                return false;
            } else {
                return processor.processTable(m);
            }
        }

        public void processTraversal(Actor self, ReducedSize reducedSize) {
            if (processor.needToProcessTraversal(self, tree)) {
                processTraversal(self, reducedSize, tree.getRoot());
            }
        }

        public void processTraversal(Actor self, ReducedSize reducedSize, KeyHistograms.HistogramNode node) {
            if (remainingProcessesLock) {
                updateScheduledTraversalProcess(self);
            } else if (node != null && node.size() > 0) {
                if (node instanceof KeyHistograms.HistogramNodeTree) {
                    for (KeyHistograms.HistogramNode ch : ((KeyHistograms.HistogramNodeTree) node).getChildren()) {
                        processTraversal(self, reducedSize, ch);
                    }
                } else if (node instanceof KeyHistograms.HistogramNodeLeaf) {
                    if (remainingProcessesLock) {
                        updateScheduledTraversalProcess(self);
                    } else {
                        processor.processTraversal(self, reducedSize, (KeyHistograms.HistogramNodeLeaf) node);
                    }
                }
            }
        }

        public HistogramProcessor getProcessor() {
            return processor;
        }

        public synchronized void updateScheduledTraversalProcess(Actor self) {
            long time = 300;
            if (self instanceof ActorAggregation) {
                time = ((ActorAggregation) self).traverseDelayTimeMs();
            }
            ScheduledFuture<?> p = scheduledProcess;
            Instant nextTime = Instant.now().plusMillis(time);
            if (remainingProcessesLock) { //forever
                scheduleBackup = nextTime;
                nextSchedule = Instant.MAX;
                time = Long.MAX_VALUE;
            } else {
                nextSchedule = nextTime;
            }
            if (p == null || p.isCancelled() || p.isDone()) {
                scheduledProcess = self.getSystem().getScheduledExecutor()
                        .schedule(() -> startTraversalProcess(self), time, TimeUnit.MILLISECONDS);
            }
        }

        public void startTraversalProcess(Actor self) {
            synchronized (this) {
                Duration remaining = Duration.between(Instant.now(), nextSchedule);
                if (remaining.isNegative()) {
                    //do the job
                    scheduledProcess = null;
                    self.tell(new TraversalProcess(entryId));
                } else {
                    long nextDelay;
                    if (remainingProcessesLock) {
                        nextDelay = Long.MAX_VALUE;
                    } else {
                        nextDelay = remaining.toMillis() + 1L;
                    }
                    scheduledProcess.cancel(false);
                    scheduledProcess = self.getSystem().getScheduledExecutor()
                            .schedule(() -> startTraversalProcess(self), nextDelay, TimeUnit.MILLISECONDS);
                }
            }
        }

        public synchronized boolean hasRemainingProcesses() {
            KeyHistograms.HistogramTree tree = this.tree;
            return scheduledProcess != null || (tree != null && !tree.getCompleted().isEmpty());
        }

        public synchronized void lockRemainingProcess() {
            remainingProcessesLock = true;
            if (scheduledProcess != null) {
                if (!nextSchedule.equals(Instant.MAX)) {
                    scheduleBackup = nextSchedule;
                    nextSchedule = Instant.MAX; //temporary defer forever!
                }
            }
        }

        public synchronized void terminateRemainingProcess() {
            ScheduledFuture<?> p = scheduledProcess;
            if (p != null && !p.isDone() && !p.isCancelled()) {
                p.cancel(false);
            }
            scheduledProcess = null;
        }

        public synchronized void unlockRemainingProcess(Actor self) {
            if (remainingProcessesLock) {
                nextSchedule = scheduleBackup;
                remainingProcessesLock = false;
                updateScheduledTraversalProcess(self);
            }
        }

        public void processPhase(Actor self, Object phaseKey, ReducedSize reducedSize) {
            if (processor.needToProcessPhase(self, phaseKey, tree)) {
                synchronized (this) {
                    processPhase(self, phaseKey, reducedSize, tree.getRoot());
                }
            }
        }

        public void processPhase(Actor self, Object phaseKey, ReducedSize reducedSize, KeyHistograms.HistogramNode node) {
            if (node != null && node.size() > 0) {
                if (node instanceof KeyHistograms.HistogramNodeTree) {
                    for (KeyHistograms.HistogramNode ch : ((KeyHistograms.HistogramNodeTree) node).getChildren()) {
                        processPhase(self, phaseKey, reducedSize, ch);
                    }
                } else if (node instanceof KeyHistograms.HistogramNodeLeaf) {
                    processor.processPhase(self, phaseKey, reducedSize, (KeyHistograms.HistogramNodeLeaf) node);
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
            HistogramProcessor p = processors.get(i);
            tables[i] = new HistogramEntry(i, p,
                    treeFactory.create(p.getKeyComparator(), getInitTreeSize(i)));
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

    public void updateScheduledTraversalProcess(Actor self, int entryId) {
        tables[entryId].updateScheduledTraversalProcess(self);
    }

    public void processTraversal(Actor self, int entryId, ReducedSize reducedSize) {
        tables[entryId].processTraversal(self, reducedSize);
    }

    public void processPhase(Actor self, Object phaseKey, ReducedSize reducedSize) {
        for (HistogramEntry e : tables) {
            e.processPhase(self, phaseKey, reducedSize);
        }
    }
}
