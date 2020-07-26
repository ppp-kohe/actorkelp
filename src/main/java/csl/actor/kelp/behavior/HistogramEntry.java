package csl.actor.kelp.behavior;

import csl.actor.Actor;
import csl.actor.CallableMessage;
import csl.actor.kelp.ActorKelp;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class HistogramEntry {
    protected int entryId;
    protected HistogramProcessor processor;
    protected KeyHistograms.HistogramTree tree;
    protected volatile ScheduledFuture<?> scheduledProcess;
    protected Instant nextSchedule = Instant.now();
    protected Instant scheduleBackup = Instant.now();
    protected Instant lastTraversal = Instant.now();
    protected volatile boolean remainingProcessesLock;

    public HistogramEntry(int entryId, HistogramProcessor p, KeyHistograms.HistogramTree tree) {
        this.entryId = entryId;
        this.processor = p;
        this.tree = tree;
    }

    public HistogramEntry create() {
        return new HistogramEntry(entryId, processor, tree.createTree(null));
    }

    public int getEntryId() {
        return entryId;
    }

    public Instant getNextSchedule() {
        return nextSchedule;
    }

    public Instant getLastTraversal() {
        return lastTraversal;
    }

    public Instant getScheduleBackup() {
        return scheduleBackup;
    }

    public KeyHistograms.HistogramTree getTree() {
        return tree;
    }

    public void setTree(KeyHistograms.HistogramTree tree) {
        this.tree = tree;
    }

    public boolean processHistogram(Actor self, MailboxKelp m) {
        if (remainingProcessesLock) {
            return false;
        } else {
            return processor.processHistogram(self, m);
        }
    }

    public void processTraversal(Actor self, MailboxKelp.ReducedSize reducedSize) {
        if (processor.needToProcessTraversal(self, tree)) {
            lastTraversal = Instant.now();
            processTraversal(self, reducedSize, tree.getRoot());
        }
    }

    public void processTraversal(Actor self, MailboxKelp.ReducedSize reducedSize, KeyHistograms.HistogramNode node) {
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

    public long traversalDelayTimeMs(Actor self) {
        if (self instanceof ActorKelp) {
            return ((ActorKelp<?>) self).getTraverseDelayTimeMs();
        } else {
            return 300;
        }
    }

    public synchronized void updateScheduledTraversalProcess(Actor self) {
        long time = traversalDelayTimeMs(self);
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

    public static class TraversalProcess implements CallableMessage.CallableMessageConsumer<ActorKelp<?>> {
        public static final long serialVersionUID = 1L;
        protected int entryId;

        public TraversalProcess() {}

        public TraversalProcess(int entryId) {
            this.entryId = entryId;
        }

        @Override
        public void accept(ActorKelp<?> self) {
            self.getMailboxAsKelp()
                    .processTraversal(self, entryId, self.getReducedSize());
        }
    }

    public synchronized void startTraversalProcess(Actor self) {
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

    public void processStageEnd(Actor self, Object stageKey, MailboxKelp.ReducedSize reducedSize) {
        if (processor.needToProcessStageEnd(self, stageKey, tree)) {
            synchronized (this) {
                processStageEnd(self, stageKey, reducedSize, tree.getRoot());
            }
        }
    }

    public void processStageEnd(Actor self, Object stageKey, MailboxKelp.ReducedSize reducedSize, KeyHistograms.HistogramNode node) {
        if (node != null && node.size() > 0) {
            if (node instanceof KeyHistograms.HistogramNodeTree) {
                for (KeyHistograms.HistogramNode ch : ((KeyHistograms.HistogramNodeTree) node).getChildren()) {
                    processStageEnd(self, stageKey, reducedSize, ch);
                }
            } else if (node instanceof KeyHistograms.HistogramNodeLeaf) {
                processor.processStageEnd(self, stageKey, reducedSize, (KeyHistograms.HistogramNodeLeaf) node);
            }
        }
    }

    public void processPersistableTraversalBeforePut(Actor self) {
        MailboxKelp.ReducedSize rs;
        if (self instanceof ActorKelp) {
            rs = ((ActorKelp<?>) self).getReducedSize();
        } else {
            rs = new MailboxKelp.ReducedSizeDefault();
        }
        processPersistableTraversalBeforePut(self, rs);
    }

    public void processPersistableTraversalBeforePut(Actor self, MailboxKelp.ReducedSize reducedSize) {
        if (!remainingProcessesLock &&
                Duration.ofMillis(traversalDelayTimeMs(self)).compareTo(Duration.between(Instant.now(), lastTraversal)) < 0 &&
                (reducedSize.needToReduce(tree.getTreeSizeForReduceCheck()) ||
                 tree.needToReduce())) {
            processTraversal(self, reducedSize);
        }
    }
}
