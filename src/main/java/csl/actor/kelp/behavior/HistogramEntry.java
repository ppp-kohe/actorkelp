package csl.actor.kelp.behavior;

import csl.actor.Actor;
import csl.actor.CallableMessage;
import csl.actor.MessageBundle;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.persist.PersistentConditionActor;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HistogramEntry {
    protected int entryId;
    protected HistogramProcessor processor;
    protected HistogramTree tree;
    protected volatile ScheduledFuture<?> scheduledProcess;
    protected Instant nextSchedule = Instant.now();
    protected Instant scheduleBackup = Instant.now();
    protected volatile boolean remainingProcessesLock;
    protected AtomicBoolean traversalReserved = new AtomicBoolean();

    public HistogramEntry(int entryId, HistogramProcessor p, HistogramTree tree) {
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

    public Instant getScheduleBackup() {
        return scheduleBackup;
    }

    public HistogramTree getTree() {
        return tree;
    }

    public void setTree(HistogramTree tree) {
        this.tree = tree;
    }

    public boolean processHistogram(Actor self, MailboxKelp m) {
        if (remainingProcessesLock) {
            return false;
        } else {
            return processor.processHistogram(self, m);
        }
    }

    public void reserveTraversal(Actor self) {
        if (self != null && traversalReserved.compareAndSet(false, true)) {
            self.tellMessage(new MessageBundle.MessageAccepted<>(self, new TraversalProcess(entryId).withSender(null)));
        }
    }

    public void processTraversalReserved(Actor self, MailboxKelp.ReducedSize reducedSize) {
        if (traversalReserved.compareAndSet(true, false)) {
            processTraversal(self, reducedSize, true);
        }
    }

    public void processTraversal(Actor self, MailboxKelp.ReducedSize reducedSize, boolean reserved) {
        if (!remainingProcessesLock && processor.needToProcessTraversal(self, tree, reducedSize, reserved)) {
            processTraversal(self, reducedSize, tree, tree.getRoot());
        }
    }

    public void processTraversal(Actor self, MailboxKelp.ReducedSize reducedSize, HistogramTree tree, KeyHistograms.HistogramTreeNode node) {
        if (remainingProcessesLock) {
            updateScheduledTraversalProcess(self);
        } else if (node != null && node.size() > 0) {
            if (node instanceof HistogramTreeNodeTable) {
                for (KeyHistograms.HistogramTreeNode ch : ((HistogramTreeNodeTable) node).getChildren(tree)) { //TODO loading all persisted sub-nodes
                    processTraversal(self, reducedSize, tree, ch);
                }
            } else if (node instanceof HistogramTreeNodeLeaf) {
                if (remainingProcessesLock) {
                    updateScheduledTraversalProcess(self);
                } else {
                    processor.processTraversal(self, reducedSize, (HistogramTreeNodeLeaf) node);
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
                    .schedule(new StartTraversalProcess(this, self), time, TimeUnit.MILLISECONDS);
        }
    }

    public static class TraversalProcess implements CallableMessage.CallableMessageConsumer<ActorKelp<?>> {
        public static final long serialVersionUID = 1L;
        public int entryId;

        public TraversalProcess() {}

        public TraversalProcess(int entryId) {
            this.entryId = entryId;
        }

        @Override
        public void accept(ActorKelp<?> self) {
            //the following process will be done by ActorKelp.processMessage(msg) //the class is just a trigger
//            self.getMailboxAsKelp()
//                    .processTraversal(self, entryId, self.getReducedSize());
        }
    }

    public synchronized void startTraversalProcess(StartTraversalProcess p) {
        Duration remaining = Duration.between(Instant.now(), nextSchedule);
        if (remaining.isNegative()) {
            //do the job
            scheduledProcess = null;
            reserveTraversal(p.getSelf());
        } else {
            long nextDelay;
            if (remainingProcessesLock) {
                nextDelay = Long.MAX_VALUE;
            } else {
                nextDelay = remaining.toMillis() + 1L;
            }
            scheduledProcess.cancel(false);
            scheduledProcess = p.getSelf().getSystem().getScheduledExecutor()
                    .schedule(p, nextDelay, TimeUnit.MILLISECONDS);
        }
    }

    public static class StartTraversalProcess implements Runnable {
        protected HistogramEntry entry;
        protected Actor self;

        public StartTraversalProcess(HistogramEntry entry, Actor self) {
            this.entry = entry;
            this.self = self;
        }
        public void run() {
            try {
                entry.startTraversalProcess(this);
            } catch (Throwable ex) {
                ex.printStackTrace();
            }
        }

        public Actor getSelf() {
            return self;
        }
    }

    public synchronized boolean hasRemainingProcesses() {
        HistogramTree tree = this.tree;
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
        HistogramTree tree = this.tree;
        if (tree != null) {
            tree.close();
        }
    }

    public synchronized void unlockRemainingProcess(Actor self) {
        if (remainingProcessesLock) {
            nextSchedule = scheduleBackup;
            remainingProcessesLock = false;
            updateScheduledTraversalProcess(self);
        }
    }

    public void processStageEnd(Actor self, Object stageKey, MailboxKelp.ReducedSize reducedSize) {
        if (processor.needToProcessStageEnd(self, reducedSize, stageKey, tree)) {
            synchronized (this) {
                processStageEnd(self, stageKey, reducedSize, tree, tree.getRoot());
            }
        }
    }

    public void processStageEnd(Actor self, Object stageKey, MailboxKelp.ReducedSize reducedSize, HistogramTree tree, KeyHistograms.HistogramTreeNode node) {
        if (node != null && node.size() > 0) {
            if (node instanceof HistogramTreeNodeTable) {
                for (KeyHistograms.HistogramTreeNode ch : ((HistogramTreeNodeTable) node).getChildren(tree)) {
                    processStageEnd(self, stageKey, reducedSize, tree, ch);
                }
            } else if (node instanceof HistogramTreeNodeLeaf) {
                processor.processStageEnd(self, stageKey, reducedSize, (HistogramTreeNodeLeaf) node);
            }
        }
    }

    public void processPersistableTraversalBeforePut(Actor self, MailboxKelp.ReducedSize reducedSize) {
        processTraversal(self, reducedSize, false);
    }
}
