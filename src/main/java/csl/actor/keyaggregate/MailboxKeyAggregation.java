package csl.actor.keyaggregate;

import csl.actor.*;
import csl.actor.cluster.MailboxPersistable;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MailboxKeyAggregation implements Mailbox, Cloneable {
    protected MailboxDefault mailbox;
    protected int treeSize;
    protected HistogramEntry[] entries;
    protected KeyHistograms treeFactory;
    protected volatile boolean remainingProcessesLock = false;

    protected int threshold;
    //private volatile int size;
    private AtomicInteger size = new AtomicInteger();

    public MailboxKeyAggregation() {
        this(1000, 32);
    }

    public MailboxKeyAggregation(int threshold, int treeSize) {
        this(threshold, treeSize, new MailboxDefault());
    }

    public MailboxKeyAggregation(int threshold, int treeSize, MailboxDefault mailbox) {
        this(threshold, treeSize, mailbox, new KeyHistograms());
    }

    public MailboxKeyAggregation(int threshold, int treeSize, MailboxDefault mailbox,
                                 KeyHistograms treeFactory) {
        this.threshold = threshold;
        this.treeSize = treeSize;
        this.mailbox = mailbox;
        this.treeFactory = treeFactory;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

    public MailboxStatus getStatus(float thresholdFactor) {
        int s = size();
        int t = threshold;
        if (s > t) {
            if (hasSufficientPoints()) {
                return MailboxStatus.Exceeded;
            } else {
                return MailboxStatus.Unready;
            }
        } else if (s < t * thresholdFactor) {
            return MailboxStatus.Few;
        } else {
            return MailboxStatus.Reasonable;
        }
    }

    public enum MailboxStatus {
        Exceeded {
            @Override
            public boolean isExcessive() {
                return true;
            }
        },
        Reasonable,
        Few {
            @Override
            public boolean isExcessive() {
                return true;
            }
        },
        Unready;

        public boolean isExcessive() {
            return false;
        }
    }


    public int getThreshold() {
        return threshold;
    }


    @Override
    public MailboxKeyAggregation create() {
        try {
            MailboxKeyAggregation m = (MailboxKeyAggregation) super.clone();
            m.remainingProcessesLock = false;
            m.mailbox = mailbox.create();
            int size = entries.length;
            m.entries = new HistogramEntry[size];
            for (int i = 0; i < size; ++i) {
                m.entries[i] = entries[i].create();
            }
            //r.size = 0;
            m.size = new AtomicInteger();
            return m;
        } catch (CloneNotSupportedException cne) {
            throw new RuntimeException(cne);
        }
    }


    public int size() {
        return size.get();
    }

    @Override
    public void offer(Message<?> message) {
        /*
        ++size; //queue.size() is slow. the volatile field is used here. it is sufficient just for checking over the threshold
        if (size < 0) { //overflow
            size = Integer.MAX_VALUE;
        }*/
        if (size.incrementAndGet() < 0) {
            size.set(Integer.MAX_VALUE);
        }
        mailbox.offer(message);
    }

    @Override
    public Message<?> poll() {
        if (remainingProcessesLock) {
            return null;
        } else {
            Message<?> m = mailbox.poll();
            if (m != null) {
                //size = Math.max(size - 1, 0);
                if (size.decrementAndGet() < 0) {
                    size.set(0);
                }
            }
            return m;
        }
    }

    @Override
    public boolean isEmpty() {
        return mailbox.isEmpty();
    }

    public boolean hasRemainingProcesses() {
        return !isEmpty() && Arrays.stream(entries)
                .anyMatch(HistogramEntry::hasRemainingProcesses);
    }

    public void lockRemainingProcesses() {
        if (!remainingProcessesLock) {
            remainingProcessesLock = true;
            for (HistogramEntry e : entries) {
                e.lockRemainingProcess();
            }
        }
    }
    public void unlockRemainingProcesses(Actor self) {
        if (remainingProcessesLock) {
            for (HistogramEntry e : entries) {
                e.unlockRemainingProcess(self);
            }
            remainingProcessesLock = false;
        }
        self.getSystem().send(new Message.MessageNone(self));
    }

    public void terminateAfterSerialized() {
        mailbox.getQueue().clear();
        for (HistogramEntry e : entries) {
            e.terminateRemainingProcess();
        }
    }


    public interface HistogramProcessor extends ActorBehavior {
        KeyHistograms.KeyComparator<?> getKeyComparator();
        boolean processHistogram(MailboxKeyAggregation m);
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

        public boolean processHistogram(MailboxKeyAggregation m) {
            if (remainingProcessesLock) {
                return false;
            } else {
                return processor.processHistogram(m);
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
            if (self instanceof ActorKeyAggregation) {
                time = ((ActorKeyAggregation) self).traverseDelayTimeMs();
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

    public interface ReducedSize {
        int nextReducedSize(long size);
    }

    public static class TraversalProcess {
        protected int entryId;

        public TraversalProcess(int entryId) {
            this.entryId = entryId;
        }
    }

    public void initMessageEntries(List<HistogramProcessor> processors) {
        int size = processors.size();
        entries = new HistogramEntry[size];
        for (int i = 0; i < size; ++i) {
            HistogramProcessor p = processors.get(i);
            entries[i] = new HistogramEntry(i, p,
                    treeFactory.create(p.getKeyComparator(), getInitTreeSize(i)));
        }
    }

    protected int getInitTreeSize(int i) {
        return treeSize;
    }

    public boolean processHistogram() {
        for (HistogramEntry e : entries) {
            if (e.processHistogram(this)) {
                return true;
            }
        }
        return false;
    }

    public KeyHistograms.HistogramTree getHistogram(int entryId) {
        return entries[entryId].getTree();
    }

    public int getEntrySize() {
        return entries.length;
    }

    /** @return implementation field getter */
    public List<HistogramEntry> getEntries() {
        return Arrays.asList(entries);
    }

    public int prune(long greaterThanLeafSize, double lessThanNonZeroLeafRate) {
        int pruneCount = 0;
        for (HistogramEntry e : entries) {
            KeyHistograms.HistogramTree t = e.getTree();
            if (t.getLeafSize() > greaterThanLeafSize && t.getLeafSizeNonZeroRate() < lessThanNonZeroLeafRate) {
                t.prune();
                pruneCount++;
            }
        }
        return pruneCount;
    }

    public HistogramSelection selectHistogram(Object value) {
        int i = 0;
        for (HistogramEntry e : entries) {
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

        @Override
        public String toString() {
            return "(entry=" + entryId + ",pos=" + position + ")";
        }
    }

    public void updateScheduledTraversalProcess(Actor self, int entryId) {
        entries[entryId].updateScheduledTraversalProcess(self);
    }

    public void processTraversal(Actor self, int entryId, ReducedSize reducedSize) {
        entries[entryId].processTraversal(self, reducedSize);
    }

    public void processPhase(Actor self, Object phaseKey, ReducedSize reducedSize) {
        for (HistogramEntry e : entries) {
            e.processPhase(self, phaseKey, reducedSize);
        }
    }


    public boolean hasSufficientPoints() {
        for (HistogramEntry h : entries) {
            if (h.getTree().hasSufficientPoints()) {
                return true;
            }
        }
        return false;
    }

    public List<Object> splitMessageHistogramIntoReplicas(MailboxKeyAggregation m1, MailboxKeyAggregation m2) {
        int size = entries.length;
        List<Object> splitPoints = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            KeyHistograms.HistogramTree rt = entries[i].getTree();
            KeyHistograms.HistogramTree lt = rt.split();
            m1.entries[i].setTree(lt);
            m2.entries[i].setTree(treeFactory.init(rt));
            entries[i] = entries[i].create();
            splitPoints.add(rt.splitPointAsRightHandSide(lt));
        }
        return splitPoints;
    }

    public Object extractKey(HistogramSelection selection, Message<?> message) {
        if (selection == null) {
            return null;
        } else {
            HistogramProcessor p = entries[selection.entryId].getProcessor();
            return p.extractKeyFromValue(message.getData(), selection.position);
        }
    }

    @SuppressWarnings("unchecked")
    public boolean compare(int entryId, Object key, Object point) {
        HistogramProcessor p = entries[entryId].getProcessor();
        return ((KeyHistograms.KeyComparator<Object>) p.getKeyComparator()).compare(key, point) < 0;
    }

    public void merge(MailboxKeyAggregation m) {
        /*size += m.size;
        if (size < 0) { //overflow
            size = Integer.MAX_VALUE;
        }*/
        if (size.addAndGet(m.size()) < 0) {
            size.set(Integer.MAX_VALUE);
        }
        ConcurrentLinkedQueue<Message<?>> q = m.mailbox.getQueue();
        mailbox.getQueue().addAll(q); //it does not change target of each message
        q.clear(); //it suppose that the q is no longer offered

        for (int i = 0, l = entries.length; i < l; ++i) {
            HistogramEntry e1 = entries[i];
            HistogramEntry e2 = m.entries[i];
            e1.getTree().merge(e2.getTree());
        }
    }

    public void serializeTo(ActorKeyAggregation.ActorKeyAggregationSerializable state) {
        state.messages = mailbox.getQueue().toArray(new Message[0]);
        state.histograms = Arrays.stream(entries)
                .map(HistogramEntry::getTree)
                .collect(Collectors.toList());
    }

    public void deserializeFrom(ActorKeyAggregation.ActorKeyAggregationSerializable state) {
        mailbox.getQueue().addAll(Arrays.asList(state.messages));
        int i = 0;
        for (KeyHistograms.HistogramTree t : state.histograms) {
            entries[i].setTree(treeFactory.init(t));
            ++i;
        }
    }

}
