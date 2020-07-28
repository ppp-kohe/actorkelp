package csl.actor.kelp.behavior;

import csl.actor.Actor;
import csl.actor.Mailbox;
import csl.actor.MailboxDefault;
import csl.actor.Message;
import csl.actor.kelp.ActorKelpFunctions.KeyComparator;
import csl.actor.kelp.ActorKelpSerializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public class MailboxKelp implements Mailbox, Cloneable {
    protected Mailbox mailbox;
    protected int treeSize;
    protected HistogramEntry[] entries;
    protected KeyHistograms treeFactory;

    public MailboxKelp() {
        this(32);
    }

    public MailboxKelp(int treeSize) {
        this(treeSize, new MailboxDefault());
    }

    public MailboxKelp(int treeSize, Mailbox mailbox) {
        this(treeSize, mailbox, new KeyHistograms());
    }

    public MailboxKelp(int treeSize, Mailbox mailbox,
                       KeyHistograms treeFactory) {
        this.treeSize = treeSize;
        this.mailbox = mailbox;
        this.treeFactory = treeFactory;
    }

    public Mailbox getMailbox() {
        return mailbox;
    }

    public KeyHistograms getTreeFactory() {
        return treeFactory;
    }


    @Override
    public MailboxKelp create() {
        try {
            MailboxKelp m = (MailboxKelp) super.clone();
            m.mailbox = mailbox.create();
            int size = entries.length;
            m.entries = new HistogramEntry[size];
            for (int i = 0; i < size; ++i) {
                m.entries[i] = entries[i].create();
            }
            return m;
        } catch (CloneNotSupportedException cne) {
            throw new RuntimeException(cne);
        }
    }

    @Override
    public void offer(Message<?> message) {
        mailbox.offer(message);
    }

    @Override
    public Message<?> poll() {
        return mailbox.poll();
    }

    @Override
    public boolean isEmpty() {
        return mailbox.isEmpty();
    }

    @Override
    public Queue<Message<?>> getQueue() {
        return mailbox.getQueue();
    }

    public void terminateAfterSerialized() {
        mailbox.getQueue().clear();
        for (HistogramEntry e : entries) {
            e.terminateRemainingProcess();
        }
    }


    public interface ReducedSize {
        boolean needToReduce(long size);
        int nextReducedSize(long size);
    }

    public static class ReducedSizeDefault implements ReducedSize {
        protected int reduceRuntimeCheckingThreshold;
        protected double reduceRuntimeRemainingBytesToSizeRatio;

        public ReducedSizeDefault() {
            this(100_000, 0.003);
        }

        public ReducedSizeDefault(int reduceRuntimeCheckingThreshold, double reduceRuntimeRemainingBytesToSizeRatio) {
            this.reduceRuntimeCheckingThreshold = reduceRuntimeCheckingThreshold;
            this.reduceRuntimeRemainingBytesToSizeRatio = reduceRuntimeRemainingBytesToSizeRatio;
        }

        @Override
        public int nextReducedSize(long size) {
            int consuming = (int) Math.min(Integer.MAX_VALUE, size);
            int rrt = reduceRuntimeCheckingThreshold;
            if (consuming > rrt) { //refer free memory size
                long aom = availableOnMemoryMessages();
                consuming = (int) Math.min(consuming, Math.max(rrt, aom));

                logReducedSize(size, aom, consuming);
            }
            return consuming;
        }

        protected void logReducedSize(long size, long availableOnMemoryMessages, int consuming) { }

        public long availableOnMemoryMessages() {
            Runtime rt = Runtime.getRuntime();
            return (long) ((rt.maxMemory() - rt.totalMemory()) * reduceRuntimeRemainingBytesToSizeRatio);
        }

        @Override
        public boolean needToReduce(long size) {
            int rrt = reduceRuntimeCheckingThreshold;
            long aom = -1;
            boolean r = size > rrt &&
                    size > (aom = availableOnMemoryMessages());
            if (r) {
                logNeedToReduce(size, aom);
            }
            return r;
        }

        protected void logNeedToReduce(long size, long availableOnMemoryMessages) { }
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

    public boolean processHistogram(Actor self) {
        for (HistogramEntry e : entries) {
            if (e.processHistogram(self, this)) {
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

    public void processPersistableTraversalBeforePut(Actor self, int entryId) {
        entries[entryId].processPersistableTraversalBeforePut(self);
    }

    public void processStageEnd(Actor self, Object stageKey, ReducedSize reducedSize) {
        for (HistogramEntry e : entries) {
            e.processStageEnd(self, stageKey, reducedSize);
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

    public List<Object> splitMessageHistogramIntoReplicas(MailboxKelp m1, MailboxKelp m2) {
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
    public int compare(int entryId, Object key, Object point) {
        HistogramProcessor p = entries[entryId].getProcessor();
        return ((KeyComparator<Object>) p.getKeyComparator()).compare(key, point);
    }

    public void merge(MailboxKelp m) {
        Queue<Message<?>> q = m.mailbox.getQueue();
        mailbox.getQueue().addAll(q); //it does not change target of each message
        q.clear(); //it suppose that the q is no longer offered

        for (int i = 0, l = entries.length; i < l; ++i) {
            HistogramEntry e1 = entries[i];
            HistogramEntry e2 = m.entries[i];
            e1.getTree().merge(e2.getTree());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void serializeTo(ActorKelpSerializable state) {
        state.setMessages(mailbox.getQueue().toArray(new Message[0]));
        state.setHistograms(Arrays.stream(entries)
                .map(HistogramEntry::getTree)
                .collect(Collectors.toList()));
    }

    public void deserializeFrom(ActorKelpSerializable<?> state) {
        mailbox.getQueue().addAll(Arrays.asList(state.messages));
        int i = 0;
        for (KeyHistograms.HistogramTree t : state.histograms) {
            entries[i].setTree(treeFactory.init(t));
            ++i;
        }
    }

}
