package csl.actor.kelp.behavior;

import csl.actor.*;
import csl.actor.kelp.ActorKelpFunctions.KeyComparator;
import csl.actor.kelp.ActorKelpSerializable;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.persist.MailboxManageable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MailboxKelp implements Mailbox, Cloneable {
    protected ActorSystemKelp.KelpHistory history;
    protected MailboxManageable mailbox;
    protected int treeSize;
    protected HistogramEntry[] entries;
    protected KeyHistograms treeFactory;
    protected Map<Object, ControlEntry> controlEntries = new ConcurrentHashMap<>();

    public MailboxKelp(int treeSize, MailboxManageable mailbox) {
        this(treeSize, mailbox, new KeyHistograms());
    }

    public MailboxKelp(int treeSize, MailboxManageable mailbox,
                       KeyHistograms treeFactory) {
        this.treeSize = treeSize;
        this.mailbox = mailbox;
        this.treeFactory = treeFactory;

        history = ActorSystemKelp.KelpHistory.create(ActorSystemKelp.HISTORY_SIZE_OLDEST * 2);
    }

    public MailboxManageable getMailbox() {
        return mailbox;
    }

    public KeyHistograms getTreeFactory() {
        return treeFactory;
    }

    public ActorSystemKelp.KelpHistory getHistory() {
        return history;
    }

    @Override
    public MailboxKelp create() {
        try {
            MailboxKelp m = (MailboxKelp) super.clone();
            m.mailbox = mailbox.createManageable();
            int size = entries.length;
            m.entries = new HistogramEntry[size];
            for (int i = 0; i < size; ++i) {
                m.entries[i] = entries[i].create();
            }
            m.controlEntries = new ConcurrentHashMap<>();
            return m;
        } catch (CloneNotSupportedException cne) {
            throw new RuntimeException(cne);
        }
    }

    @Override
    public void offer(Message<?> message) {
        history = history.set(mailbox.getPreviousSizeOnMemory());
        mailbox.offer(message);
    }

    public boolean isMessageControl(Message<?> message) {
        return getMessageControlDataFromMessage(message) != null;
    }

    public MessageDataControl getMessageControlDataFromMessage(Message<?> message) {
        return message != null ? getMessageControlData(message.getData()) : null;
    }
    public MessageDataControl getMessageControlData(Object data) {
        if (data instanceof MessageDataControl) {
            return (MessageDataControl) data;
        } else if (data instanceof Message.MessageDataHolder<?>) {
            return getMessageControlData(((Message.MessageDataHolder<?>) data).getData());
        } else {
            return null;
        }
    }

    public interface MessageDataControl extends Message.MessageDataSpecial {
        void control(Message<?> message, MailboxKelp mbox);
    }

    public static class ControlEntry {
        protected Object key;

        public ControlEntry(Object key) {
            this.key = key;
        }
    }

    @SuppressWarnings("unchecked")
    public <EntryType extends ControlEntry> EntryType getOrCreateControlEntry(Object key, Function<Object, EntryType> factory) {
        return (EntryType) controlEntries.computeIfAbsent(key, factory);
    }

    public ControlEntry removeControlEntry(Object key) {
        return controlEntries.remove(key);
    }

    @Override
    public Message<?> poll() {
        if (controlEntries.isEmpty()) {
            return mailbox.poll();
        } else {
            return null;
        }
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
        boolean needToReduceForComplete(boolean allowPersist, long sizeChecked, int reduceReq);

        boolean needToReduceForTraversal(boolean allowPersist, long treeSize, long treeSizeOnMemory, HistogramTree tree);
        int nextReducedSize(long size);
    }

    public void initMessageEntries(List<HistogramProcessor> processors) {
        int size = processors.size();
        entries = new HistogramEntry[size];
        for (int i = 0; i < size; ++i) {
            HistogramProcessor p = processors.get(i);
            entries[i] = new HistogramEntry(i, p,
                    treeFactory.create(p.getKeyComparator(), getInitTreeSize(i), p.keyType(), p.valueTypesForPositions()));
        }
    }

    protected int getInitTreeSize(int i) {
        return treeSize;
    }

    public HistogramTree getHistogram(int entryId) {
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
            HistogramTree t = e.getTree();
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

    public void reserveTraversal(Actor self, int entryId) {
        entries[entryId].reserveTraversal(self);
    }

    public void processTraversalReserved(Actor self, ReducedSize reducedSize, int entryId) {
        entries[entryId].processTraversalReserved(self, reducedSize);
    }


    public void processPersistableTraversalBeforePut(Actor self, int entryId, ReducedSize reducedSize) {
        entries[entryId].processPersistableTraversalBeforePut(self, reducedSize);
    }

    public void processStageEnd(Actor self, Object stageKey, ReducedSize reducedSize) {
        for (HistogramEntry e : entries) {
            e.processStageEnd(self, stageKey, reducedSize);
        }
    }

    public HistogramEntry remainingProcessesEntry() {
        for (HistogramEntry e : entries) {
            if (e.hasRemainingProcesses()) {
                return e;
            }
        }
        return null;
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
            HistogramTree rt = entries[i].getTree();
            HistogramTree lt = rt.split();
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
    public void serializeTo(Actor owner, ActorKelpSerializable state) {
        state.setMessages(mailbox.getQueue().toArray(new Message[0]));
        state.setHistograms(Arrays.stream(entries)
                .map(e -> copyTree(owner, e.getTree()))
                .collect(Collectors.toList()));
    }

    protected HistogramTree copyTree(Actor owner, HistogramTree tree) {
        return tree.copy();
    }

    public void deserializeFrom(Actor owner, ActorKelpSerializable<?> state) {
        mailbox.getQueue().addAll(Arrays.asList(state.messages));
        boolean copy = state.internalStateUsed;
        int i = 0;
        for (HistogramTree t : state.histograms) {
            if (copy) {
                t = copyTree(owner, t);
            }
            entries[i].setTree(treeFactory.init(t));
            ++i;
        }
    }

}
