package csl.actor.msgassoc;

import csl.actor.Message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MailboxAggregationReplicable extends MailboxAggregation {
    protected int threshold;
    //private volatile int size;
    private AtomicInteger size = new AtomicInteger();

    public MailboxAggregationReplicable() {
        this(1000, 32);
    }

    public MailboxAggregationReplicable(int threshold, int treeSize) {
        super(treeSize);
        this.threshold = threshold;
        this.treeSize = treeSize;
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
    public MailboxAggregationReplicable create() {
        MailboxAggregationReplicable r = (MailboxAggregationReplicable) super.create();
        //r.size = 0;
        r.size = new AtomicInteger();
        return r;
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
        super.offer(message);
    }

    @Override
    public Message<?> poll() {
        Message<?> m = super.poll();
        if (m != null) {
            //size = Math.max(size - 1, 0);
            if (size.decrementAndGet() < 0) {
                size.set(0);
            }
        }
        return m;
    }

    public boolean hasSufficientPoints() {
        for (HistogramEntry h : tables) {
            if (h.getTree().hasSufficientPoints()) {
                return true;
            }
        }
        return false;
    }

    public List<Object> splitMessageTableIntoReplicas(MailboxAggregationReplicable m1, MailboxAggregationReplicable m2) {
        int size = tables.length;
        List<Object> splitPoints = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            KeyHistograms.HistogramTree rt = tables[i].getTree();
            KeyHistograms.HistogramTree lt = rt.split();
            m1.tables[i].setTree(lt);
            m2.tables[i].setTree(rt);
            tables[i] = tables[i].create();
            splitPoints.add(rt.splitPointAsRightHandSide(lt));
        }
        return splitPoints;
    }

    public Object extractKey(HistogramSelection selection, Message<?> message) {
        HistogramProcessor p = tables[selection.entryId].getProcessor();
        return p.extractKeyFromValue(message.getData(), selection.position);
    }

    @SuppressWarnings("unchecked")
    public boolean compare(int entryId, Object key, Object point) {
        HistogramProcessor p = tables[entryId].getProcessor();
        return ((KeyHistograms.KeyComparator<Object>) p.getKeyComparator()).compare(key, point) < 0;
    }

    public void merge(MailboxAggregationReplicable m) {
        /*size += m.size;
        if (size < 0) { //overflow
            size = Integer.MAX_VALUE;
        }*/
        if (size.addAndGet(m.size()) < 0) {
            size.set(Integer.MAX_VALUE);
        }
        mailbox.getQueue().addAll(m.mailbox.getQueue()); //it does not change target of each message

        for (int i = 0, l = tables.length; i < l; ++i) {
            HistogramEntry e1 = tables[i];
            HistogramEntry e2 = m.tables[i];
            e1.getTree().merge(e2.getTree());
        }
    }

    public void serializeTo(ActorAggregationReplicable.ActorReplicableSerializableState state) {
        state.messages = mailbox.getQueue().toArray(new Message[0]);
        state.tables = Arrays.stream(tables)
                .map(HistogramEntry::getTree)
                .collect(Collectors.toList());
    }

    public void deserializeFrom(ActorAggregationReplicable.ActorReplicableSerializableState state) {
        mailbox.getQueue().addAll(Arrays.asList(state.messages));
        int i = 0;
        for (KeyHistograms.HistogramTree t : state.tables) {
            tables[i].setTree(t);
            ++i;
        }
    }

}
