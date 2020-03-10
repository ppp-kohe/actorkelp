package csl.actor.keyaggregate;

import csl.actor.ActorRef;
import csl.actor.Message;
import csl.actor.cluster.ActorPlacement;
import csl.actor.cluster.PhaseShift;
import csl.actor.keyaggregate.KeyAggregationRoutingSplit.SplitOrMergeContextDefault;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class KeyAggregationStateRouter implements ActorKeyAggregation.State {
    protected volatile KeyAggregationRoutingSplit split;
    protected Random random = new Random();
    protected int height = 0;
    protected int maxHeight = -1;

    protected volatile boolean parallelRouting1;
    protected volatile boolean parallelRouting2;
    protected volatile boolean needClearHistory;
    protected volatile boolean logAfterParallelRouting;

    protected Map<Object, KeyAggregationPhaseEntry> phase = new HashMap<>();
    protected Set<ActorRef> canceled = new HashSet<>();

    protected AtomicLong processCount = new AtomicLong();

    public void split(ActorKeyAggregation self, int height) {
        this.height = height;
        SplitOrMergeContextDefault ctx = new SplitOrMergeContextDefault("split: height=" + height, self);
        if (split == null) { //root
            if (height > 0) {
                split = self.internalCreateSplitLeaf(ctx, null, self, new KeyAggregationRoutingSplit.SplitPath(), height);
            }
        } else {
            split = split.split(ctx, height);
            mergeSingleRoot(ctx, self);
        }
        self.afterSplitOrMerge(ctx);
    }

    public void mergeInactive(ActorKeyAggregation self) {
        SplitOrMergeContextDefault ctx = new SplitOrMergeContextDefault("mergeInactive", self);
        if (split != null) {
            split = split.mergeInactive(ctx);
            mergeSingleRoot(ctx, self);
        }
        split.clearHistory();
        self.afterSplitOrMerge(ctx);
    }

    public void splitOrMerge(ActorKeyAggregation self, int height) {
        SplitOrMergeContextDefault ctx = new SplitOrMergeContextDefault("splitOrMerge: height=" + height, self);
        this.height = height;
        if (split == null) {
            if (height > 0) {
                split = self.internalCreateSplitLeaf(ctx, null, self, new KeyAggregationRoutingSplit.SplitPath(), height);
            }
        } else {
            split = split.splitOrMerge(ctx, height);
            mergeSingleRoot(ctx, self);
        }
        self.afterSplitOrMerge(ctx);
    }

    protected void mergeSingleRoot(SplitOrMergeContextDefault context, ActorKeyAggregation self) {
        if (this.height == 0 && split instanceof KeyAggregationRoutingSplit.RoutingSplitLeaf) { //merge single root to the router
            ActorRef root = ((KeyAggregationRoutingSplit.RoutingSplitLeaf) split).getActor();
            if (root instanceof ActorKeyAggregation) { //local actor
                ActorKeyAggregation singleRoot = (ActorKeyAggregation) root;
                if (!self.hasRemainingProcesses() && !singleRoot.hasRemainingProcesses()) {
                    self.internalMerge(singleRoot);
                    context.merged(null, split, null);
                    split = null;
                }
            }
        }
    }

    @Override
    public void processMessage(ActorKeyAggregation self, Message<?> message) {
        MailboxKeyAggregation.MailboxStatus status;
        if (isNonParallelRouting() &&
                (status = self.getMailboxAsKeyAggregation().getStatus(self.lowerBoundThresholdFactor())).isExcessive()) {

            int maxHeight = getMaxHeight(self);
            MailboxKeyAggregation m = self.getMailboxAsKeyAggregation();
            int size = m.size();

            if (self.routerAutoSplit() && status.equals(MailboxKeyAggregation.MailboxStatus.Exceeded)) {
                int h = nextHeight(maxHeight, size, self.minSizeOfEachMailboxSplit());
                splitAndParallelRouting(self, m, message, h);
            } else if (self.routerAutoMerge() && split != null && status.equals(MailboxKeyAggregation.MailboxStatus.Few) &&
                    split.isHistoryExceeded((int) (self.historyExceededLimit()))) {
                mergeInactive(self, m, message);
            } else {
                route(self, m, message, false);
            }
        } else {
            if (needClearHistory) {
                needClearHistory = false;
                if (split != null && isNonParallelRouting()) {
                    split.clearHistory();
                }
            }
            if (logAfterParallelRouting && isNonParallelRouting()) {
                logAfterParallelRouting = false;
                if (self.logSplit()) {
                    self.printStatus("after parallelRouting");
                }
            }
            route(self, self.getMailboxAsKeyAggregation(), message, false);
        }
    }

    public int getMaxHeight() {
        return maxHeight;
    }

    public int getMaxHeight(ActorKeyAggregation self) {
        if (maxHeight < 0) {
            maxHeight = initMaxHeight(self);
        }
        return maxHeight;
    }

    protected int initMaxHeight(ActorKeyAggregation self) {
        int th = getTotalThreads(self, self.getPlacement());
        return Math.max(1, (int) (Math.log(th) / Math.log(2)));
    }

    protected int getTotalThreads(ActorKeyAggregation self, ActorPlacement placement) {
        if (placement instanceof ActorPlacement.ActorPlacementDefault) {
            return ((ActorPlacement.ActorPlacementDefault) placement).getTotalThreads();
        } else {
            return self.getSystem().getThreads();
        }
    }

    public boolean isNonParallelRouting() {
        return !(parallelRouting1 || parallelRouting2);
    }

    public int nextHeight(int maxHeight, int size, int eachSizeOfSplits) {
        int h = 1;
        while ((size >>> h) > eachSizeOfSplits && h < maxHeight) {
            ++h;
        }
        return Math.min(Math.max(height, h), maxHeight);
    }

    protected void splitAndParallelRouting(ActorKeyAggregation self, MailboxKeyAggregation m, Message<?> message,
                                           int height) {
        split(self, height);
        route(self, m, message, false);
        startParallelRouting(self);
    }

    public void startParallelRouting(ActorKeyAggregation self) {
        int max = Math.min(self.getMailboxAsKeyAggregation().size(), self.maxParallelRouting());

        needClearHistory = true;
        logAfterParallelRouting = self.logSplit();
        if (split != null) {
            parallelRouting1 = true;
            parallelRouting2 = true;
            self.getSystem().execute(() -> {
                try {
                    routeRemaining(self, max);
                } finally {
                    parallelRouting1 = false;
                }
            });
            self.getSystem().execute(() -> {
                try {
                    routeRemaining(self, max);
                } finally {
                    parallelRouting2 = false;
                }
            });
        }
    }

    protected void mergeInactive(ActorKeyAggregation self, MailboxKeyAggregation m, Message<?> message) {
        mergeInactive(self);
        route(self, m, message, false);
    }

    protected void routeRemaining(ActorKeyAggregation self, int max) {
        MailboxKeyAggregation m = self.getMailboxAsKeyAggregation();
        int i = 0;
        List<Message<?>> noRoutingTops = new ArrayList<>(16);
        while (!m.isEmpty() && i < max) {
            Message<?> msg = self.internalPollForParallelRouting();
            if (msg != null) {
                if (self.isNoRoutingMessage(msg)) {
                    if (noRoutingTops.contains(msg)) {
                        break;
                    } else if (noRoutingTops.size() < 16) {
                        noRoutingTops.add(msg);
                    }
                    self.processMessageDelayWhileParallelRouting(msg);
                } else {
                    route(self, m, msg, true);
                }
            }
            ++i;
        }
    }

    protected void route(ActorKeyAggregation self, MailboxKeyAggregation m, Message<?> message, boolean fromParallelRouting) {
        if (split == null) {
            if (fromParallelRouting) {
                self.getSystem().send(message);
            } else {
                processCount.incrementAndGet();
                self.processMessageBehavior(message);
            }
        } else {
            MailboxKeyAggregation.HistogramSelection selection = m.selectHistogram(message.getData());
            Object key = m.extractKey(selection, message);
            split.process(self, this, key, selection, message);
        }
    }

    public Random getRandom() {
        return random;
    }

    @Override
    public long getProcessCount() {
        return processCount.get();
    }

    /** @return implementation field getter */
    public KeyAggregationRoutingSplit getSplit() {
        return split;
    }

    /** @return implementation field getter */
    public int getHeight() {
        return height;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
                "split=" + split + ", height=" + height
                + ", parallelRouting1=" + this.parallelRouting1
                + ", parallelRouting2=" + this.parallelRouting2
                + ", needClearHistory=" + this.needClearHistory +
                ')';
    }

    @Override
    public boolean processMessagePhase(ActorKeyAggregation self, Message<?> message) {
        Object val = message.getData();
        if (val instanceof PhaseShift) {
            processMessagePhaseShift(self, message, (PhaseShift) val);
            return true;
        } else if (val instanceof PhaseShift.PhaseShiftIntermediate) {
            processMessagePhaseShiftIntermediate(self, message, (PhaseShift.PhaseShiftIntermediate) val);
            return true;
        } else if (val instanceof ActorKeyAggregation.CancelChange) {
            processMessageCanceledChange(self, message, (ActorKeyAggregation.CancelChange) val);
            return true;
        } else {
            return false;
        }
    }

    protected void processMessagePhaseShift(ActorKeyAggregation self, Message<?> message, PhaseShift ps) {
        self.logPhase("#phase        start: %s : %s : target=%s", ps.getKey(), self, ps.getTarget());
        KeyAggregationPhaseEntry e = phase.computeIfAbsent(ps.getKey(), KeyAggregationPhaseEntry::new);
        e.setOriginAndSender(ps, message.getSender());
        e.startRouter(self); //router only delivers to canceled actors without traversal
    }

    protected void processMessagePhaseShiftIntermediate(ActorKeyAggregation self, Message<?> message, PhaseShift.PhaseShiftIntermediate ps) {
        KeyAggregationPhaseEntry finish = phase.computeIfAbsent(ps.getKey(), KeyAggregationPhaseEntry::new);
        if (ps.getActor() == self && ps.getType().equals(PhaseShift.PhaseShiftIntermediateType.PhaseIntermediateFinishLeaf)) {
            self.processPhaseEnd(ps.getKey());
        }
        if (finish.processIntermediate(self, ps)) {
            phase.remove(ps.getKey());
        }
    }

    protected void processMessageCanceledChange(ActorKeyAggregation self, Message<?> message, ActorKeyAggregation.CancelChange changed) {
        Object data = changed.getData();
        ActorRef canceledActor = changed.getCanceledActor();
        if (data.equals(ActorKeyAggregation.CanceledChangeType.CancelAdded)) {
            canceled.add(canceledActor);
            canceledActor.tell(new ActorKeyAggregation.CancelChange(canceledActor, ActorKeyAggregation.CanceledChangeType.CancelFinished), self);
        } else if (data.equals(ActorKeyAggregation.CanceledChangeType.CancelFinished)) {
            canceled.remove(canceledActor);
        }
    }

    public Set<ActorRef> getCanceled() {
        return canceled;
    }
}
