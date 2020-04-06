package csl.actor.kelp;

import csl.actor.ActorRef;
import csl.actor.Message;
import csl.actor.cluster.ActorPlacement;
import csl.actor.cluster.PhaseShift;
import csl.actor.kelp.KelpRoutingSplit.SplitOrMergeContextDefault;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("rawtypes")
public class KelpStateRouter implements ActorKelp.State {
    protected volatile KelpRoutingSplit split;
    protected Random random = new Random();
    protected int height = 0;
    protected int maxHeight = -1;

    protected volatile boolean parallelRouting1;
    protected volatile boolean parallelRouting2;
    protected volatile boolean needClearHistory;
    protected volatile boolean logAfterParallelRouting;

    protected Map<Object, KelpPhaseEntry> phase = new ConcurrentHashMap<>();
    protected Set<ActorRef> canceled = Collections.synchronizedSet(new HashSet<>());

    protected AtomicLong processCount = new AtomicLong();

    public void split(ActorKelp self, int height) {
        this.height = height;
        SplitOrMergeContextDefault ctx = new SplitOrMergeContextDefault("split: height=" + height, self);
        if (split == null) { //root
            if (height > 0) {
                split = self.internalCreateSplitLeaf(ctx, null, self, new KelpRoutingSplit.SplitPath(), height);
            }
        } else {
            split = split.split(ctx, height);
            mergeSingleRoot(ctx, self);
        }
        self.afterSplitOrMerge(ctx);
    }

    public void mergeInactive(ActorKelp self) {
        SplitOrMergeContextDefault ctx = new SplitOrMergeContextDefault("mergeInactive", self);
        if (split != null) {
            split = split.mergeInactive(ctx);
            mergeSingleRoot(ctx, self);
        }
        split.clearHistory();
        self.afterSplitOrMerge(ctx);
    }

    public void splitOrMerge(ActorKelp self, int height) {
        SplitOrMergeContextDefault ctx = new SplitOrMergeContextDefault("splitOrMerge: height=" + height, self);
        this.height = height;
        if (split == null) {
            if (height > 0) {
                split = self.internalCreateSplitLeaf(ctx, null, self, new KelpRoutingSplit.SplitPath(), height);
            }
        } else {
            split = split.splitOrMerge(ctx, height);
            mergeSingleRoot(ctx, self);
        }
        self.afterSplitOrMerge(ctx);
    }

    @SuppressWarnings("unchecked")
    protected void mergeSingleRoot(SplitOrMergeContextDefault context, ActorKelp self) {
        if (this.height == 0 && split instanceof KelpRoutingSplit.RoutingSplitLeaf) { //merge single root to the router
            ActorRef root = ((KelpRoutingSplit.RoutingSplitLeaf) split).getActor();
            if (root instanceof ActorKelp) { //local actor
                ActorKelp singleRoot = (ActorKelp) root;
                if (!self.hasRemainingProcesses() && !singleRoot.hasRemainingProcesses()) {
                    self.internalMerge(singleRoot);
                    context.merged(null, split, null);
                    split = null;
                }
            }
        }
    }

    @Override
    public void processMessage(ActorKelp self, Message<?> message) {
        MailboxKelp.MailboxStatus status;
        if (isNonParallelRouting() &&
                (status = self.getMailboxAsKelp().getStatus(self.lowerBoundThresholdFactor())).isExcessive()) {

            int maxHeight = getMaxHeight(self);
            MailboxKelp m = self.getMailboxAsKelp();
            int size = m.size();

            if (self.routerAutoSplit() && status.equals(MailboxKelp.MailboxStatus.Exceeded)) {
                int h = nextHeight(maxHeight, size, self.minSizeOfEachMailboxSplit());
                splitAndParallelRouting(self, m, message, h);
            } else if (self.routerAutoMerge() && split != null && status.equals(MailboxKelp.MailboxStatus.Few) &&
                    split.isHistoryExceeded((self.historyExceededLimit()))) {
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
            route(self, self.getMailboxAsKelp(), message, false);
        }
    }

    public int getMaxHeight() {
        return maxHeight;
    }

    public int getMaxHeight(ActorKelp self) {
        if (maxHeight < 0) {
            maxHeight = initMaxHeight(self);
        }
        return maxHeight;
    }

    protected int initMaxHeight(ActorKelp self) {
        int th = getTotalThreads(self, self.getPlacement());
        return Math.max(1, (int) (Math.log(th) / Math.log(2)));
    }

    protected int getTotalThreads(ActorKelp self, ActorPlacement placement) {
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

    protected void splitAndParallelRouting(ActorKelp self, MailboxKelp m, Message<?> message,
                                           int height) {
        if (m.hasSufficientPoints()) {
            split(self, height);
        }
        route(self, m, message, false);
        if (height > 0) {
            startParallelRouting(self);
        }
    }

    public void startParallelRouting(ActorKelp self) {
        int max = Math.min(self.getMailboxAsKelp().size(), self.maxParallelRouting());

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

    protected void mergeInactive(ActorKelp self, MailboxKelp m, Message<?> message) {
        mergeInactive(self);
        route(self, m, message, false);
    }

    @SuppressWarnings("unchecked")
    protected void routeRemaining(ActorKelp self, int max) {
        MailboxKelp m = self.getMailboxAsKelp();
        int i = 0;
        List<Message<?>> noRoutingTops = new ArrayList<>(16);
        while (!m.isEmpty() && i < max) {
            Message<?> msg = self.internalPollForParallelRouting();
            if (msg != null) {
                if (self.isNoRoutingMessage(msg)) {
                    self.processMessageDelayWhileParallelRouting(msg);
                    if (noRoutingTops.contains(msg)) {
                        break;
                    } else if (noRoutingTops.size() < 16) {
                        noRoutingTops.add(msg);
                    }
                } else {
                    route(self, m, msg, true);
                }
            }
            ++i;
        }
    }

    @SuppressWarnings("unchecked")
    protected void route(ActorKelp self, MailboxKelp m, Message<?> message, boolean fromParallelRouting) {
        if (split == null) {
            if (fromParallelRouting) {
                self.getSystem().send(message);
            } else {
                processCount.incrementAndGet();
                self.processMessageBehavior(message);
            }
        } else {
            MailboxKelp.HistogramSelection selection = m.selectHistogram(message.getData());
            Object key = m.extractKey(selection, message);
            split.process(self, this, key, selection, message, KelpRoutingSplit.ProcessGuide.InitToLeft);
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
    public KelpRoutingSplit getSplit() {
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
    public boolean processMessagePhase(ActorKelp self, Message<?> message) {
        Object val = message.getData();
        if (val instanceof PhaseShift) {
            processMessagePhaseShift(self, message, (PhaseShift) val);
            return true;
        } else if (val instanceof PhaseShift.PhaseShiftIntermediate) {
            processMessagePhaseShiftIntermediate(self, message, (PhaseShift.PhaseShiftIntermediate) val);
            return true;
        } else if (val instanceof ActorKelp.CancelChange) {
            processMessageCanceledChange(self, message, (ActorKelp.CancelChange) val);
            return true;
        } else {
            return false;
        }
    }

    protected void processMessagePhaseShift(ActorKelp self, Message<?> message, PhaseShift ps) {
        self.logPhase("#phase        start: %s : %s : target=%s", ps.getKey(), self, ps.getTarget());
        KelpPhaseEntry e = phase.computeIfAbsent(ps.getKey(), KelpPhaseEntry::new);
        e.setOriginAndSender(ps, message.getSender());
        e.startRouter(self); //router only delivers to canceled actors without traversal
    }

    protected void processMessagePhaseShiftIntermediate(ActorKelp self, Message<?> message, PhaseShift.PhaseShiftIntermediate ps) {
        KelpPhaseEntry finish = phase.computeIfAbsent(ps.getKey(), KelpPhaseEntry::new);
        if (ps.getActor() == self && ps.getType().equals(PhaseShift.PhaseShiftIntermediateType.PhaseIntermediateFinishLeaf)) {
            self.processPhaseEnd(ps.getKey());
        }
        if (finish.processIntermediate(self, ps)) {
            processMessagePhaseCleanUp(self);
        }
    }

    protected void processMessagePhaseCleanUp(ActorKelp self) {
        int size = phase.size();
        if (size > 30) {
            List<Object> keys = phase.values().stream()
                    .filter(e -> e.getCompletedTime() != null)
                    .sorted(Comparator.comparing(KelpPhaseEntry::getCompletedTime))
                    .map(KelpPhaseEntry::getKey)
                    .collect(Collectors.toList());
            IntStream.range(0, Math.max(size - 30, 0))
                    .mapToObj(keys::get)
                    .forEach(phase::remove);
        }
    }

    protected void processMessageCanceledChange(ActorKelp self, Message<?> message, ActorKelp.CancelChange changed) {
        Object data = changed.getData();
        ActorRef canceledActor = changed.getCanceledActor();
        if (data.equals(ActorKelp.CanceledChangeType.CancelAdded)) {
            canceled.add(canceledActor);
            canceledActor.tell(new ActorKelp.CancelChange(canceledActor, ActorKelp.CanceledChangeType.CancelFinished), self);
        } else if (data.equals(ActorKelp.CanceledChangeType.CancelFinished)) {
            canceled.remove(canceledActor);
        }
    }

    public Map<Object, KelpPhaseEntry> getPhase() {
        return new HashMap<>(phase);
    }

    public Set<ActorRef> getCanceled() {
        return new HashSet<>(canceled);
    }
}
