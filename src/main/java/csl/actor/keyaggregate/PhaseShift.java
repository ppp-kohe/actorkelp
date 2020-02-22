package csl.actor.keyaggregate;

import csl.actor.*;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class PhaseShift implements CallableMessage.CallableMessageConsumer<Actor>, MessageNoRouting {
    protected Instant startTime;
    protected Object key;
    protected ActorRef target;
    protected int count;

    public static CompletableFuture<PhaseCompleted> start(ActorSystem system, ActorRef target) {
        return start(UUID.randomUUID(), system, target);
    }

    public static CompletableFuture<PhaseCompleted> start(Object key, ActorSystem system, ActorRef target) {
        return new PhaseTerminalActor(system, false).start(key, target);
    }

    public PhaseShift(Object key, ActorRef target) {
        this.key = key;
        this.target = target;
        startTime = Instant.now();
    }

    public PhaseShift(Object key) {
        this(key, null);
    }

    public Object getKey() {
        return key;
    }

    public ActorRef getTarget() {
        return target;
    }

    @Override
    public void accept(Actor actor, ActorRef sender) {
        if (!actor.isEmptyMailbox()) {
            count = 0;
            retry(actor, sender);
        } else if (count < 3) {
            retry(actor, sender);
        } else {
            completed(actor, sender);
        }
    }

    public int getCount() {
        return count;
    }

    public void retry(Actor actor, ActorRef sender) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException ie) {
        }
        ++count;
        actor.tell(this, sender);
    }

    public PhaseCompleted createCompleted(Actor actor) {
        return new PhaseCompleted(key, actor, this);
    }

    public void completed(Actor router, ActorRef sender) {
        count = 0;
        PhaseCompleted c = createCompleted(router);
        log(router, "#phase    completed: " + key + " "  + c.getElapsedTime() + " : " + router);
        router.tell(c, router);
    }

    public void log(Actor router, String str) {
        if (router instanceof ActorKeyAggregation) {
            ((ActorKeyAggregation) router).logPhase(str);
        } else {
            Config c = Config.CONFIG_DEFAULT;
            c.log(c.logColorPhase, str);
        }
    }

    public Instant getStartTime() {
        return startTime;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this)) +
                "(key=" + key + ", target=" + target + ", count=" + count + ", start=" + startTime + ")";
    }

    public enum PhaseShiftIntermediateType {
        PhaseIntermediateRouterStart,
        PhaseIntermediateFinishCanceled,
        PhaseIntermediateFinishLeaf
    }

    public interface StageSupported {
        ActorRef nextStage();
    }

    public static class PhaseShiftIntermediate implements CallableMessageConsumer<Actor>, MessageNoRouting {
        protected Object key;
        protected ActorRef actor;
        protected PhaseShiftIntermediateType type;

        protected int count;

        public PhaseShiftIntermediate(Object key, ActorRef actor, PhaseShiftIntermediateType type) {
            this.key = key;
            this.actor = actor;
            this.type = type;
        }

        public Object getKey() {
            return key;
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" + type +
                    ", key=" + key +
                    ", actor=" + actor +
                    '}';
        }

        public PhaseShiftIntermediateType getType() {
            return type;
        }

        @Override
        public void accept(Actor self, ActorRef sender) {
            accept(self, sender, sender);
        }

        public void accept(Actor self, ActorRef router, ActorRef sender) {
            if (!self.isEmptyMailbox() || count < 3) {
                retry(self, sender);
            } else {
                router.tell(this, self);
                count = 0;
            }
        }

        protected void retry(Actor actor, ActorRef sender) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
            }
            ++count;
            actor.tell(this, sender);
        }
    }

    public static class PhaseCompleted implements Serializable, MessageNoRouting, CallableMessageConsumer<Actor> {
        protected Object key;
        protected ActorRef actor;
        protected PhaseShift origin;
        protected Instant time;

        public PhaseCompleted(Object key, ActorRef actor, PhaseShift origin) {
            this.key = key;
            this.actor = actor;
            this.origin = origin;
            this.time = Instant.now();
        }

        public Object getKey() {
            return key;
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "key=" + key +
                    ", actor=" + actor +
                    '}';
        }

        @Override
        public void accept(Actor self, ActorRef sender) {
            if (self instanceof StageSupported) {
                ActorRef next = ((StageSupported) self).nextStage();
                if (next != null) {
                    redirectTo(next);
                } else {
                    sendToTarget();
                }
            } else {
                sendToTarget();
            }
        }

        public void redirectTo(ActorRef nextRouter) {
            log("#phase   redirectTo: " + key + " " + getElapsedTime() + " : " + actor + " -> " + nextRouter);
            nextRouter.tell(origin);
        }


        public void log(String str) {
            if (actor instanceof ActorKeyAggregation) {
                ((ActorKeyAggregation) actor).logPhase(str);
            } else {
                Config c = Config.CONFIG_DEFAULT;
                c.log(c.logColorPhase, str);
            }
        }

        public void sendToTarget() {
            log("#phase sendToTarget: " + key + " " + getElapsedTime() + " : " + actor + " -> " + origin.getTarget());
            if (origin.getTarget() != null) {
                origin.getTarget().tell(this);
            }
        }

        public PhaseShift getOrigin() {
            return origin;
        }

        public Instant getTime() {
            return time;
        }

        public Duration getElapsedTime() {
            return Duration.between(origin.getStartTime(), time);
        }
    }

    public static class PhaseTerminalActor extends ActorDefault {
        protected boolean closeSystem;
        protected BiConsumer<ActorSystem, PhaseCompleted> handler;

        protected Map<Object, PhaseTerminalEntry> completed = new ConcurrentHashMap<>();

        public PhaseTerminalActor(ActorSystem system, boolean closeSystem, BiConsumer<ActorSystem, PhaseCompleted> handler) {
            super(system);
            this.closeSystem = closeSystem;
            this.handler = handler;
            this.name = getClass().getName() + "#" + UUID.randomUUID().toString();
            system.register(this);
        }

        public PhaseTerminalActor(ActorSystem system, boolean closeSystem) {
            this(system, closeSystem, null);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(PhaseCompleted.class, this::completed)
                    .build();
        }

        public void completed(PhaseCompleted c) {
            PhaseTerminalEntry e = completed.computeIfAbsent(c.getKey(), PhaseTerminalEntry::new);
            int n = e.getNextCount();
            c.log("#phase       finish: " + c.getKey() + " " + c.getElapsedTime() + " : " + c.getTime() + " count=" + n);
            e.complete(c);
            if (handler != null) {
                handler.accept(getSystem(), c);
            }
            if (closeSystem) {
                getSystem().close();
            }
        }

        public CompletableFuture<PhaseCompleted> start(Object key, ActorRef initialTarget) {
            CompletableFuture<PhaseCompleted> c = completed.computeIfAbsent(key, PhaseTerminalEntry::new)
                    .future();
            initialTarget.tell(new PhaseShift(key, this), this);
            return c;
        }

        public Map<Object, PhaseTerminalEntry> getCompleted() {
            return new HashMap<>(completed);
        }

        public int getCompletedCount(Object key) {
            PhaseTerminalEntry e = completed.get(key);
            if (e == null) {
                return 0;
            } else {
                return e.getCount();
            }
        }

        @Override
        public String toStringContents() {
            return "";
        }
    }

    public static class PhaseTerminalEntry {
        protected Object key;
        protected int count;
        protected CompletableFuture<PhaseCompleted> future;

        public PhaseTerminalEntry(Object key) {
            this.key = key;
        }

        public Object getKey() {
            return key;
        }

        public synchronized int getNextCount() {
            ++count;
            return count;
        }

        public synchronized int getCount() {
            return count;
        }

        public synchronized CompletableFuture<PhaseCompleted> future() {
            if (future == null) {
                future = new CompletableFuture<>();
            }
            return future;
        }

        public synchronized void complete(PhaseCompleted c) {
            if (future != null) {
                future.complete(c);
                future = null;
            }
        }
    }

    public static class PhaseEntry {
        protected Object key;
        protected PhaseShift origin;
        protected ActorRef sender;
        protected Map<ActorRef, Boolean> finished = new HashMap<>();

        public PhaseEntry(Object key) {
            this.key = key;
        }

        public Object getKey() {
            return key;
        }

        public void setOriginAndSender(PhaseShift origin, ActorRef sender) {
            this.origin = origin;
            this.sender = sender;
        }

        public void startRouter(ActorKeyAggregation router) {
            router.tell(new PhaseShiftIntermediate(key, router,
                    PhaseShiftIntermediateType.PhaseIntermediateRouterStart), router);
        }

        public boolean processIntermediate(ActorKeyAggregation self, PhaseShift.PhaseShiftIntermediate ps) {
            if (ps.getType().equals(PhaseShiftIntermediateType.PhaseIntermediateRouterStart)) {
                if (self.getMailboxAsKeyAggregation().hasRemainingProcesses()) {
                    self.tell(ps);
                    return false;
                } else {
                    Collection<ActorRef> canceled = ((KeyAggregationStateRouter) self.getState()).getCanceled();
                    if (startCancel(self, canceled)) { //delivers to canceled actors: if true, empty canceled, go to next step
                        return startRouterSplits(self);
                    } else {
                        return false;
                    }
                }
            } else if (ps.getType().equals(PhaseShiftIntermediateType.PhaseIntermediateFinishCanceled)) {
                if (completedCancel(self, ps.getActor())) {
                    startRouter(self); //restart
                }
                return false;
            } else if (ps.getType().equals(PhaseShiftIntermediateType.PhaseIntermediateFinishLeaf)) {
                return completed(self, ps.getActor());
            } else { //error
                self.logPhase("??? " + self + " : " + ps);
                return false;
            }
        }

        public boolean startCancel(Actor router, Collection<ActorRef> canceled) {
            boolean complete = true;
            for (ActorRef a : canceled) {
                if (incompleteCancel(router, a)) {
                    complete = false;
                }
            }
            return complete;
        }

        public boolean incompleteCancel(Actor router, ActorRef a) {
            if (!finished.computeIfAbsent(a, _k -> false)) {
                a.tell(new PhaseShiftIntermediate(key, a,
                        PhaseShiftIntermediateType.PhaseIntermediateFinishCanceled), router);
                return true;
            } else {
                return false;
            }
        }

        public boolean startRouterSplits(ActorKeyAggregation router) {
            new VisitorIncompleteLeaf(this).accept(router, null); //no delayed message for router
            if (completed(router, "SPLITS")) {
                origin.completed(router, sender);
                return true;
            } else {
                return false;
            }
        }

        protected static class VisitorIncompleteLeaf implements ActorKeyAggregationVisitor.VisitorNoSender<Actor> {
            protected PhaseEntry entry;

            public VisitorIncompleteLeaf(PhaseEntry entry) {
                this.entry = entry;
            }

            @Override
            public void visitActor(Actor actor) {
                entry.incompleteLeaf(actor, actor);
            }

            @Override
            public void visitRouterLeaf(Actor actor, ActorRef sender, KeyAggregationRoutingSplit.RoutingSplitLeaf leaf) {
                entry.incompleteLeaf(actor, leaf.getActor());
            }
        }

        public void incompleteLeaf(Actor router, ActorRef a) {
            finished.put(a, false);
            a.tell(new PhaseShiftIntermediate(key, a, PhaseShiftIntermediateType.PhaseIntermediateFinishLeaf), router);
        }

        public boolean completedCancel(ActorKeyAggregation router, ActorRef canceled) {
            finished.put(canceled, true);
            return completed(router, String.format("%s : CANCEL", canceled));
        }

        public boolean completed(ActorKeyAggregation router, ActorRef a) {
            finished.put(a, true);
            Set<ActorRef> currentSplits = collect(router);
            currentSplits.removeAll(finished.keySet());
            if (!currentSplits.isEmpty()) { //new splits
                currentSplits.forEach(s -> incompleteLeaf(router, s));
                return false;
            }
            if (completed(router, String.format("%s : SPLITS", a))) {
                origin.completed(router, sender);
                return true;
            } else {
                return false;
            }
        }

        public boolean completed(ActorKeyAggregation router, String msg) {
            int ok = (int) finished.values().stream()
                    .filter(b -> b)
                    .count();
            int all = finished.size();

            float okp = (float) ok / (float) all * 100f;
            router.logPhase("#phase intermediate: " + origin.getKey() + " : " + msg + String.format(" %3.1f%%", okp));
            return ok >= all;
        }


        public Set<ActorRef> collect(ActorKeyAggregation router) {
            Set<ActorRef> result = new HashSet<>();
            result.add(router);
            KeyAggregationStateRouter stateRouter = (KeyAggregationStateRouter) router.getState();
            result.addAll(stateRouter.getCanceled());
            collect(router, result, stateRouter.getSplit());
            return result;

        }

        public void collect(ActorKeyAggregation router, Set<ActorRef> result, KeyAggregationRoutingSplit split) {
            if (split instanceof KeyAggregationRoutingSplit.RoutingSplitLeaf) {
                result.add(((KeyAggregationRoutingSplit.RoutingSplitLeaf) split).getActor());
            } else if (split instanceof KeyAggregationRoutingSplit.RoutingSplitNode) {
                KeyAggregationRoutingSplit.RoutingSplitNode n = (KeyAggregationRoutingSplit.RoutingSplitNode) split;
                collect(router, result, n.getLeft());
                collect(router, result, n.getRight());
            }
        }
    }
}
