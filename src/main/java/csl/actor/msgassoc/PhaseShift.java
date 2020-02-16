package csl.actor.msgassoc;

import csl.actor.*;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;

public class PhaseShift implements CallableMessage.CallableMessageConsumer<Actor>, ActorAggregationReplicable.NoRouting {
    protected Object key;
    protected ActorRef target;

    public PhaseShift(Object key, ActorRef target) {
        this.key = key;
        this.target = target;
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
        if (!actor.getMailbox().isEmpty()) {
            retry(actor, sender);
        } else {
            completed(actor, sender);
        }
    }

    protected void retry(Actor actor, ActorRef sender) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException ie) {
        }
        actor.tell(this, sender);
    }

    public PhaseShiftCompleted createCompleted(Actor actor) {
        return new PhaseShiftCompleted(key, actor, this);
    }

    public void completed(Actor router, ActorRef sender) {
        log(router, "#phase    completed: " + key + " : " + router);
        router.tell(createCompleted(router), router);
    }

    public void log(Actor router, String str) {
        if (router instanceof ActorAggregationReplicable) {
            ((ActorAggregationReplicable) router).logPhase(str);
        } else {
            Config c = Config.CONFIG_DEFAULT;
            c.log(c.logColorPhase, str);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this)) + "(key=" + key + ", target=" + target + ")";
    }

    public enum PhaseShiftCompletedIntermediateType {
        PhaseIntermediateRouterStart,
        PhaseIntermediateFinishDisabled,
        PhaseIntermediateFinishLeaf

    }

    public static class PhaseShiftIntermediate implements Serializable, ActorAggregationReplicable.NoRouting {
        protected Object key;
        protected ActorRef actor;
        protected PhaseShiftCompletedIntermediateType type;

        public PhaseShiftIntermediate(Object key, ActorRef actor, PhaseShiftCompletedIntermediateType type) {
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

        public PhaseShiftCompletedIntermediateType getType() {
            return type;
        }

        public void accept(Actor self, ActorRef router, ActorRef sender) {
            if (!self.getMailbox().isEmpty()) {
                retry(self, sender);
            } else {
                router.tell(this, self);
            }
        }

        protected void retry(Actor actor, ActorRef sender) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
            }
            actor.tell(this, sender);
        }
    }

    public static class PhaseShiftCompleted implements Serializable, ActorAggregationReplicable.NoRouting {
        protected Object key;
        protected ActorRef actor;
        protected PhaseShift origin;

        public PhaseShiftCompleted(Object key, ActorRef actor, PhaseShift origin) {
            this.key = key;
            this.actor = actor;
            this.origin = origin;
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

        public void redirectTo(ActorRef nextRouter) {
            log("#phase   redirectTo: " + key + " : " + actor + " -> " + nextRouter);
            nextRouter.tell(origin);
        }


        public void log(String str) {
            if (actor instanceof ActorAggregationReplicable) {
                ((ActorAggregationReplicable) actor).logPhase(str);
            } else {
                Config c = Config.CONFIG_DEFAULT;
                c.log(c.logColorPhase, str);
            }
        }

        public void sendToTarget() {
            log("#phase sendToTarget: " + key + " : " + actor + " -> " + origin.getTarget());
            if (origin.getTarget() != null) {
                origin.getTarget().tell(this);
            }
        }
    }

    public static class PhaseFinishActor extends ActorDefault {
        protected Instant start;
        protected boolean closeSystem;
        protected BiConsumer<ActorSystem, PhaseShiftCompleted> handler;

        public PhaseFinishActor(ActorSystem system, Instant start, boolean closeSystem, BiConsumer<ActorSystem, PhaseShiftCompleted> handler) {
            super(system);
            this.start = start;
            this.closeSystem = closeSystem;
            this.handler = handler;
            this.name = getClass().getName() + "#" + Long.toHexString(start.getEpochSecond()) + "." + Integer.toHexString(start.getNano());
            system.register(this);
        }

        public PhaseFinishActor(ActorSystem system, boolean closeSystem, BiConsumer<ActorSystem, PhaseShiftCompleted> handler) {
            this(system, Instant.now(), closeSystem, handler);
        }

        public PhaseFinishActor(ActorSystem system, boolean closeSystem) {
            this(system, Instant.now(), closeSystem, null);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(PhaseShiftCompleted.class, this::completed)
                    .build();
        }

        public void completed(PhaseShiftCompleted c) {
            c.log("#phase       finish: " + c.getKey() + " : " + Instant.now() + " : " + Duration.between(start, Instant.now()));
            if (handler != null) {
                handler.accept(getSystem(), c);
            }
            if (closeSystem) {
                getSystem().close();
            }
        }

        public void start(Object key, ActorRef initialTarget) {
            initialTarget.tell(new PhaseShift(key, this), this);
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

        public void startRouter(ActorAggregationReplicable router) {
            router.tell(new PhaseShiftIntermediate(key, router,
                    PhaseShiftCompletedIntermediateType.PhaseIntermediateRouterStart));
        }

        public boolean processIntermediate(ActorAggregationReplicable self, PhaseShift.PhaseShiftIntermediate ps) {
            if (ps.getType().equals(PhaseShiftCompletedIntermediateType.PhaseIntermediateRouterStart)) {
                if (self.getMailboxAsReplicable().hasRemainingProcesses()) {
                    self.tell(ps);
                    return false;
                } else {
                    Collection<ActorRef> disabled = ((ActorAggregationReplicable.StateSplitRouter) self.getState()).getDisabled();
                    if (startDisabled(disabled)) { //delivers to disabled actors: if true, empty disabled, go to next step
                        return startRouterSplits(self);
                    } else {
                        return false;
                    }
                }
            } else if (ps.getType().equals(PhaseShiftCompletedIntermediateType.PhaseIntermediateFinishDisabled)) {
                if (completeDisabled(self, ps.getActor())) {
                    startRouter(self); //restart
                }
                return false;
            } else if (ps.getType().equals(PhaseShiftCompletedIntermediateType.PhaseIntermediateFinishLeaf)) {
                return completed(self, ps.getActor());
            } else { //error
                self.logPhase("??? " + self + " : " + ps);
                return false;
            }
        }

        public boolean startDisabled(Collection<ActorRef> disabled) {
            boolean complete = true;
            for (ActorRef a : disabled) {
                if (incompleteDisabled(a)) {
                    complete = false;
                }
            }
            return complete;
        }

        public boolean incompleteDisabled(ActorRef a) {
            if (!finished.computeIfAbsent(a, _k -> false)) {
                a.tell(new PhaseShiftIntermediate(key, a,
                        PhaseShiftCompletedIntermediateType.PhaseIntermediateFinishDisabled));
                return true;
            } else {
                return false;
            }
        }

        public boolean startRouterSplits(ActorAggregationReplicable router) {
            new VisitorIncompleteLeaf(this).accept(router, null); //no delayed message for router
            if (completed(router, "SPLITS")) {
                origin.completed(router, sender);
                return true;
            } else {
                return false;
            }
        }

        protected static class VisitorIncompleteLeaf implements ActorVisitor.VisitorNoSender<Actor> {
            protected PhaseEntry entry;

            public VisitorIncompleteLeaf(PhaseEntry entry) {
                this.entry = entry;
            }

            @Override
            public void visitActor(Actor actor) {
                entry.incompleteLeaf(actor);
            }

            @Override
            public void visitRouterLeaf(Actor actor, ActorRef sender, ActorAggregationReplicable.SplitLeaf leaf) {
                entry.incompleteLeaf(leaf.getActor());
            }
        }

        public void incompleteLeaf(ActorRef a) {
            finished.put(a, false);
            a.tell(new PhaseShiftIntermediate(key, a, PhaseShiftCompletedIntermediateType.PhaseIntermediateFinishLeaf));
        }

        public boolean completeDisabled(ActorAggregationReplicable router, ActorRef disabled) {
            finished.put(disabled, true);
            return completed(router, String.format("%s : DISABLED", disabled));
        }

        public boolean completed(ActorAggregationReplicable router, ActorRef a) {
            finished.put(a, true);
            Set<ActorRef> currentSplits = collect(router);
            currentSplits.removeAll(finished.keySet());
            if (!currentSplits.isEmpty()) { //new splits
                currentSplits.forEach(this::incompleteLeaf);
                return false;
            }
            if (completed(router, String.format("%s : SPLITS", a))) {
                origin.completed(router, sender);
                return true;
            } else {
                return false;
            }
        }

        public boolean completed(ActorAggregationReplicable router, String msg) {
            int ok = (int) finished.values().stream()
                    .filter(b -> b)
                    .count();
            int all = finished.size();

            float okp = (float) ok / (float) all * 100f;
            router.logPhase("#phase intermediate: " + origin.getKey() + " : " + msg + String.format(" %3.1f%%", okp));
            return ok >= all;
        }


        public Set<ActorRef> collect(ActorAggregationReplicable router) {
            Set<ActorRef> result = new HashSet<>();
            result.add(router);
            ActorAggregationReplicable.StateSplitRouter stateRouter = (ActorAggregationReplicable.StateSplitRouter) router.getState();
            result.addAll(stateRouter.getDisabled());
            collect(router, result, stateRouter.getSplit());
            return result;

        }

        public void collect(ActorAggregationReplicable router, Set<ActorRef> result, ActorAggregationReplicable.Split split) {
            if (split instanceof ActorAggregationReplicable.SplitLeaf) {
                result.add(((ActorAggregationReplicable.SplitLeaf) split).getActor());
            } else if (split instanceof ActorAggregationReplicable.SplitNode) {
                ActorAggregationReplicable.SplitNode n = (ActorAggregationReplicable.SplitNode) split;
                collect(router, result, n.getLeft());
                collect(router, result, n.getRight());
            }
        }
    }
}
