package csl.actor.keyaggregate;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.cluster.PhaseShift;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KeyAggregationPhaseEntry {
    protected Object key;
    protected PhaseShift origin;
    protected ActorRef sender;
    protected Map<ActorRef, Boolean> finished = new ConcurrentHashMap<>();

    public KeyAggregationPhaseEntry(Object key) {
        this.key = key;
    }

    public Object getKey() {
        return key;
    }

    public ActorRef getSender() {
        return sender;
    }

    public PhaseShift getOrigin() {
        return origin;
    }

    public Map<ActorRef, Boolean> getFinished() {
        return finished;
    }

    public void setOriginAndSender(PhaseShift origin, ActorRef sender) {
        this.origin = origin;
        this.sender = sender;
    }

    public void startRouter(ActorKeyAggregation router) {
        router.tell(origin.createIntermediate(router,
                PhaseShift.PhaseShiftIntermediateType.PhaseIntermediateRouterStart), router);
    }

    public boolean processIntermediate(ActorKeyAggregation self, PhaseShift.PhaseShiftIntermediate ps) {
        if (ps.getType().equals(PhaseShift.PhaseShiftIntermediateType.PhaseIntermediateRouterStart)) {
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
        } else if (ps.getType().equals(PhaseShift.PhaseShiftIntermediateType.PhaseIntermediateFinishCanceled)) {
            if (completedCancel(self, ps.getActor())) {
                startRouter(self); //restart
            }
            return false;
        } else if (ps.getType().equals(PhaseShift.PhaseShiftIntermediateType.PhaseIntermediateFinishLeaf)) {
            return completed(self, ps.getActor());
        } else { //error
            self.logPhase("??? %s : %s", self, ps);
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
            a.tell(origin.createIntermediate(a,
                    PhaseShift.PhaseShiftIntermediateType.PhaseIntermediateFinishCanceled), router);
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

    public static class VisitorIncompleteLeaf implements KeyAggregationVisitor.VisitorNoSender<Actor> {
        protected KeyAggregationPhaseEntry entry; //never a remote message

        public VisitorIncompleteLeaf(KeyAggregationPhaseEntry entry) {
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
        a.tell(origin.createIntermediate(a, PhaseShift.PhaseShiftIntermediateType.PhaseIntermediateFinishLeaf), router);
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
        router.logPhase("#phase intermediate: %s : %s %3.1f%%", origin.getKey(), msg, okp);
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
