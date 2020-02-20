package csl.actor.msgassoc;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.CallableMessage;

@FunctionalInterface
public interface ActorVisitor <ActorType extends Actor>
        extends CallableMessage.CallableMessageConsumer<ActorType>, MessageNoRouting {
    void visitActor(ActorType actor, ActorRef sender);

    @Override
    default void accept(ActorType actor, ActorRef sender) {
        if (actor instanceof ActorAggregationReplicable) {
            ActorAggregationReplicable ar = (ActorAggregationReplicable) actor;
            if (ar.getState() instanceof ActorAggregationReplicable.StateLeaf) {
                visitActor(actor, sender);
            } else if (ar.getState() instanceof ActorAggregationReplicable.StateSplitRouter) {
                ActorAggregationReplicable.Split s = ((ActorAggregationReplicable.StateSplitRouter) ar.getState()).getSplit();
                visitActor(actor, sender);
                if (s != null) {
                    s.accept(actor, sender, this);
                }
            } else if (ar.getState() instanceof ActorAggregationReplicable.StateCanceled) {
                visitActor(actor, sender);
            }
        } else {
            visitActor(actor, sender);
        }
    }

    default boolean visitRouter(ActorType actor, ActorRef sender, ActorAggregationReplicable.Split split) {
        return true;
    }

    default void visitRouterNode(ActorType actor, ActorRef sender, ActorAggregationReplicable.SplitNode node) {
        if (visitRouter(actor, sender, node)) {
            node.getLeft().accept(actor, sender, this);
            node.getRight().accept(actor, sender, this);
        }
    }

    default void visitRouterLeaf(ActorType actor, ActorRef sender, ActorAggregationReplicable.SplitLeaf leaf) {
        if (visitRouter(actor, sender, leaf)) {
            leaf.getActor().tell(this, sender);
        }
    }

    static <ActorType extends ActorAggregationReplicable> void tell(ActorType a, ActorVisitor<ActorType> v, ActorRef sender) {
        a.tell(v, sender);
    }

    static <ActorType extends ActorAggregationReplicable> void tell(ActorType a, VisitorNoSender<ActorType> v) {
        a.tell(v);
    }

    static <ActorType extends ActorAggregationReplicable> ActorVisitor<ActorType> visitor(ActorVisitor<ActorType> v) {
        return v;
    }

    static <ActorType extends ActorAggregationReplicable> VisitorNoSender<ActorType> visitorNoSender(VisitorNoSender<ActorType> v) {
        return v;
    }

    @FunctionalInterface
    interface VisitorNoSender<ActorType extends Actor> extends ActorVisitor<ActorType> {
        @Override
        default void visitActor(ActorType actor, ActorRef sender) {
            visitActor(actor);
        }

        void visitActor(ActorType actor);
    }

}
