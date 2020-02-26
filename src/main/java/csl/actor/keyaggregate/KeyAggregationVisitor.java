package csl.actor.keyaggregate;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.CallableMessage;

@FunctionalInterface
public interface KeyAggregationVisitor<ActorType extends Actor>
        extends CallableMessage.CallableMessageConsumer<ActorType>, MessageNoRouting {
    void visitActor(ActorType actor, ActorRef sender);

    @Override
    default void accept(ActorType actor, ActorRef sender) {
        if (actor instanceof ActorKeyAggregation) {
            ActorKeyAggregation ar = (ActorKeyAggregation) actor;
            if (ar.getState() instanceof ActorKeyAggregation.StateUnit) {
                visitActor(actor, sender);
            } else if (ar.getState() instanceof KeyAggregationStateRouter) {
                KeyAggregationRoutingSplit s = ((KeyAggregationStateRouter) ar.getState()).getSplit();
                visitActor(actor, sender);
                if (s != null) {
                    s.accept(actor, sender, this);
                }
            } else if (ar.getState() instanceof ActorKeyAggregation.StateCanceled) {
                visitActor(actor, sender);
            }
        } else {
            visitActor(actor, sender);
        }
    }

    default boolean visitRouter(ActorType actor, ActorRef sender, KeyAggregationRoutingSplit split) {
        return true;
    }

    default void visitRouterNode(ActorType actor, ActorRef sender, KeyAggregationRoutingSplit.RoutingSplitNode node) {
        if (visitRouter(actor, sender, node)) {
            node.getLeft().accept(actor, sender, this);
            node.getRight().accept(actor, sender, this);
        }
    }

    default void visitRouterLeaf(ActorType actor, ActorRef sender, KeyAggregationRoutingSplit.RoutingSplitLeaf leaf) {
        if (visitRouter(actor, sender, leaf)) {
            leaf.getActor().tell(this, sender);
        }
    }

    static <ActorType extends ActorKeyAggregation> void tell(ActorType a, KeyAggregationVisitor<ActorType> v, ActorRef sender) {
        a.tell(v, sender);
    }

    static <ActorType extends ActorKeyAggregation> void tell(ActorType a, VisitorNoSender<ActorType> v) {
        a.tell(v);
    }

    static <ActorType extends ActorKeyAggregation> KeyAggregationVisitor<ActorType> visitor(KeyAggregationVisitor<ActorType> v) {
        return v;
    }

    static <ActorType extends ActorKeyAggregation> VisitorNoSender<ActorType> visitorNoSender(VisitorNoSender<ActorType> v) {
        return v;
    }

    @FunctionalInterface
    interface VisitorNoSender<ActorType extends Actor> extends KeyAggregationVisitor<ActorType> {
        @Override
        default void visitActor(ActorType actor, ActorRef sender) {
            visitActor(actor);
        }

        void visitActor(ActorType actor);
    }

}
