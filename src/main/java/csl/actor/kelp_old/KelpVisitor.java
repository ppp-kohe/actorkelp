package csl.actor.kelp_old;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.CallableMessage;

@FunctionalInterface
public interface KelpVisitor<ActorType extends Actor>
        extends CallableMessage.CallableMessageConsumer<ActorType>, MessageNoRouting {
    void visitActor(ActorType actor, ActorRef sender);

    default void accept(ActorType actor) {
        accept(actor, null);
    }

    @Override
    default void accept(ActorType actor, ActorRef sender) {
        if (actor instanceof ActorKelp) {
            ActorKelp<?> ar = (ActorKelp<?>) actor;
            if (ar.getState() instanceof ActorKelp.StateUnit) {
                visitActor(actor, sender);
            } else if (ar.getState() instanceof KelpStateRouter) {
                KelpRoutingSplit s = ((KelpStateRouter) ar.getState()).getSplit();
                visitActor(actor, sender);
                if (s != null) {
                    s.accept(actor, sender, this);
                }
            } else if (ar.getState() instanceof ActorKelp.StateCanceled) {
                visitActor(actor, sender);
            }
        } else {
            visitActor(actor, sender);
        }
    }

    default boolean visitRouter(ActorType actor, ActorRef sender, KelpRoutingSplit split) {
        return true;
    }

    default void visitRouterNode(ActorType actor, ActorRef sender, KelpRoutingSplit.RoutingSplitNode node) {
        if (visitRouter(actor, sender, node)) {
            node.getLeft().accept(actor, sender, this);
            node.getRight().accept(actor, sender, this);
        }
    }

    default void visitRouterLeaf(ActorType actor, ActorRef sender, KelpRoutingSplit.RoutingSplitLeaf leaf) {
        if (visitRouter(actor, sender, leaf)) {
            leaf.getActor().tell(this, sender);
        }
    }

    static <ActorType extends ActorKelp<ActorType>> void tell(ActorType a, KelpVisitor<ActorType> v, ActorRef sender) {
        a.tell(v, sender);
    }

    static <ActorType extends ActorKelp<ActorType>> void tell(ActorType a, VisitorNoSender<ActorType> v) {
        a.tell(v);
    }

    static <ActorType extends ActorKelp<ActorType>> KelpVisitor<ActorType> visitor(KelpVisitor<ActorType> v) {
        return v;
    }

    static <ActorType extends ActorKelp<ActorType>> VisitorNoSender<ActorType> visitorNoSender(VisitorNoSender<ActorType> v) {
        return v;
    }

    @FunctionalInterface
    interface VisitorNoSender<ActorType extends Actor> extends KelpVisitor<ActorType> {
        @Override
        default void visitActor(ActorType actor, ActorRef sender) {
            visitActor(actor);
        }

        void visitActor(ActorType actor);
    }

}
