package csl.actor.kelp;

import csl.actor.Actor;
import csl.actor.CallableMessage;

public interface MessageNoRouting {
    interface Routing { }

    interface CallableMessageRouting<A extends Actor,T> extends CallableMessage<A,T>, Routing { }

    static <A extends Actor,T> MessageNoRouting.CallableMessageRouting<A,T> callableRouting(MessageNoRouting.CallableMessageRouting<A,T> t) {
        return t;
    }
}
