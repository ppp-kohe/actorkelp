package csl.actor.remote;

import csl.actor.ActorRef;

public class ActorRefRemote implements ActorRef {
    protected ActorAddress address;

    public ActorAddress getAddress() {
        return address;
    }

    @Override
    public void tell(Object data, ActorRef sender) {

    }
}
