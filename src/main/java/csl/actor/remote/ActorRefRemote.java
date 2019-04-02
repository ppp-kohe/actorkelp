package csl.actor.remote;

import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;

public class ActorRefRemote implements ActorRef {
    protected ActorSystem system;
    protected ActorAddress address;

    public static ActorRefRemote get(ActorSystem system, String host, int port, String name) {
        return get(system, ActorAddress.get(host, port, name));
    }

    public static ActorRefRemote get(ActorSystem system, ActorAddress address) {
        return new ActorRefRemote(system, address);
    }

    public ActorRefRemote(ActorSystem system, ActorAddress address) {
        this.system = system;
        this.address = address;
    }

    public ActorAddress getAddress() {
        return address;
    }

    @Override
    public void tell(Object data, ActorRef sender) {
        system.send(new Message<>(this, sender, data));
    }

    @Override
    public String toString() {
        return "ActorRefRemote(" +
                "" + system +
                ", " + address +
                ')';
    }
}
