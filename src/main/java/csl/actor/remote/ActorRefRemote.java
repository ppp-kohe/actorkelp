package csl.actor.remote;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;

import java.util.Objects;

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

    /** @return implementation field getter */
    public ActorSystem getSystem() {
        return system;
    }

    public ActorAddress getAddress() {
        return address;
    }

    @Override
    public void tellMessage(Message<?> message) {
        system.send(message);
    }

    @Override
    public String toString() {
        return "ActorRefRemote(" +
                "" + system +
                ", " + address +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActorRefRemote that = (ActorRefRemote) o;
        return Objects.equals(system, that.system) &&
                Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(system, address);
    }

    @Override
    public Actor asLocal() {
        return system.resolveActorLocalNamed(address.toLocal(system));
    }
}
