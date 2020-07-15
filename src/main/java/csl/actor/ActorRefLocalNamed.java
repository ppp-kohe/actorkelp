package csl.actor;

import csl.actor.remote.ActorAddress;

import java.util.Objects;

public class ActorRefLocalNamed implements ActorRef {
    protected ActorSystem system;
    protected String name;

    public static ActorRefLocalNamed get(ActorSystem system, String name) {
        return new ActorRefLocalNamed(system, name);
    }

    public ActorRefLocalNamed(ActorSystem system, String name) {
        this.system = system;
        this.name = name;
    }

    /** @return implementation field getter */
    public ActorSystem getSystem() {
        return system;
    }

    public String getName() {
        return name;
    }

    @Override
    public void tellMessage(Message<?> message) {
        system.send(message);
    }

    @Override
    public String toString() {
        return "ref(" + name + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActorRefLocalNamed that = (ActorRefLocalNamed) o;
        return Objects.equals(system, that.system) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(system, name);
    }

    public static class ActorRefLocalNamedNoName extends ActorRefLocalNamed {
        protected ActorAddress origin;

        public ActorRefLocalNamedNoName(ActorSystem system, ActorAddress origin) {
            super(system, null);
            this.origin = origin;
        }

        public ActorAddress getOrigin() {
            return origin;
        }

        @Override
        public String toString() {
            return "ref(anonymous," + origin + ")";
        }
    }
}
