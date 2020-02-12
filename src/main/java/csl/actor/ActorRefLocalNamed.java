package csl.actor;

import csl.actor.remote.ActorAddress;

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
    public void tell(Object data, ActorRef sender) {
        system.send(new Message<>(this, sender, data));
    }

    @Override
    public String toString() {
        return "ref(" + name + ")";
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
