package csl.actor;

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
}
