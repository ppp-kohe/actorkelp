package csl.actor;

public abstract class ActorDefault extends Actor {
    protected ActorBehavior behavior;

    public ActorDefault(ActorSystem system, ActorBehavior behavior) {
        super(system);
        this.behavior = behavior;
    }

    public ActorDefault(ActorSystem system) {
        super(system);
        this.behavior = initBehavior();
    }

    public void setBehavior(ActorBehavior behavior) {
        this.behavior = behavior;
    }

    protected abstract ActorBehavior initBehavior();

    protected ActorBehaviorBuilder behaviorBuilder() {
        return new ActorBehaviorBuilder();
    }

    @Override
    protected void processMessage(Message<?> message) {
        behavior.process(this, message);
    }
}
