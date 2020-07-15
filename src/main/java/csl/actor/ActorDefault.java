package csl.actor;

public abstract class ActorDefault extends Actor {
    protected ActorBehavior behavior;

    public ActorDefault(ActorSystem system, String name, Mailbox mailbox, ActorBehavior behavior) {
        super(system, name, mailbox);
        this.behavior = behavior;
    }

    public ActorDefault(ActorSystem system, String name, ActorBehavior behavior) {
        super(system, name);
        this.behavior = behavior;
    }

    public ActorDefault(ActorSystem system, ActorBehavior behavior) {
        super(system);
        this.behavior = behavior;
    }

    public ActorDefault(ActorSystem system, String name) {
        super(system, name);
        this.behavior = initBehavior();
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
    public void processMessage(Message<?> message) {
        if (!behavior.process(this, message)) {
            processMessageUnhandled(message);
        }
    }

    protected void processMessageUnhandled(Message<?> message) {
        throw new RuntimeException(String.format("actor %s could not handle %s", this, message));
    }

    /** @return implementation field getter */
    public ActorBehavior getBehavior() {
        return behavior;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() +
                "@" + Integer.toHexString(System.identityHashCode(this))
                + "(" + toStringContents() + ")";
    }

    public String toStringContents() {
        return name == null ? "" : name;
    }
}
