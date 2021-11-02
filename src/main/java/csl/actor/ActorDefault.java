package csl.actor;

import java.util.Map;

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

    protected ActorBehavior initBehavior() {
        return initBehavior(behaviorBuilder()).build();
    }

    protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilder builder) {
        return builder;
    }

    protected ActorBehaviorBuilder behaviorBuilder() {
        return new ActorBehaviorBuilder();
    }


    /**
     * obtains a parameterized class for matching: e.g.
     *    <code>builder.match(this.&lt;String&gt;type(List.class), listStr -&gt; ...)</code>
     * @param cls the casting type
     * @param <R> a type arg including generics such as &lt;T&lt;String&gt;&gt;
     * @return cls with type R
     */
    @SuppressWarnings({"unchecked", "rawtype"})
    public <R> Class<R> type(Class<?> cls) {
        return (Class<R>) cls;
    }

    /**
     * obtains a parameterized class for matching with Map.Entry:
     *    <code>builder.match(this.&lt;String,Integer&gt;typeEntry(), e -&gt; ...)</code>
     * @param <K> the key type
     * @param <V> the value type
     * @return Class&lt;Map.Entry&lt;K,V&gt;&gt;
     */
    public <K,V> Class<Map.Entry<K,V>> typeEntry() {
        return type(Map.Entry.class);
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
