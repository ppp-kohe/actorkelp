package csl.actor;

public interface ActorRef {
    void tellMessage(Message<?> message);

    default void tell(Object data, ActorRef sender) {
        tellMessage(new Message<>(this, sender, data));
    }

    /**
     * tell(data, null)
     * @param data the data
     */
    default void tell(Object data) {
        tell(data, null);
    }

    default Actor asLocal() {
        if (this instanceof Actor) {
            return (Actor) this;
        } else {
            return null;
        }
    }
}
