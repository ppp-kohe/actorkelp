package csl.actor;

public interface ActorRef {
    void tellMessage(Message<?> message);

    /**
     * tell(data, null)
     * @param data the data
     */
    default void tell(Object data) {
        tellMessage(new Message<>(this, data));
    }

    default Actor asLocal() {
        if (this instanceof Actor) {
            return (Actor) this;
        } else {
            return null;
        }
    }
}
