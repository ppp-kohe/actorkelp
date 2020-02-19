package csl.actor;

public interface ActorRef {
    void tell(Object data, ActorRef sender);

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
