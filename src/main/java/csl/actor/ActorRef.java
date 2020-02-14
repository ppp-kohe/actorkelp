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
}
