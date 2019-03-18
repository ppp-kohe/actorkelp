package csl.actor;

public interface ActorBehavior {
    /**
     * @param message processed message
     * @return true if processed
     */
    boolean process(Message<?> message);
}
