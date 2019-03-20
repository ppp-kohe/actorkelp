package csl.actor;

public interface ActorBehavior {
    /**
     * @param self target actor which processing the message
     * @param message processed message
     * @return true if processed
     */
    boolean process(Actor self, Message<?> message);
}
