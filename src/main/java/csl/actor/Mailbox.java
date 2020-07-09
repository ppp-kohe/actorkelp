package csl.actor;

public interface Mailbox {
    void offer(Message<?> message);
    Message<?> poll();
    boolean isEmpty();

    /**
     * @return a new empty clone
     */
    Mailbox create();
}
