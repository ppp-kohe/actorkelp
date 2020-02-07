package csl.actor;

public interface Mailbox {
    void offer(Message<?> message);
    Message<?> poll();
    boolean isEmpty();
    Mailbox create();
}
