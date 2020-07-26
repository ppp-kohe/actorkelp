package csl.actor;

import java.util.Queue;

public interface Mailbox {
    void offer(Message<?> message);
    Message<?> poll();
    boolean isEmpty();

    /**
     * @return a new empty clone
     */
    Mailbox create();

    Queue<Message<?>> getQueue();
}
