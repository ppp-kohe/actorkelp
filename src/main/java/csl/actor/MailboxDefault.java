package csl.actor;

import java.util.concurrent.ConcurrentLinkedQueue;

public class MailboxDefault extends Mailbox {
    protected ConcurrentLinkedQueue<Message<?>> queue = new ConcurrentLinkedQueue<>();

    @Override
    public void offer(Message<?> message) {
        queue.offer(message);
    }

    @Override
    public Message<?> poll() {
        return queue.poll();
    }

    /** @return implementation field getter */
    public ConcurrentLinkedQueue<Message<?>> getQueue() {
        return queue;
    }
}
