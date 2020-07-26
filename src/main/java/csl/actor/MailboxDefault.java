package csl.actor;

import java.util.concurrent.ConcurrentLinkedQueue;

public class MailboxDefault implements Mailbox, Cloneable {
    protected volatile ConcurrentLinkedQueue<Message<?>> queue = new ConcurrentLinkedQueue<>();

    @Override
    public MailboxDefault create() {
        try {
            MailboxDefault m = (MailboxDefault) super.clone();
            m.queue = new ConcurrentLinkedQueue<>();
            return m;
        } catch (CloneNotSupportedException e) {
            //never
            throw new RuntimeException(e);
        }
    }

    @Override
    public void offer(Message<?> message) {
        queue.offer(message);
    }

    @Override
    public Message<?> poll() {
        return queue.poll();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public ConcurrentLinkedQueue<Message<?>> getQueue() {
        return queue;
    }
}
