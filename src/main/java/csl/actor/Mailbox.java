package csl.actor;

public abstract class Mailbox {
    public abstract void offer(Message<?> message);
    public abstract Message<?> poll();
}
