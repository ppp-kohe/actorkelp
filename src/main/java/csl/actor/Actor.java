package csl.actor;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Actor implements ActorRef {
    protected ActorSystem system;
    protected Mailbox mailbox;
    protected String name;
    protected AtomicBoolean processLock = new AtomicBoolean(false);

    public Actor(ActorSystem system) {
        this(system, null);
    }

    public Actor(ActorSystem system, String name) {
        this.system = system;
        this.name = name;
        if (system != null && name != null) {
            system.register(this);
        }
        initMailbox();
    }

    public String getName() {
        return name;
    }

    protected void initMailbox() {
        this.mailbox = new MailboxDefault();
    }

    public Mailbox getMailbox() {
        return mailbox;
    }

    public ActorSystem getSystem() {
        return system;
    }

    public void offer(Message<?> message) {
        mailbox.offer(message);
    }

    public boolean processMessageBefore() {
        return processLock.compareAndSet(false, true);
    }

    public boolean processMessageNext() {
        Message<?> message = mailbox.poll();
        if (message != null) {
            processMessage(message);
            return true;
        } else {
            return false;
        }
    }

    protected abstract void processMessage(Message<?> message);

    public void processMessageAfter() {
        processLock.set(false);
    }

    @Override
    public void tell(Object data, ActorRef sender) {
        system.send(new Message<>(this, sender, data));
    }
}
