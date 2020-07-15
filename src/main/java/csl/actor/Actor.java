package csl.actor;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Actor implements ActorRef {
    protected transient ActorSystem system;
    protected Mailbox mailbox;
    protected volatile String name;
    protected transient AtomicBoolean processLock = new AtomicBoolean(false);
    protected Mailbox delayedMailbox;

    public Actor(ActorSystem system) {
        this(system, null);
    }

    public Actor(ActorSystem system, String name) {
        this(system, name, null);
        mailbox = initMailbox();
    }

    public Actor(ActorSystem system, String name, Mailbox mailbox) {
        this.system = system;
        this.name = name;
        if (system != null && name != null) {
            system.register(this);
        }
        this.mailbox = mailbox;
    }

    public String getName() {
        return name;
    }

    protected Mailbox initMailbox() {
        return new MailboxDefault();
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

    public boolean processMessageLocked() {
        return processLock.get();
    }

    /**
     * @return true if {@link #getMailbox()} is empty. No checking for {@link #getDelayedMailbox()}
     */
    public boolean isEmptyMailbox() {
        return mailbox.isEmpty();
    }

    /**
     * @return poll and process a message in the mailbox. using {@link #getMailbox()} and {@link #getDelayedMailbox()}
     */
    public boolean processMessageNext() {
        Message<?> message = mailbox.poll();
        if (message != null) {
            processMessage(message);
            return true;
        } else {
            Mailbox dm = delayedMailbox;
            if (dm != null && !dm.isEmpty() &&
                    (message = dm.poll()) != null) {
                processDelayedMessage(message);
                return true;
            } else {
                return false;
            }
        }
    }

    public abstract void processMessage(Message<?> message);

    public void processMessageAfter() {
        processLock.set(false);
    }

    @Override
    public void tellMessage(Message<?> message) {
        system.send(message);
    }

    /**
     * @return another mailbox used for observing phase completion
     */
    public Mailbox getDelayedMailbox() {
        if (delayedMailbox == null) {
            synchronized (this) {
                if (delayedMailbox == null) {
                    delayedMailbox = initDelayedMailbox();
                }
            }
        }
        return delayedMailbox;
    }

    protected Mailbox initDelayedMailbox() {
        return new MailboxDefault();
    }

    protected void processDelayedMessage(Message<?> message) {
        processMessage(message);
    }
}
