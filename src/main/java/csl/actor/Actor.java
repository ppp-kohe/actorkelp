package csl.actor;

public abstract class Actor implements ActorRef {
    protected ActorSystem system;
    protected Mailbox mailbox;

    public Actor(ActorSystem system) {
        this.system = system;
        initMailbox();
    }

    protected void initMailbox() {
        this.mailbox = new MailboxDefault();
    }

    public Mailbox getMailbox() {
        return mailbox;
    }

    public void offer(Message<?> message) {
        mailbox.offer(message);
    }

    public boolean offerAndProcess(Message<?> message) {
        offer(message);
        return processMessageNext();
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

    @Override
    public void tell(Object data, ActorRef sender) {
        system.send(new Message<Object>(this, sender, data));
    }
}
