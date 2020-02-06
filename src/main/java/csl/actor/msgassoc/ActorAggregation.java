package csl.actor.msgassoc;

import csl.actor.*;

public abstract class ActorAggregation extends ActorDefault {
    public ActorAggregation(ActorSystem system, String name, MailboxAggregation mailbox, ActorBehavior behavior) {
        super(system, name, mailbox, behavior);
    }

    public ActorAggregation(ActorSystem system, String name, ActorBehavior behavior) {
        super(system, name, behavior);
    }

    public ActorAggregation(ActorSystem system, ActorBehavior behavior) {
        super(system, behavior);
    }

    public ActorAggregation(ActorSystem system, String name) {
        super(system, name);
    }

    public ActorAggregation(ActorSystem system) {
        super(system);
    }

    @Override
    protected void initMailbox() {
        this.mailbox = new MailboxAggregation();
    }

    @Override
    protected ActorBehaviorBuilderKeyValue behaviorBuilder() {
        return new ActorBehaviorBuilderKeyValue((ps) -> getMailboxAsAggregation().initMessageTable(ps));
    }

    public MailboxAggregation getMailboxAsAggregation() {
        return (MailboxAggregation) mailbox;
    }

    @Override
    public boolean processMessageNext() {
        if (getMailboxAsAggregation().processTable()) {
            return true;
        }
        return super.processMessageNext();
    }

    @Override
    protected void processMessage(Message<?> message) {
        if (message.getData() instanceof MailboxAggregation.TraversalProcess) {
            getMailboxAsAggregation()
                    .processTraversal(this, ((MailboxAggregation.TraversalProcess) message.getData()).entryId);
        } else {
            super.processMessage(message);
        }
    }
}
