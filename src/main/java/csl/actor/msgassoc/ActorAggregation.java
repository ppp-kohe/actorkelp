package csl.actor.msgassoc;

import csl.actor.*;

public abstract class ActorAggregation extends ActorDefault {
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
        return new ActorBehaviorBuilderKeyValue((ls, ks, sel) -> getMailboxAsAggregation().initMessageTable(ls, ks, sel));
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
}