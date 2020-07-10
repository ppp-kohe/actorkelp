package csl.actor.kelp2;

import csl.actor.ActorBehavior;
import csl.actor.ActorDefault;
import csl.actor.ActorSystem;
import csl.actor.Mailbox;

public class ActorKelp<SelfType extends csl.actor.kelp.ActorKelp<SelfType>> extends ActorDefault {
    public ActorKelp(ActorSystem system, String name, Mailbox mailbox, ActorBehavior behavior) {
        super(system, name, mailbox, behavior);
    }

    @Override
    protected ActorBehavior initBehavior() {
        return null; //TODO
    }
}
