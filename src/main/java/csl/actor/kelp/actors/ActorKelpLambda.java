package csl.actor.kelp.actors;

import csl.actor.ActorBehavior;
import csl.actor.ActorBehaviorBuilder;
import csl.actor.ActorSystem;
import csl.actor.Mailbox;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.behavior.ActorBehaviorBuilderKelp;

import java.io.Serializable;

public class ActorKelpLambda extends ActorKelp<ActorKelpLambda> {
    protected transient ActorBuilder builderFunction;

    public interface ActorBuilder extends Serializable {
        ActorBehaviorBuilder build(ActorKelpLambda self, ActorBehaviorBuilderKelp builder);
    }

    public ActorKelpLambda(ActorSystem system, String name, Mailbox mailbox, ActorBehavior behavior, ConfigKelp config) {
        super(system, name, mailbox, behavior, config);
    }

    //for serialization
    public ActorKelpLambda(ActorSystem system, String name, ConfigKelp config, Object consState) {
        super(system, name, config, consState);
    }

    @Override
    protected void initConstructionState(Object constructionState) {
        this.builderFunction = (ActorBuilder) constructionState;
    }

    @Override
    public Object getConstructionState() {
        return builderFunction;
    }

    public ActorKelpLambda(ActorSystem system, String name, ConfigKelp config, ActorBuilder builderFunction) {
        this(system, name, null, null, config);
        this.builderFunction = builderFunction;
        initMailboxAndBehavior();
    }

    public ActorKelpLambda(ActorSystem system, ConfigKelp config, ActorBuilder builderFunction) {
        this(system, null, null, null, config);
        setNameRandom();
        this.builderFunction = builderFunction;
        initMailboxAndBehavior();
    }

    @Override
    protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilderKelp builder) {
        return builderFunction.build(this, builder);
    }

}
