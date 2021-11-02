package csl.actor.kelp.shuffle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.KelpStage;
import csl.actor.kelp.behavior.KelpDispatcher;
import csl.actor.util.Staging;

import java.util.List;
import java.util.function.BiFunction;

public class ActorRefShuffleKelp<ActorType extends ActorKelp<ActorType>> extends ActorRefShuffle implements KelpStage<ActorType>, Staging.StagingPointMembers {
    public static final long serialVersionUID = 1L;
    protected Class<ActorType> actorType;
    protected ConfigKelp config;

    public ActorRefShuffleKelp() {
    }

    public ActorRefShuffleKelp(ActorSystem system, List<ShuffleEntry> entries,
                               List<KelpDispatcher.SelectiveDispatcher> extractorsAndDispatchers, int bufferSize,
                               Class<ActorType> actorType, ConfigKelp config, String name) {
        super(system, entries, extractorsAndDispatchers, bufferSize);
        this.actorType = actorType;
        this.config = config;
        this.name = Staging.stageNameArray(name, "shuffle");
    }
    @Override
    public Class<ActorType> getActorType() {
        return actorType;
    }

    @Override
    public <NextActorType extends Actor> KelpStage<NextActorType> connects(Class<NextActorType> actorType, ActorRef ref) {
        ref = connectStageInitialActor(ref, Integer.MAX_VALUE);
        try {
            connectStageWithoutInit(ref).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return ActorKelp.toKelpStage(system, actorType, ref, getBufferSize());
    }

    @Override
    public void write(Kryo kryo, Output output) {
        super.write(kryo, output);
        output.writeString(name);
        kryo.writeClass(output, actorType);
        kryo.writeClassAndObject(output, config);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void read(Kryo kryo, Input input) {
        super.read(kryo, input);
        name = input.readString();
        actorType = (Class<ActorType>) kryo.readClass(input).getType();
        config = (ConfigKelp) kryo.readClassAndObject(input);
    }

    @Override
    public ActorType merge() {
        try (ActorKelpMerger<ActorType> m = new ActorKelpMerger<>(system, config)) {
            return m.mergeToLocalSync(getMemberActors());
        }
    }

    @Override
    public ActorType getMergedState() {
        if (ActorKelp.class.isAssignableFrom(actorType)) {
            try (ActorKelpMergerSharing<ActorType> m = new ActorKelpMergerSharing<>(system, config)) {
                return m.mergeToLocalSync(getMemberActors());
            }
        } else {
            return null;
        }
    }

    @Override
    public <StateType> StateType merge(BiFunction<ActorSystem, ConfigKelp, ? extends ActorKelpStateSharing<ActorType, StateType>> factory) {
        try (ActorKelpStateSharing<ActorType, StateType> m = factory.apply(system, new ConfigKelp())) {
            return m.mergeSync(getMemberActors());
        }
    }
}
