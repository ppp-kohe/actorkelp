package csl.actor.kelp.shuffle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.KelpStage;
import csl.actor.util.ResponsiveCalls;
import csl.actor.util.StagingActor;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class ActorRefShuffleSingle<ActorType extends Actor> implements KelpStage<ActorType>, Serializable, KryoSerializable, StagingActor.StagingNonSubject {
    public static final long serialVersionUID = 1L;
    protected transient ActorSystem system;
    protected Class<ActorType> actorType;
    protected ActorRefShuffle.ShuffleEntry entry;

    public ActorRefShuffleSingle() {
    }

    public ActorRefShuffleSingle(ActorSystem system, Class<ActorType> actorType, ActorRef ref, int bufferSize) {
        this.system = system;
        this.actorType = actorType;
        entry = new ActorRefShuffle.ShuffleEntry(ref, bufferSize, 0);
    }

    @Override
    public <NextActorType extends Actor> KelpStage<NextActorType> connects(Class<NextActorType> nextActorType, ActorRef next) {
        if (entry.getActor() instanceof KelpStage<?>) {
            return ((KelpStage<?>) entry.getActor()).connects(nextActorType, next);
        } else if (this.actorType.isInstance(KelpStage.class)) {
            return connectsKelpStageSend(nextActorType, next);
        } else if (this.actorType.isInstance(StagingActor.StagingSupported.class)) {
            return connectsStagingSend(nextActorType, next);
        } else {
            throw new RuntimeException("unsupported: (" + actorType + ", " + entry.getActor() +
                    ").connects(" + nextActorType + ", " + next + ")");
        }
    }


    @SuppressWarnings("unchecked")
    public <NextActorType extends Actor> KelpStage<NextActorType> connectsKelpStageSend(Class<NextActorType> nextActorType, ActorRef next) {
        try {
            return ResponsiveCalls.sendTask(system, entry.getActor(), (self) ->
                    ((KelpStage<NextActorType>) self).connects(nextActorType, next)).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public <NextActorType extends Actor> KelpStage<NextActorType> connectsStagingSend(Class<NextActorType> nextActorType, ActorRef next) {
        ActorRef nextRef = ActorRefShuffle.connectStageInitialActor(system, next, getShuffleBufferSizeMax());
        try {
            ResponsiveCalls.sendTaskConsumer(system, entry.getActor(), (self) ->
                    ((StagingActor.StagingSupported) self).setNextStage(
                            nextRef));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return ActorKelp.toKelpStage(system, nextActorType, nextRef, entry.getBufferSize());
    }

    public int getShuffleBufferSizeMax() {
        return Integer.MAX_VALUE;
    }

    public void setSystem(ActorSystem system) {
        this.system = system;
    }

    public ActorSystem getSystem() {
        return system;
    }


    @Override
    public List<ActorRef> getMemberActors() {
        return Collections.singletonList(entry.getActor());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void read(Kryo kryo, Input input) {
        actorType = (Class<ActorType>) kryo.readClass(input).getType();
        entry = (ActorRefShuffle.ShuffleEntry) kryo.readClassAndObject(input);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeClass(output, actorType);
        kryo.writeClassAndObject(output, entry);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public ActorType merge() {
        if (ActorKelp.class.isAssignableFrom(actorType)) {
            try (ActorKelpMerger m = new ActorKelpMerger(system, new ConfigKelp())) {
                return (ActorType) m.mergeToLocalSync(getMemberActors());
            }
        } else {
            return null;
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public ActorType getMergedState() {
        if (ActorKelp.class.isAssignableFrom(actorType)) {
            try (ActorKelpMergerSharing<?> m = new ActorKelpMergerSharing<>(system, new ConfigKelp())) {
                return (ActorType) m.mergeToLocalSync(getMemberActors());
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

    @Override
    public void tellMessage(Message<?> message) {
        if (isMessageBroadcasted(message)) {
            entry.getActor().tell(message);
        } else {
            entry.tellMessage(message);
        }
    }

    @Override
    public void flush(ActorRef sender) {
        entry.flush(sender);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + this.actorType.getName() + ", " + entry.getActor() + ")";
    }

    @Override
    public List<ActorRef> getStagingSubjectActors() {
        flush();
        return getMemberActors();
    }

    @Override
    public void forEach(Consumer<ShuffleMember> task) {
        task.accept(entry);
    }
}
