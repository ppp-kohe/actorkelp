package csl.actor.kelp.shuffle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.KelpStage;
import csl.actor.kelp.behavior.KelpDispatcher;
import csl.actor.util.ResponsiveCalls;
import csl.actor.util.Staging;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class ActorRefShuffleSingle<ActorType extends Actor> implements KelpStage<ActorType>, Serializable, KryoSerializable,
        Staging.StagingPointMembers {
    public static final long serialVersionUID = 1L;
    protected transient ActorSystem system;
    protected Class<ActorType> actorType;
    protected ActorRefShuffle.ShuffleEntry entry;
    protected String name;

    public ActorRefShuffleSingle() {
    }

    public ActorRefShuffleSingle(ActorSystem system, Class<ActorType> actorType, ActorRef ref, int bufferSize) {
        this.system = system;
        this.actorType = actorType;
        entry = new ActorRefShuffle.ShuffleEntry(ref, bufferSize, 0);
        this.name = Staging.stageNameArray(Staging.name(ref), "single");
    }

    public ActorRefShuffleSingle(ActorSystem system, Class<ActorType> actorType, ActorRefShuffle.ShuffleEntry entry) {
        this.system = system;
        this.actorType = actorType;
        this.entry = entry;
        this.name = Staging.stageNameArray(Staging.name(entry.getActor()), "single");
    }

    @Override
    public Class<ActorType> getActorType() {
        return actorType;
    }

    @Override
    public <NextActorType extends Actor> KelpStage<NextActorType> connects(Class<NextActorType> nextActorType, ActorRef next) {
        if (entry.getActor() instanceof KelpStage<?>) {
            return ((KelpStage<?>) entry.getActor()).connects(nextActorType, next);
        } else if (this.actorType.isInstance(KelpStage.class)) {
            return connectsKelpStageSend(nextActorType, next);
        } else if (this.actorType.isInstance(Staging.StagingSupported.class)) {
            return connectsStagingSend(nextActorType, next);
        } else {
            throw new RuntimeException("unsupported: (" + actorType + ", " + entry.getActor() +
                    ").connects(" + nextActorType + ", " + next + ")");
        }
    }


    @SuppressWarnings("unchecked")
    public <NextActorType extends Actor> KelpStage<NextActorType> connectsKelpStageSend(Class<NextActorType> nextActorType, ActorRef next) {
        try {
            return ResponsiveCalls.sendTask(system, entry.getActor(),
                    new MessageConnectKelpStageSend<>(nextActorType, next)).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class MessageConnectKelpStageSend<NextActorType extends Actor>
            implements CallableMessage<Actor,KelpStage<NextActorType>> {
        public static final long serialVersionUID = -1;
        public Class<NextActorType> nextActorType;
        public ActorRef next;

        public MessageConnectKelpStageSend() {}

        public MessageConnectKelpStageSend(Class<NextActorType> nextActorType, ActorRef next) {
            this.nextActorType = nextActorType;
            this.next = next;
        }

        @Override
        @SuppressWarnings("unchecked")
        public KelpStage<NextActorType> call(Actor self) {
            return ((KelpStage<NextActorType>) self).connects(nextActorType, next);
        }
    }

    public <NextActorType extends Actor> KelpStage<NextActorType> connectsStagingSend(Class<NextActorType> nextActorType, ActorRef next) {
        ActorRef nextRef = ActorRefShuffle.connectStageInitialActor(system, next, getShuffleBufferSizeMax());
        try {
            ResponsiveCalls.sendTaskConsumer(system, entry.getActor(), new MessageConnectStagingSend(nextRef));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return ActorKelp.toKelpStage(system, nextActorType, nextRef, entry.getBufferSize());
    }

    public static class MessageConnectStagingSend
            implements CallableMessage.CallableMessageConsumer<Actor> {
        public static final long serialVersionUID = -1;
        public ActorRef nextRef;

        public MessageConnectStagingSend() {}

        public MessageConnectStagingSend(ActorRef nextRef) {
            this.nextRef = nextRef;
        }

        @Override
        public void accept(Actor self) {
            ((Staging.StagingSupported) self).setNextStage(nextRef);
        }
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
        name = input.readString();
        actorType = (Class<ActorType>) kryo.readClass(input).getType();
        entry = (ActorRefShuffle.ShuffleEntry) kryo.readClassAndObject(input);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(name);
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
    public void flush() {
        entry.flush();
    }

    @Override
    public String toString() {
        return getName() + "(" + this.actorType.getName() + ", " + entry.getActor() + ")";
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<ActorRef> getStagingSubjectActors() {
        flush();
        return getMemberActors();
    }

    @Override
    public void forEach(Consumer<KelpDispatcher.DispatchUnit> task) {
        task.accept(entry);
    }

    @Override
    public List<? extends KelpDispatcher.DispatchUnit> getDispatchUnits() {
        return Collections.singletonList(entry);
    }

    @Override
    public KelpDispatcher.DispatchUnit getDispatchUnit(int index) {
        return index == 0 ? entry : null;
    }
}
