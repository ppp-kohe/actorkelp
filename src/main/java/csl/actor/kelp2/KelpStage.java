package csl.actor.kelp2;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.util.ResponsiveCalls;
import csl.actor.util.StagingActor;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public interface KelpStage<ActorType extends Actor> {

    /**
     * set the nextStage property of each shuffle-members to the next actor.
     *  <ol>
     *      <li>If the next is an original actor ({@link ActorKelp#isOriginal()}),
     *          it uses {@link ActorKelp#shuffle(int)} as the actual next</li>
     *      <li>If the next is a remote (or local) reference,
     *          it sends synchronous task for doing the first step. </li>
     *      <li>If the next is a {@link ActorRefShuffle},
     *          it will copy the next for each shuffle-members, by using {@link ActorRefShuffle#use()}</li>
     *  </ol>
     * @param actor the next stage actor
     * @param <NextActorType> the actual actor type of the next stage actor
     * @return the next stage
     */
    @SuppressWarnings("unchecked")
    default <NextActorType extends Actor> KelpStage<NextActorType> connects(NextActorType actor) {
        return connects((Class<NextActorType>) actor.getClass(), actor);
    }

    <NextActorType extends Actor> KelpStage<NextActorType> connects(Class<NextActorType> actorType, ActorRef ref);

    List<ActorRef> getMemberActors();

    ActorType merge();

    class KelpStageRefWrapper<ActorType extends Actor> implements KelpStage<ActorType>, Serializable, KryoSerializable {
        public static final long serialVersionUID = 1L;
        protected transient ActorSystem system;
        protected Class<ActorType> actorType;
        protected ActorRef ref;

        public KelpStageRefWrapper() {}

        public KelpStageRefWrapper(ActorSystem system, Class<ActorType> actorType, ActorRef ref) {
            this.system = system;
            this.actorType = actorType;
            this.ref = ref;
        }

        @Override
        public <NextActorType extends Actor> KelpStage<NextActorType> connects(Class<NextActorType> nextActorType, ActorRef next) {
            if (ref instanceof KelpStage<?>) {
                return ((KelpStage<?>) ref).connects(nextActorType, next);
            } else if (this.actorType.isInstance(KelpStage.class)) {
                return connectsKelpStageSend(nextActorType, next);
            } else if (this.actorType.isInstance(StagingActor.StagingSupported.class)) {
                return connectsStagingSend(nextActorType, next);
            } else {
                throw new RuntimeException("unsupported: (" + actorType + ", " + ref +
                        ").connects(" + nextActorType + ", " + next + ")");
            }
        }


        @SuppressWarnings("unchecked")
        public <NextActorType extends Actor> KelpStage<NextActorType> connectsKelpStageSend(Class<NextActorType> nextActorType, ActorRef next) {
            try {
                return ResponsiveCalls.sendTask(system, ref, (self) ->
                        ((KelpStage<NextActorType>) self).connects(nextActorType, next)).get();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public <NextActorType extends Actor> KelpStage<NextActorType> connectsStagingSend(Class<NextActorType> nextActorType, ActorRef next) {
            ActorRef nextRef = ActorRefShuffle.connectStageInitialActor(system, next, getShuffleBufferSizeMax());
            try {
                ResponsiveCalls.sendTaskConsumer(system, ref, (self) ->
                        ((StagingActor.StagingSupported) self).setNextStage(
                                nextRef));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return ActorKelp.toKelpStage(system, nextActorType, nextRef);
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
            return Collections.singletonList(ref);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void read(Kryo kryo, Input input) {
            actorType = (Class<ActorType>) kryo.readClass(input).getType();
            ref = (ActorRef) kryo.readClassAndObject(input);
        }

        @Override
        public void write(Kryo kryo, Output output) {
            kryo.writeClass(output, actorType);
            kryo.writeClassAndObject(output, ref);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public ActorType merge() {
            if (ActorKelp.class.isAssignableFrom(actorType)) {
                return (ActorType) new ActorKelpMerger(system, new ConfigKelp())
                        .mergeToLocalSync(getMemberActors());
            } else {
                return null;
            }
        }
    }

    class KelpStageRefWrapperSerializer extends Serializer<KelpStageRefWrapper<?>> {
        protected ActorSystem system;
        protected DefaultSerializers.KryoSerializableSerializer serializer;

        public KelpStageRefWrapperSerializer(ActorSystem system) {
            this.system = system;
            serializer = new DefaultSerializers.KryoSerializableSerializer();
        }

        @Override
        public void write(Kryo kryo, Output output, KelpStageRefWrapper<?> actorRefShuffle) {
            serializer.write(kryo, output, actorRefShuffle);
        }

        @Override
        public KelpStageRefWrapper<?> read(Kryo kryo, Input input, Class<? extends KelpStageRefWrapper<?>> aClass) {
            KelpStageRefWrapper<?> r = (KelpStageRefWrapper<?>) serializer.read(kryo, input, aClass);
            if (r != null) {
                r.setSystem(system);
            }
            return r;
        }
    }
}
