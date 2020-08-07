package csl.actor.kelp.shuffle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.kelp.KelpStage;
import csl.actor.remote.ActorRefRemoteSerializer;

public class KelpStageSerializer extends ActorRefRemoteSerializer<ActorRef> {
    protected DefaultSerializers.KryoSerializableSerializer serializer;
    public static final int BYTE_MARK_REF = 0;
    public static final int BYTE_MARK_SHUFFLE = 1;

    public KelpStageSerializer(ActorSystem system) {
        super(system);
        serializer = new DefaultSerializers.KryoSerializableSerializer();
    }

    @Override
    public void write(Kryo kryo, Output output, ActorRef ref) {
        if ((ref instanceof KelpStage && ref instanceof KryoSerializable) ||
                ref instanceof ActorRefShuffle.ShuffleEntry || //ShuffleEntry is an ActorRef and KryoSerializable
                ref instanceof ActorRefShuffle) { //ActorRefShuffle is KryoSerializable but not a KelpStage
            output.writeByte(BYTE_MARK_SHUFFLE);
            serializer.write(kryo, output, (KryoSerializable) ref);
        } else {
            output.writeByte(BYTE_MARK_REF);
            super.write(kryo, output, ref);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public ActorRef read(Kryo kryo, Input input, Class<? extends ActorRef> aClass) {
        int n = input.readByte();
        if (n == BYTE_MARK_SHUFFLE) {
            ActorRef r = (ActorRef) serializer.read(kryo, input, (Class<? extends KryoSerializable>) aClass);
            if (r != null) {
                if (r instanceof KelpStage<?>) {
                    ((KelpStage<?>) r).setSystemBySerializer(getRemoteSystem());
                } else if (r instanceof ActorRefShuffle) {
                    ((ActorRefShuffle) r).setSystemBySerializer(getRemoteSystem());
                }
            }
            return r;
        } else {
            return super.read(kryo, input, aClass);
        }
    }

    @Override
    public ActorRef copy(Kryo kryo, ActorRef original) {
        if (original instanceof ActorRefShuffle) {
            return ((ActorRefShuffle) original).use();
        } else {
            return original;
        }
    }
}
