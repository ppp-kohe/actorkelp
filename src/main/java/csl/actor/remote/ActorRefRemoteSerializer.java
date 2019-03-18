package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorRef;

public class ActorRefRemoteSerializer extends Serializer<ActorRef> {
    protected ActorSystemRemote remoteSystem;

    public ActorRefRemoteSerializer(ActorSystemRemote remoteSystem) {
        this.remoteSystem = remoteSystem;
    }

    @Override
    public void write(Kryo kryo, Output output, ActorRef actorRef) {

    }

    @Override
    public ActorRef read(Kryo kryo, Input input, Class<? extends ActorRef> aClass) {
        return null;
    }
}
