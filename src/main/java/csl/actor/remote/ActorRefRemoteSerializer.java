package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.ActorRefLocalNamed;

public class ActorRefRemoteSerializer<RefType extends ActorRef> extends Serializer<RefType> {
    protected ActorSystemRemote remoteSystem;

    public ActorRefRemoteSerializer(ActorSystemRemote remoteSystem) {
        this.remoteSystem = remoteSystem;
    }

    @Override
    public void write(Kryo kryo, Output output, RefType actorRef) {
        ActorAddress addr = null;
        if (actorRef instanceof ActorRefRemote) {
            addr = ((ActorRefRemote) actorRef).getAddress();
        } else if (actorRef instanceof ActorRefLocalNamed) {
            addr = remoteSystem.getServerAddress().getActor(((ActorRefLocalNamed) actorRef).getName());
        } else if (actorRef instanceof Actor) {
            addr = remoteSystem.getServerAddress().getActor(((Actor) actorRef).getName());
        }
        kryo.writeClassAndObject(output, addr);
    }

    public ActorSystemRemote getRemoteSystem() {
        return remoteSystem;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RefType read(Kryo kryo, Input input, Class<? extends RefType> aClass) {
        ActorAddress o = (ActorAddress)  kryo.readClassAndObject(input);
        return (RefType) ActorRefRemote.get(remoteSystem, o);
    }
}
