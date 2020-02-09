package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.ActorRefLocalNamed;
import csl.actor.ActorSystem;

public class ActorRefRemoteSerializer<RefType extends ActorRef> extends Serializer<RefType> {
    protected ActorSystem remoteSystem;

    public ActorRefRemoteSerializer(ActorSystem remoteSystem) {
        this.remoteSystem = remoteSystem;
    }

    @Override
    public void write(Kryo kryo, Output output, RefType actorRef) {
        ActorAddress addr = null;
        if (actorRef instanceof ActorRefRemote) {
            addr = ((ActorRefRemote) actorRef).getAddress();
        } else if (actorRef instanceof ActorRefLocalNamed) {
            addr = getServerAddress().getActor(((ActorRefLocalNamed) actorRef).getName());
        } else if (actorRef instanceof Actor) {
            getServerAddress().getActor(((Actor) actorRef).getName());
        }
        kryo.writeClassAndObject(output, addr);
    }

    static ActorAddress.ActorAddressRemote local = ActorAddress.get("localhost", -1);

    public ActorAddress.ActorAddressRemote getServerAddress() {
        if (remoteSystem instanceof ActorSystemRemote) {
            return ((ActorSystemRemote) remoteSystem).getServerAddress();
        } else {
            return local;
        }
    }

    /** @return implementation field getter */
    public ActorSystem getRemoteSystem() {
        return remoteSystem;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RefType read(Kryo kryo, Input input, Class<? extends RefType> aClass) {
        ActorAddress o = (ActorAddress)  kryo.readClassAndObject(input);
        return (RefType) ActorRefRemote.get(remoteSystem, o);
    }
}
