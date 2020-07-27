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
        this(remoteSystem, true);
    }

    public ActorRefRemoteSerializer(ActorSystem remoteSystem, boolean immutable) {
        super(true, immutable);
        this.remoteSystem = remoteSystem;
    }


    @Override
    public void write(Kryo kryo, Output output, RefType actorRef) {
        kryo.writeClassAndObject(output, getAddress(actorRef));
    }

    static ActorAddress.ActorAddressRemote local = ActorAddress.get("localhost", -1);

    public ActorAddress getAddress(RefType actorRef) {
        ActorAddress addr = null;
        if (actorRef instanceof ActorRefRemote) {
            addr = ((ActorRefRemote) actorRef).getAddress();
        } else if (actorRef instanceof ActorRefLocalNamed) {
            addr = getServerAddress().getActor((ActorRefLocalNamed) actorRef);
        } else if (actorRef instanceof Actor) {
            addr = getServerAddress().getActor((Actor) actorRef);
        }
        return addr;
    }

    public ActorAddress.ActorAddressRemote getServerAddress() {
        if (remoteSystem instanceof ActorSystemRemote) {
            ActorAddress.ActorAddressRemote a =((ActorSystemRemote) remoteSystem).getServerAddress();
            if (a == null) {
                return local;
            } else {
                return a;
            }
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
        if (remoteSystem instanceof ActorSystemRemote) {
            o = ((ActorSystemRemote) remoteSystem).normalizeHostAddress(o);
        }
        return (RefType) ActorRefRemote.get(remoteSystem, o);
    }

}
