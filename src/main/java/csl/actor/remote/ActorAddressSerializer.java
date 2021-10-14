package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ActorAddressSerializer extends Serializer<ActorAddress> {
    final static int NULL = 0;
    final static int ANONYMOUS_ACTOR = 1;
    final static int ERROR = 2;
    final static int REMOTE_ACTOR = 3;
    final static int REMOTE = 4;

    @Override
    public void write(Kryo kryo, Output output, ActorAddress object) {
        if (object == null) {
            output.write(NULL);
        } else if (object instanceof ActorAddress.ActorAddressRemote) {
            if (object instanceof ActorAddress.ActorAddressRemoteActor) {
                if (object instanceof ActorAddress.ActorAddressAnonymousActor) {
                    ActorAddress.ActorAddressAnonymousActor a = (ActorAddress.ActorAddressAnonymousActor) object;
                    output.write(ANONYMOUS_ACTOR);
                    output.writeString(a.getHost());
                    output.writeInt(a.getPort());
                    output.writeString(a.getActorName());
                    output.writeString(a.getTypeName());
                    output.writeInt(a.getIdentityHashCode());
                } else if (object instanceof ActorAddress.ActorAddressError) {
                    output.write(ERROR);
                    ActorAddress.ActorAddressError a = (ActorAddress.ActorAddressError) object;
                    output.writeString(a.getHost());
                    output.writeInt(a.getPort());
                    output.writeString(a.getActorName());
                    output.writeString(a.getInfo());
                } else {
                    output.write(REMOTE_ACTOR);
                    ActorAddress.ActorAddressRemoteActor a = (ActorAddress.ActorAddressRemoteActor) object;
                    output.writeString(a.getHost());
                    output.writeInt(a.getPort());
                    output.writeString(a.getActorName());
                }
            } else {
                output.write(REMOTE);
                ActorAddress.ActorAddressRemote a = (ActorAddress.ActorAddressRemote) object;
                output.writeString(a.getHost());
                output.writeInt(a.getPort());
            }
        } else {
            throw new RuntimeException("unsupported:" + object);
        }
    }

    @Override
    public ActorAddress read(Kryo kryo, Input input, Class<? extends ActorAddress> type) {
        int n = input.read();
        switch (n) {
            case REMOTE:
                return new ActorAddress.ActorAddressRemote(input.readString(), input.readInt());
            case REMOTE_ACTOR:
                return new ActorAddress.ActorAddressRemoteActor(input.readString(), input.readInt(), input.readString());
            case ERROR:
                return new ActorAddress.ActorAddressError(input.readString(), input.readInt(), input.readString());
            case ANONYMOUS_ACTOR:
                return new ActorAddress.ActorAddressAnonymousActor(input.readString(), input.readInt(), input.readString(), input.readInt());
        }
        return null;
    }
}
