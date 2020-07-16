package csl.actor.kelp2;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.Message;
import csl.actor.cluster.ActorSystemCluster;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ActorSystemKelp extends ActorSystemRemote {

    public ActorSystemKelp() {
        super(new ActorSystemCluster.ActorSystemDefaultForCluster(), KryoBuilder.builder());
    }

    public ActorSystemKelp(ActorSystemDefault localSystem, Function<ActorSystem, Kryo> kryoFactory) {
        super(localSystem, kryoFactory);
    }

    @Override
    protected ConnectionActor createConnection(ActorAddress addr) {
        if (addr instanceof ActorAddress.ActorAddressRemote) {
            try {
                return new ConnectionActorKelp(localSystem, this, (ActorAddress.ActorAddressRemote) addr);
            } catch (InterruptedException ex) {
                getLogger().log(true, debugLogColor, ex, "createConnection: %s", addr);
                return null;
            }
        } else {
            return null;
        }
    }

    public static class ConnectionActorKelp extends ConnectionActor {
        protected Instant previousTime;
        public ConnectionActorKelp(ActorSystem system, ActorSystemRemote remoteSystem, ActorAddress.ActorAddressRemote address) throws InterruptedException {
            super(system, remoteSystem, address);
            previousTime = Instant.now();
            getSystem().getLogger().log(debugLogMsg, debugLogColor, "ConnectionActorKelp: %s %s", address, ActorSystem.timeForLog(previousTime));
        }

        @Override
        public void tell(Object data) {
            super.tell(data);
        }

        @Override
        protected void writeSingleMessage(Message<?> message) {
            long pre = connection.getRecordSendBytes();
            try {
                connection.getChannel().writeAndFlush(new TransferredMessage(count, message)).sync();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            long pos = connection.getRecordSendBytes();
            long len = pos - pre;
            getSystem().getLogger().log(debugLogMsg, debugLogColor, "writeSingleMessage: %,d bytes time=%s", len, Duration.between(previousTime, Instant.now()));
            previousTime = Instant.now();
        }

        @Override
        protected void writeNonEmpty(List<Object> messageBundle) {
            long pre = connection.getRecordSendBytes();
            try {
                connection.getChannel().writeAndFlush(new TransferredMessage(count, messageBundle)).sync();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            long pos = connection.getRecordSendBytes();
            long len = pos - pre;
            getSystem().getLogger().log(debugLogMsg, debugLogColor, "writeNonEmpty: %,d bytes time=%s", len, Duration.between(previousTime, Instant.now()));
            previousTime = Instant.now();
        }
    }

    public static class Breaker {

    }
}
