package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ActorSystemRemote implements ActorSystem {
    protected ActorSystem localSystem;

    protected InetSocketAddress serverAddress;
    protected ServerSocketChannel serverChannel;
    protected Map<Object, Connection> connectionMap;
    protected ActorRefRemoteSerializer refSerializer;
    protected Kryo serializer;

    public ActorSystemRemote(ActorSystem localSystem) {
        this.localSystem = localSystem;
        init();
    }

    protected void init() {
        initConnectionMap();
        initSerializer();
    }

    protected void initConnectionMap() {
        connectionMap = new ConcurrentHashMap<>();
    }

    protected void initSerializer() {
        serializer = new Kryo();
        serializer.register(Message.class);
        refSerializer = new ActorRefRemoteSerializer(this);
        serializer.register(ActorRef.class, refSerializer);
    }

    public void start(InetSocketAddress serverAddress) {
        this.serverAddress = serverAddress;
        try {
            this.serverChannel = ServerSocketChannel.open().bind(serverAddress);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        //TODO accept forever
    }

    @Override
    public void send(Message<?> message) {
        ActorRef target = message.getTarget();
        if (target instanceof ActorRefRemote) {
            ActorAddress addr = ((ActorRefRemote) target).getAddress();
            connectionMap.computeIfAbsent(addr.getKey(), k -> createConnection(addr))
                    .tell(message, null);
        } else {
            localSystem.send(message);
        }
    }

    protected Connection createConnection(ActorAddress addr) {
        return new Connection(localSystem, this, addr);
    }

    public SocketChannel createChannel(String host, int port) {
        try {
            return SocketChannel.open(InetSocketAddress.createUnresolved(host, port));
        }catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ByteBuffer serialize(Message<?> message) {
        ByteBufferOutput output = new ByteBufferOutput();
        serializer.writeObject(output, message);
        output.close();
        return output.getByteBuffer();
    }

    public static class Connection extends Actor {
        protected ActorSystemRemote remoteSystem;
        protected ActorAddress address;
        protected SocketChannel channel;

        public Connection(ActorSystem system, ActorSystemRemote remoteSystem, ActorAddress address) {
            super(system);
            this.remoteSystem = remoteSystem;
            this.address = address;
        }

        @Override
        protected void processMessage(Message<?> message) {
            send((Message) message.getData());
        }

        public void send(Message<?> message) {
            if (channel != null && !channel.isConnected()) {
                try {
                    channel.close();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                channel = null;
            }
            if (channel == null) {
                if (address instanceof ActorAddress.ActorAddressDefault) {
                    channel = ((ActorAddress.ActorAddressDefault) address).createChannel(remoteSystem);
                }
            }
            try {
                channel.write(remoteSystem.serialize(message));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
