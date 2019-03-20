package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import csl.actor.*;
import io.netty.channel.EventLoopGroup;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ActorSystemRemote implements ActorSystem {
    protected ActorSystemDefault localSystem;

    protected ActorAddress.ActorAddressRemote serverAddress;

    protected ObjectMessageServer server;
    protected ObjectMessageClient client;

    protected Map<Object, ConnectionActor> connectionMap;
    protected Kryo serializer;

    public ActorSystemRemote() {
        this(new ActorSystemDefaultForRemote());
    }

    public ActorSystemRemote(ActorSystemDefault localSystem) {
        this.localSystem = localSystem;
        init();
    }

    protected void init() {
        initConnectionMap();
        initSerializer();
        initServerAndClient();
    }

    protected void initConnectionMap() {
        connectionMap = new ConcurrentHashMap<>();
    }

    protected void initSerializer() {
        serializer = new KryoBuilder()
                .setSystem(this)
                .build();
    }

    protected void initServerAndClient() {
        server = new ObjectMessageServer();
        server.setReceiver(this::receive);
        client = new ObjectMessageClient();
        if (this.localSystem instanceof ActorSystemDefaultForRemote) {
            ActorSystemDefaultForRemote r = (ActorSystemDefaultForRemote) localSystem;
            server.setLeaderThreads(r.getServerLeaderThreads());
            server.setWorkerThreads(r.getServerWorkerThreads());
            client.setThreads(r.getClientThreads());
        }
        server.setSerializer(serializer);
        client.setSerializer(serializer);
    }

    public void start(int port) {
        start(ActorAddress.create("localhost", port));
    }

    public void start(ActorAddress.ActorAddressRemote serverAddress) {
        this.serverAddress = serverAddress;
        try {

            server.setHost(serverAddress.getHost())
                    .setPort(serverAddress.getPort());
            server.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ActorAddress.ActorAddressRemote getServerAddress() {
        return serverAddress;
    }

    @Override
    public void execute(Runnable task) {
        localSystem.execute(task);
    }

    @Override
    public void send(Message<?> message) {
        ActorRef target = message.getTarget();
        if (target instanceof ActorRefRemote) {
            ActorAddress addr = ((ActorRefRemote) target).getAddress();
            ConnectionActor a = connectionMap.computeIfAbsent(addr.getKey(), k -> createConnection(addr));
            if (a != null) {
                a.tell(message, null);
            } else {
                localSystem.sendDeadLetter(message);
            }
        } else {
            localSystem.send(message);
        }
    }

    protected ConnectionActor createConnection(ActorAddress addr) {
        if (addr instanceof ActorAddress.ActorAddressRemote) {
            try {
                return new ConnectionActor(localSystem, this, (ActorAddress.ActorAddressRemote) addr);
            } catch (InterruptedException ex) {
                //TODO report
                ex.printStackTrace();
                return null;
            }
        } else {
            return null;
        }
    }

    public ByteBuffer serialize(Message<?> message) {
        ByteBufferOutput output = new ByteBufferOutput();
        serializer.writeObject(output, message);
        output.close();
        return output.getByteBuffer();
    }

    public Kryo getSerializer() {
        return serializer;
    }

    public ObjectMessageServer getServer() {
        return server;
    }

    public ObjectMessageClient getClient() {
        return client;
    }

    public void receive(Object msg) {
        if (msg instanceof Message<?>) {
            Message<?> m = (Message)  msg;
            localSystem.send(new Message<>(
                    localize(m.getTarget()),
                    m.getSender(),
                    m.getData()));
        } else {
            //error
            System.err.println("receive unintended object: " + msg);
        }
    }

    public ActorRef localize(ActorRef ref) {
        if (ref instanceof ActorRefLocalNamed) {
            return ref;
        } else if (ref instanceof ActorRefRemote) {
            ActorAddress addr = ((ActorRefRemote) ref).getAddress();
            String localName = null;
            if (addr instanceof ActorAddress.ActorAddressRemoteActor) {
                localName = ((ActorAddress.ActorAddressRemoteActor) addr).getActorName();
            }
            return new ActorRefLocalNamed(this, localName);
        } else {
            return ref;
        }
    }



    public void register(Actor actor) {
        localSystem.register(actor);
    }


    /**
     * default threads:
     * <ul>
     *    <li>procs : availableProcessors()</li>
     *    <li>systemThreads : procs / 2 //{@link ActorSystemDefault#threads}</li>
     *    <li>server.leader : 1         // {@link ObjectMessageServer#setLeaderThreads(int)} </li>
     *    <li>server.worker : procs / 2 // {@link ObjectMessageServer#setWorkerThreads(int)} (EventLoopGroup)} </li>
     *    <li>client.group  : procs / 2 // {@link ObjectMessageClient#setGroup(EventLoopGroup)} (EventLoopGroup)}</li>
     * </ul>
     */
    public static class ActorSystemDefaultForRemote extends ActorSystemDefault {
        @Override
        protected void initSystemThreads() {
            this.threads = Runtime.getRuntime().availableProcessors() / 2;
        }

        public int getServerLeaderThreads() {
            return 1;
        }

        public int getServerWorkerThreads() {
            return getThreads();
        }

        public int getClientThreads() {
            return getThreads();
        }
    }

    public static class ConnectionActor extends Actor {
        protected ActorSystemRemote remoteSystem;
        protected ActorAddress address;
        protected ObjectMessageClient.ObjectMessageConnection connection;

        public ConnectionActor(ActorSystem system, ActorSystemRemote remoteSystem, ActorAddress.ActorAddressRemote address)
            throws InterruptedException {
            super(system);
            this.remoteSystem = remoteSystem;
            this.address = address;
            connection = remoteSystem.getClient().connect()
                    .setHost(address.getHost())
                    .setPort(address.getPort())
                    .open();
        }

        @Override
        protected void processMessage(Message<?> message) {
            send((Message) message.getData());
        }

        public void send(Message<?> message) {
            connection.write(message);
        }
    }
}
