package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.util.Pool;
import csl.actor.*;
import io.netty.channel.EventLoopGroup;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

public class ActorSystemRemote implements ActorSystem {
    protected ActorSystemDefault localSystem;

    protected ActorAddress.ActorAddressRemote serverAddress;

    protected ObjectMessageServer server;
    protected ObjectMessageClient client;

    protected Map<Object, ConnectionActor> connectionMap;

    protected Function<ActorSystemRemote, Kryo> serializer;
    protected Pool<Kryo> serializerPool;

    public static boolean debugLog = System.getProperty("csl.actor.debug", "false").equals("true");

    public ActorSystemRemote() {
        this(new ActorSystemDefaultForRemote(), KryoBuilder.builder());
    }

    public ActorSystemRemote(ActorSystemDefault localSystem, Function<ActorSystemRemote, Kryo> serializer) {
        this.localSystem = localSystem;
        this.serializer = serializer;
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
        serializerPool = new Pool<Kryo>(true, false) {
            @Override
            protected Kryo create() {
                return createSerializer();
            }
        };
    }

    public Kryo createSerializer() {
        return serializer.apply(this);
    }

    protected void initServerAndClient() {
        log("initServerAndClient: %s", this);
        server = new ObjectMessageServer();
        server.setReceiver(this::receive);
        client = new ObjectMessageClient();
        if (this.localSystem instanceof ActorSystemDefaultForRemote) {
            ActorSystemDefaultForRemote r = (ActorSystemDefaultForRemote) localSystem;
            server.setLeaderThreads(r.getServerLeaderThreads());
            server.setWorkerThreads(r.getServerWorkerThreads());
            client.setThreads(r.getClientThreads());
        }
        server.setSerializer(getSerializer());
        client.setSerializer(getSerializer());
    }

    /** @return implementation field getter */
    public ActorSystemDefault getLocalSystem() {
        return localSystem;
    }

    /** @return implementation field getter */
    public Pool<Kryo> getSerializerPool() {
        return serializerPool;
    }

    /** @return implementation field getter */
    public Function<ActorSystemRemote, Kryo> getSerializerFunction() {
        return serializer;
    }

    /** @return implementation field getter */
    public Map<Object, ConnectionActor> getConnectionMap() {
        return connectionMap;
    }

    public void start(int port) {
        start(ActorAddress.get("localhost", port));
    }

    public void start(ActorAddress.ActorAddressRemote serverAddress) {
        setServerAddress(serverAddress);
        try {

            server.setHost(serverAddress.getHost())
                    .setPort(serverAddress.getPort());
            server.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ActorSystemRemote startWithoutWait(int port) {
        startWithoutWait(ActorAddress.get("localhost", port));
        return this;
    }

    public void startWithoutWait(ActorAddress.ActorAddressRemote serverAddress) {
        setServerAddress(serverAddress);
        try {

            server.setHost(serverAddress.getHost())
                    .setPort(serverAddress.getPort());
            server.startWithoutWait();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ActorAddress.ActorAddressRemote getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(ActorAddress.ActorAddressRemote serverAddress) {
        this.serverAddress = serverAddress;
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
            log("send to remote %s", addr);
            ConnectionActor a = connectionMap.computeIfAbsent(addr.getKey(), k -> createConnection(addr));
            if (a != null) {
                a.tell(message, null);
            } else {
                localSystem.sendDeadLetter(message);
            }
            log("send finish to remote %s", addr);
        } else {
            localSystem.send(message);
        }
    }

    protected ConnectionActor createConnection(ActorAddress addr) {
        if (addr instanceof ActorAddress.ActorAddressRemote) {
            try {
                log("createConnection: %s", addr);
                return new ConnectionActor(localSystem, this, (ActorAddress.ActorAddressRemote) addr);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                return null;
            }
        } else {
            return null;
        }
    }

    public static void log(String msg, Object... args) {
        if (debugLog) {
            System.err.println("\033[38;5;33m" + String.format(msg, args) + "\033[0m");
        }
    }


    public ByteBuffer serialize(Message<?> message) {
        ByteBufferOutput output = new ByteBufferOutput();
        serializerPool.obtain().writeObject(output, message);
        output.close();
        return output.getByteBuffer();
    }

    public Supplier<Kryo> getSerializer() {
        return serializerPool::obtain;
    }

    public ObjectMessageServer getServer() {
        return server;
    }

    public ObjectMessageClient getClient() {
        return client;
    }

    public void receive(Object msg) {
        if (msg instanceof Message<?>) {
            Message<?> m = (Message<?>)  msg;
            log("receive-remote: %s", m);
            localSystem.send(new Message<>(
                    localize(m.getTarget()),
                    m.getSender(),
                    m.getData()));
        } else {
            log("receive unintended object: %s", msg);
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



    @Override
    public void register(Actor actor) {
        localSystem.register(actor);
    }

    @Override
    public void unregister(String actorName) {
        localSystem.unregister(actorName);
    }

    @Override
    public Actor resolveActorLocalNamed(ActorRefLocalNamed ref) {
        return localSystem.resolveActorLocalNamed(ref);
    }

    @Override
    public void close() {
        try {
            log("%s: close", this);
            server.close();
            new ArrayList<>(connectionMap.values())
                    .forEach(ConnectionActor::close);
            client.close();
        } finally {
            localSystem.close();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this)) +
                "(" + localSystem + ", address=" + serverAddress + ")";
    }

    public void connectionClosed(ConnectionActor ca) {
        connectionMap.remove(ca.getAddress().getKey(), ca);
    }

    @Override
    public int getThreads() {
        return localSystem.getThreads();
    }

    @Override
    public ScheduledExecutorService getScheduledExecutor() {
        return localSystem.getScheduledExecutor();
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
            send((Message<?>) message.getData());
        }

        public void send(Message<?> message) {
            if (message.getData() instanceof ConnectionClose) {
                log("%s close", message);
                close();
            } else {
                log("%s write %s", this, message);
                connection.write(message);
                log("%s after write", this);
            }
        }

        public void close() {
            connection.close();
            remoteSystem.connectionClosed(this);
        }

        public ActorAddress getAddress() {
            return address;
        }

        /** @return implementation field getter */
        public ActorSystemRemote getRemoteSystem() {
            return remoteSystem;
        }

        /** @return implementation field getter */
        public ObjectMessageClient.ObjectMessageConnection getConnection() {
            return connection;
        }
    }

    public static class ConnectionClose implements Serializable { }
}
