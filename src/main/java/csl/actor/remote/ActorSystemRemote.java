package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.util.Pool;
import csl.actor.*;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.SocketChannelConfig;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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

    protected MessageDeliveringActor deliverer;
    protected Map<Object, ConnectionActor> connectionMap;

    protected Function<ActorSystem, Kryo> serializer;
    protected Pool<Kryo> serializerPool;

    public static boolean debugLog = System.getProperty("csl.actor.debug", "false").equals("true");
    public static int debugLogColor = Integer.parseInt(System.getProperty("csl.actor.debug.color", "124"));

    public ActorSystemRemote() {
        this(new ActorSystemDefaultForRemote(), KryoBuilder.builder());
    }

    public ActorSystemRemote(ActorSystemDefault localSystem, Function<ActorSystem, Kryo> serializer) {
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
        deliverer = new MessageDeliveringActor(this);
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
    public Function<ActorSystem, Kryo> getSerializerFunction() {
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
            log("startWithoutWait: %s", this);
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
            ActorAddress addr = ((ActorRefRemote) target).getAddress().getHostAddress();
            log(19, "client tell to remote %s", addr);
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
                log(19, "client createConnection: %s", addr);
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
        log(debugLogColor, msg, args);
    }

    public static void log(int n, String msg, Object... args) {
        if (debugLog) {
            System.err.println("\033[38;5;" + n + "m" + String.format(msg, args) + "\033[0m");
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

    public int receive(Object msg) {
        deliverer.tell(msg, null);
        if (msg instanceof TransferredMessage) {
            return ((TransferredMessage) msg).id;
        } else {
            return 200;
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

        protected int count;

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
                log(20, "%s close", message);
                close();
            } else {
                if (mailbox.isEmpty()) {
                    log(20, "%s write %s", this, message);
                    connection.write(new TransferredMessage(count, message));
                } else {
                    int maxBundle = 30;
                    List<Object> messageBundle = new ArrayList<>(maxBundle);
                    messageBundle.add(message);
                    int i = 1;
                    while (i < maxBundle) {
                        Message<?> msg = mailbox.poll();
                        if (msg != null) {
                            messageBundle.add((Message<?>) msg.getData()); //the data of the msg is a Message for remote actor
                            ++i;
                        } else {
                            break;
                        }
                    }
                    log(20, "%s write %,d messages: %s,...", this, messageBundle.size(), message);
                    connection.write(new TransferredMessage(count, messageBundle));
                }
            }
            ++count;
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

        @Override
        public String toString() {
            return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this)) +
                    "(" + address + ")";
        }
    }

    public static class ConnectionClose implements Serializable { }

    public static void settingsSocketChannel(SocketChannel ch) {
        SocketChannelConfig conf = ch.config();
        //for UDP conf.setRecvByteBufAllocator(new FixedRecvByteBufAllocator(96_000_000));
        /*
        conf.setSendBufferSize(96_000_000);
        conf.setReceiveBufferSize(96_000_000);
        conf.setConnectTimeoutMillis(15_000);*/
    }

    public static class MessageDeliveringActor extends Actor {
        protected ActorSystemRemote remote;

        public MessageDeliveringActor(ActorSystemRemote system) {
            super(system);
            this.remote = system;
        }

        @Override
        protected void processMessage(Message<?> message) {
            Object msg = message.getData();
            if (msg instanceof TransferredMessage) {
                msg = ((TransferredMessage) msg).body;
            }
            if (msg instanceof List<?>) { //message bundle
                List<?> msgs = (List<?>) msg;
                log(163, "server receive-remote: messages %,d", msgs.size());
                int i = 0;
                for (Object elem : msgs) {
                    Message<?> msgElem = (Message<?>) elem;
                    log(163, "server receive-remote: [%,d] %s", i, msgElem);
                    remote.getLocalSystem().send(new Message<>(
                            remote.localize(msgElem.getTarget()),
                            msgElem.getSender(),
                            msgElem.getData()));
                    ++i;
                }
            } else if (msg instanceof Message<?>) {
                Message<?> m = (Message<?>)  msg;
                log(163, "server receive-remote: %s", m);
                remote.getLocalSystem().send(new Message<>(
                        remote.localize(m.getTarget()),
                        m.getSender(),
                        m.getData()));
            } else {
                log(163, "server receive unintended object: %s", msg);
            }
        }
    }

    public static class TransferredMessage implements Serializable {
        public int id;
        public Object body;

        public TransferredMessage(int id, Object body) {
            this.id = id;
            this.body = body;
        }

        @Override
        public String toString() {
            return "TransferredMessage{" +
                    "id=" + id +
                    ", body=" + body +
                    '}';
        }
    }

    public static final boolean CLOSE_EACH_WRITE = false;
}
