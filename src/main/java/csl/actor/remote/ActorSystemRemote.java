package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.*;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.SocketChannelConfig;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ActorSystemRemote implements ActorSystem, KryoBuilder.SerializerFactory {
    protected ActorSystemDefault localSystem;

    protected ActorAddress.ActorAddressRemote serverAddress;

    protected ObjectMessageServer server;
    protected ObjectMessageClient client;

    protected MessageDeliveringActor deliverer;
    protected Map<ActorAddress, ConnectionActor> connectionMap;

    protected Function<ActorSystem, Kryo> kryoFactory;
    protected KryoBuilder.SerializerFunction serializer;
    protected volatile boolean closeAfterOtherConnectionsClosed;

    public static boolean debugLog = System.getProperty("csl.actor.debug", "false").equals("true");
    public static int debugLogColor = ActorSystem.systemPropertyColor("csl.actor.debug.color", 124);

    public static boolean debugLogMsg = System.getProperty("csl.actor.debugMsg", "false").equals("true");
    public static int debugLogMsgColorSend = ActorSystem.systemPropertyColor("csl.actor.debugMsg.color.send", 126);
    public static int debugLogMsgColorConnect = ActorSystem.systemPropertyColor("csl.actor.debugMsg.color.connect", 161);
    public static int debugLogMsgColorDeliver = ActorSystem.systemPropertyColor("csl.actor.debugMsg.color.deliver", 160);

    public ActorSystemRemote() {
        this(new ActorSystemDefaultForRemote(), KryoBuilder.builder());
    }

    public ActorSystemRemote(ActorSystemDefault localSystem, Function<ActorSystem, Kryo> kryoFactory) {
        this.localSystem = localSystem;
        this.kryoFactory = kryoFactory;
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
        serializer = new KryoBuilder.SerializerPoolDefault(this);
    }

    public Kryo createSerializer() {
        return kryoFactory.apply(this);
    }

    protected void initServerAndClient() {
        logDebug("initServerAndClient: %s", this);
        deliverer = initMessageDeliveringActor();
        server = initObjectMessageServer();
        server.setReceiver(this::receive);
        client = initObjectMessageClient();
        if (this.localSystem instanceof ActorSystemDefaultForRemote) {
            ActorSystemDefaultForRemote r = (ActorSystemDefaultForRemote) localSystem;
            server.setLeaderThreads(r.getServerLeaderThreads());
            server.setWorkerThreads(r.getServerWorkerThreads());
            client.setThreads(r.getClientThreads());
        }
        server.setSerializer(getSerializer());
        client.setSerializer(getSerializer());
        initConnectionCloseActor(); //register the actor for handling closing connections
    }

    protected MessageDeliveringActor initMessageDeliveringActor() {
        return new MessageDeliveringActor(this);
    }

    protected ObjectMessageServer initObjectMessageServer() {
        return new ObjectMessageServer(getLogger());
    }

    protected ObjectMessageClient initObjectMessageClient() {
        return new ObjectMessageClient(getLogger());
    }

    protected ConnectionCloseActor initConnectionCloseActor() {
        return new ConnectionCloseActor(this);
    }

    /** @return implementation field getter */
    public ActorSystemDefault getLocalSystem() {
        return localSystem;
    }

    /** @return implementation field getter */
    public Function<ActorSystem, Kryo> getKryoFactory() {
        return kryoFactory;
    }

    /** @return implementation field getter */
    public Map<ActorAddress, ConnectionActor> getConnectionMap() {
        return connectionMap;
    }

    public void start(int port) {
        start(ActorAddress.get("0.0.0.0", port));
    }

    public void startLocalhost(int port) {
        start(ActorAddress.get("localhost", port)); //limited to local
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

    public ActorSystemRemote startWithoutWaitLocalhost(int port) {
        startWithoutWait(ActorAddress.get("localhost", port));
        return this;
    }

    public ActorSystemRemote startWithoutWait(int port) {
        startWithoutWait(ActorAddress.get("0.0.0.0", port)); //limited to local
        return this;
    }

    public void startWithoutWait(ActorAddress.ActorAddressRemote serverAddress) {
        setServerAddress(serverAddress);
        try {
            logDebug("startWithoutWait: %s", this);
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
            ActorAddress addrActor = ((ActorRefRemote) target).getAddress();
            ActorAddress addrHost = addrActor.getHostAddress();
            ActorAddress local = getServerAddress();
            if (local != null && local.equals(addrHost)) { //localhost
                localSystem.send(message.renewTarget(localize(target)));
            } else {
                ActorAddress addr = getAddressForConnection(addrActor);
                getLogger().log(debugLogMsg, debugLogMsgColorSend, "%s: client tell <%s> to remote %s", this,
                        message.getData(), addrActor);
                ConnectionActor a = connectionMap.computeIfAbsent(addr, k -> createConnection(addr));
                if (a != null) {
                    a.tell(message);
                } else {
                    localSystem.sendDeadLetter(message);
                }
            }
        } else {
            localSystem.send(message);
        }
    }

    protected ActorAddress getAddressForConnection(ActorAddress target) {
        return target;
    }

    protected ConnectionActor createConnection(ActorAddress addr) {
        if (addr instanceof ActorAddress.ActorAddressRemote) {
            try {
                return new ConnectionActor(localSystem, this, (ActorAddress.ActorAddressRemote) addr);
            } catch (InterruptedException ex) {
                getLogger().log(true, debugLogColor, ex, "createConnection: %s", addr);
                return null;
            }
        } else {
            return null;
        }
    }


    public void logDebug(String msg, Object... args) {
        getLogger().log(debugLog, debugLogColor, msg, args);
    }

    public KryoBuilder.SerializerFunction getSerializer() {
        return serializer;
    }

    public ObjectMessageServer getServer() {
        return server;
    }

    public ObjectMessageClient getClient() {
        return client;
    }

    public int receive(Object msg) {
        deliverer.deliver(msg);
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
            return addr.toLocal(this);
        } else {
            return ref;
        }
    }


    @Override
    public void register(Actor actor) {
        localSystem.register(actor);
    }

    @Override
    public void unregister(Actor actor) {
        localSystem.unregister(actor);
    }

    @Override
    public Actor resolveActorLocalNamed(ActorRefLocalNamed ref) {
        return localSystem.resolveActorLocalNamed(ref);
    }

    @Override
    public void close() {
        try {
            logDebug("%s: close, connections=%,d", this, connectionMap.size());
            server.close();
            new ArrayList<>(connectionMap.values())
                    .forEach(ConnectionActor::notifyAndClose);
            client.close();
        } finally {
            localSystem.close();
        }
    }

    @Override
    public void awaitClose(long time, TimeUnit unit) throws InterruptedException {
        localSystem.awaitClose(time, unit);
    }

    @Override
    public String toString() {
        return toStringSystemName() + "@" + Integer.toHexString(System.identityHashCode(this)) +
                "(" + localSystem + ", " + serverAddress + ")";
    }

    public String toStringSystemName() {
        return "remote";
    }

    public void connectionClosed(ConnectionActor ca) {
        connectionMap.remove(ca.getAddress(), ca);
        checkClose();
    }

    public void closeAfterOtherConnectionsClosed() {
        closeAfterOtherConnectionsClosed = true;
        checkClose();
    }

    public void checkClose() {
        if (connectionMap.isEmpty() && closeAfterOtherConnectionsClosed) {
            ExecutorService service = localSystem.getExecutorService();
            service.shutdown();
            try {
                service.awaitTermination(3_000, TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                logDebug("%s: awaiting termination: %s", this, ex);
            }
            close();
        }
    }

    @Override
    public int getThreads() {
        return localSystem.getThreads();
    }

    @Override
    public ScheduledExecutorService getScheduledExecutor() {
        return localSystem.getScheduledExecutor();
    }

    @Override
    public SystemLogger getLogger() {
        return localSystem.getLogger();
    }

    public MessageDeliveringActor getDeliverer() {
        return deliverer;
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
            return 4;
        }

        public int getServerWorkerThreads() {
            return getThreads() * 20;
        }

        public int getClientThreads() {
            return getThreads() * 10;
        }
    }

    public boolean isSpecialMessage(Message<?> message) {
        if (message != null) {
            return isSpecialMessageData(message.getData());
        } else {
            return false;
        }
    }

    public boolean isSpecialMessageData(Object data) {
        return data instanceof SpecialMessage ||
                data instanceof ConnectionCloseNotice ||
                data instanceof CallableMessage ||
                data instanceof CallableMessage.CallableFailure ||
                data instanceof CallableMessage.CallableResponse<?> ||
                data instanceof CallableMessage.CallableResponseVoid;
    }

    public interface SpecialMessage {}

    public static class ConnectionActor extends Actor {
        protected ActorSystemRemote remoteSystem;
        protected ActorAddress address;
        protected ObjectMessageClient.ObjectMessageConnection connection;

        protected int count;

        protected long recordSendMessages;
        protected Duration recordSendMessageTime;

        public ConnectionActor(ActorSystem system, ActorSystemRemote remoteSystem, ActorAddress.ActorAddressRemote address)
            throws InterruptedException {
            super(system);
            this.remoteSystem = remoteSystem;
            this.address = address;
            connection = remoteSystem.getClient().connect()
                    .setHost(address.getHost())
                    .setPort(address.getPort())
                    .open();
            remoteSystem.logDebug("%s connection: %s", this, connection);
        }

        @Override
        public void processMessage(Message<?> message) {
            send((Message<?>) message.getData());
        }

        public void send(Message<?> message) {
            Instant start = Instant.now();
            try {
                if (remoteSystem.isSpecialMessage(message)) {
                    writeSpecial(message);
                } else {
                    if (mailbox.isEmpty()) {
                        writeSingleMessage(message);
                    } else {
                        int maxBundle = getMaxBundle();
                        List<Object> messageBundle = new ArrayList<>(Math.max(0, maxBundle));
                        messageBundle.add(message);
                        int i = 1;
                        while (i < maxBundle) {
                            Message<?> msg = mailbox.poll();
                            if (remoteSystem.isSpecialMessage(msg)) {
                                write(messageBundle);
                                messageBundle.clear();

                                writeSpecial(msg);
                                break;
                            } else if (msg != null) {
                                messageBundle.add((Message<?>) msg.getData()); //the data of the msg is a Message for remote actor
                                ++i;
                            } else {
                                break;
                            }
                        }
                        write(messageBundle);
                    }
                }
            } finally {
                recordSendMessageTime = Duration.between(start, Instant.now());
            }
        }

        public int getMaxBundle() {
            return 30;
        }

        private void write(List<Object> messageBundle) {
            if (!messageBundle.isEmpty()) {
                writeNonEmpty(messageBundle);
            }
        }

        protected void writeSingleMessage(Message<?> message) {
            logMsg("%s write %s", this, message);
            connection.write(new TransferredMessage(count, message));
            ++count;
            ++recordSendMessages;
        }

        protected void writeNonEmpty(List<Object> messageBundle) {
            logMsg("%s write %,d messages: %s,...", this, messageBundle.size(), messageBundle.get(0));
            connection.write(new TransferredMessage(count, messageBundle));
            ++count;
            recordSendMessages += messageBundle.size();
        }

        protected void writeSpecial(Message<?> message) {
            logMsg("%s special", message);
            boolean close = (message.getData() instanceof ConnectionCloseNotice);
            int id = close ? ObjectMessageServer.RESULT_CLOSE : count;
            if (!close || connection.isOpen()) {
                writeSpecial(message, id);
            }
            ++count;
            ++recordSendMessages;
            if (close) {
                close();
            }
        }

        protected void writeSpecial(Message<?> message, int id) {
            connection.write(new TransferredMessage(id, message));
        }

        protected void logMsg(String fmt, Object... args) {
            remoteSystem.getLogger().log(debugLogMsg, debugLogMsgColorConnect, fmt, args);
        }

        public void notifyAndClose() {
            remoteSystem.logDebug("%s %s notifyAncClose -> %s", remoteSystem, this, address);
            tell(new Message<Object>(
                    ActorRefRemote.get(remoteSystem, address.getActor(NAME_CONNECTION_ACTOR)),
                    this,
                    new ConnectionCloseNotice(remoteSystem.getServerAddress())));
            try {
                Thread.sleep(3_000);
            } catch (Exception ex) {
                remoteSystem.getLogger().log(debugLog, debugLogColor, ex, "notifyAndClose wait");
            }
            close();
        }

        public void close() {
            if (connection.isOpen()) {
                logMsg("write empty for closing -> %s", address);
                connection.write(new TransferredMessage(ObjectMessageServer.RESULT_CLOSE, new ArrayList<>()));
            }
            connection.close();
            remoteSystem.connectionClosed(this);
        }

        public ActorAddress getAddress() {
            return address;
        }

        public int getCount() {
            return count;
        }

        public long getRecordSendMessages() {
            return recordSendMessages;
        }

        public Duration getRecordSendMessageTime() {
            return recordSendMessageTime;
        }

        /** @return implementation field getter */
        public ActorSystemRemote getRemoteSystem() {
            return remoteSystem;
        }

        public ObjectMessageClient.ObjectMessageConnection getConnection() {
            return connection;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this)) +
                    "(" + address + ")";
        }
    }

    public static final String NAME_CONNECTION_ACTOR = "$ConnectionClose";

    public static class ConnectionCloseNotice implements Serializable {
        public static final long serialVersionUID = 1L;
        protected ActorAddress address;

        public ConnectionCloseNotice(ActorAddress address) {
            this.address = address;
        }

        public ActorAddress getAddress() {
            return address;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + address + ")";
        }
    }

    public static class ConnectionCloseActor extends ActorDefault {
        public ConnectionCloseActor(ActorSystemRemote system) {
            super(system, NAME_CONNECTION_ACTOR);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(ConnectionCloseNotice.class, this::receive)
                    .build();
        }

        public void receive(ConnectionCloseNotice notice) {
            getSystem().logDebug("receive close-notice: %s", notice);
            ActorSystemRemote remote = getSystem();
            remote.getConnectionMap().values().stream()
                .filter(a -> a.getAddress().getHostAddress().equals(notice.getAddress().getHostAddress()))
                .forEach(a -> close(notice, a));
        }

        public void close(ConnectionCloseNotice notice, ConnectionActor a) {
            getSystem().logDebug("receive close-notice: %s -> close %s", notice, a);
            a.close();
        }

        @Override
        public ActorSystemRemote getSystem() {
            return (ActorSystemRemote) system;
        }
    }

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
        protected AtomicLong receiveMessages = new AtomicLong();

        public MessageDeliveringActor(ActorSystemRemote system) {
            super(system, MessageDeliveringActor.class.getName());
            this.remote = system;
        }

        @Override
        public void processMessage(Message<?> message) {
            deliver(message.getData());
        }

        public void deliver(Object msg) {
            if (msg instanceof TransferredMessage) {
                msg = ((TransferredMessage) msg).body;
            }
            if (msg instanceof List<?>) { //message bundle
                List<?> msgs = (List<?>) msg;
                logMsg("%s receive-remote: messages %,d", this, msgs.size());
                int i = 0;
                for (Object elem : msgs) {
                    Message<?> msgElem = (Message<?>) elem;
                    logMsg("%s receive-remote: [%,d] %s", this, i, msgElem);
                    remote.getLocalSystem().send(msgElem.renewTarget(
                            remote.localize(msgElem.getTarget())));
                    ++i;
                    receiveMessages.incrementAndGet();
                }
            } else if (msg instanceof Message<?>) {
                Message<?> m = (Message<?>)  msg;
                logMsg("%s receive-remote: %s", this, m);
                remote.getLocalSystem().send(m.renewTarget(
                        remote.localize(m.getTarget())));
                receiveMessages.incrementAndGet();
            } else {
                logMsg("%s receive unintended object: %s", this, msg);
            }
        }

        protected void logMsg(String fmt, Object... args) {
            remote.getLogger().log(debugLogMsg, debugLogMsgColorDeliver, fmt, args);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this)) +
                    "(" + remote + ")";
        }

        public long getReceiveMessages() {
            return receiveMessages.get();
        }
    }

    public static class TransferredMessage implements Serializable {
        public static final long serialVersionUID = 1L;
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
