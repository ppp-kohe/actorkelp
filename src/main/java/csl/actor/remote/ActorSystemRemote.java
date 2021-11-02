package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.SocketChannelConfig;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ActorSystemRemote implements ActorSystem, KryoBuilder.SerializerFactory {
    protected ActorSystemDefault localSystem;

    protected ActorAddress.ActorAddressRemote serverAddress;

    protected ObjectMessageServer server;
    protected ObjectMessageClient client;

    protected MessageDeliveringActor deliverer;
    protected Map<ActorAddress, ConnectionActor> connectionMap;
    protected Map<ActorAddress, LinkedList<ConnectionActor>> connectionHostMap;
    protected AtomicInteger createdConnections = new AtomicInteger();

    protected Function<ActorSystem, Kryo> kryoFactory;
    protected KryoBuilder.SerializerFunction serializer;
    protected volatile boolean closeAfterOtherConnectionsClosed;

    public static boolean debugLog = System.getProperty("csl.actor.debug", "false").equals("true");
    public static int debugLogColor = ActorSystem.systemPropertyColor("csl.actor.debug.color", 124);

    public static boolean debugLogMsg = System.getProperty("csl.actor.debugMsg", "false").equals("true");
    public static int debugLogMsgColorSend = ActorSystem.systemPropertyColor("csl.actor.debugMsg.color.send", 126);
    public static int debugLogMsgColorConnect = ActorSystem.systemPropertyColor("csl.actor.debugMsg.color.connect", 161);
    public static int debugLogMsgColorDeliver = ActorSystem.systemPropertyColor("csl.actor.debugMsg.color.deliver", 160);

    protected List<Function<ActorAddress,ActorAddress>> addressNormalizers;

    protected AtomicInteger clock = new AtomicInteger();
    protected Map<ActorRefRemote, Integer> clockTable = Collections.synchronizedMap(new WeakHashMap<>());

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
        initAddressNormalizers();
        initServerAndClient();
    }

    protected void initConnectionMap() {
        connectionMap = new ConcurrentHashMap<>();
        connectionHostMap = new ConcurrentHashMap<>();
    }

    protected void initSerializer() {
        serializer = new KryoBuilder.SerializerPoolDefault(this);
    }

    protected void initAddressNormalizers() {
        addressNormalizers = new ArrayList<>();
        addressNormalizers.add(new NormalizerLocalhost());
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
        start(serverAddress, serverAddress);
    }
    public void start(ActorAddress.ActorAddressRemote serverAddress,
                      ActorAddress.ActorAddressRemote serverAddressForBind) {
        setServerAddress(serverAddress);
        try {
            server.setHost(serverAddressForBind.getHost())
                    .setPort(serverAddressForBind.getPort());
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
        startWithoutWait(serverAddress, serverAddress);
    }
    public void startWithoutWait(ActorAddress.ActorAddressRemote serverAddress,
                                 ActorAddress.ActorAddressRemote serverAddressForBind) {
        setServerAddress(serverAddress);
        try {
            logDebug("startWithoutWait: %s bind:%s", this, serverAddressForBind);
            server.setHost(serverAddressForBind.getHost())
                    .setPort(serverAddressForBind.getPort());
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
                Message.MessageDataClock<Message<?>> clockMessage = createClockMessageForSend(message);
                getLogger().log(debugLogMsg, debugLogMsgColorSend, "%s: client tell <%d: %s> to remote %s", this,
                        clockMessage.clock, message.getData(), addrActor);
                ConnectionActor a = connection(addr, addrHost);
                if (a != null) {
                    a.tell(clockMessage); //the network message sending is processed as regular message by ConnectionActor with localSystem
                } else {
                    localSystem.sendDeadLetter(message);
                }
            }
        } else {
            localSystem.send(message);
        }
    }

    public void sendClockedMessage(Message.MessageDataClock<Message<?>> message) { //internal use from ConnectionActor: simple version of send(m)
        ActorRef target = message.body.getTarget();
        if (target instanceof ActorRefRemote) {
            ActorAddress addrActor = ((ActorRefRemote) target).getAddress();
            ActorAddress addrHost = addrActor.getHostAddress();
            ActorAddress addr = getAddressForConnection(addrActor);
            ConnectionActor a = connection(addr, addrHost);
            if (a != null) {
                a.tell(message);
            } else {
                localSystem.sendDeadLetter(message.body);
            }
        } else {
            send(message.body);
        }
    }

    protected synchronized ConnectionActor connection(ActorAddress addr, ActorAddress addrHost) {
        ConnectionActor exa = connectionMap.get(addr);
        if (exa == null) {
            LinkedList<ConnectionActor> hostCons = connectionHostMap.computeIfAbsent(addrHost, k -> new LinkedList<>());
            //it needs to create a new connection
            if (createdConnections.get() >= getConnectionMax()) {
                //reuse existing
                if (hostCons.isEmpty()) {
                    setupConnectionMapRestructure();
                }
                exa = hostCons.removeFirst();
                hostCons.addLast(exa); //rotate
                connectionMap.put(addr, exa);
            } else {
                exa = createConnectionAndIncrement(addrHost);
                connectionMap.put(addr, exa);
                hostCons.add(exa);
            }
        }
        return exa;
    }

    protected int getConnectionMax() {
        return 128;
    }

    protected void setupConnectionMapRestructure() {
        int n = Math.max(1, createdConnections.get() / connectionHostMap.size());
        for (Map.Entry<ActorAddress, LinkedList<ConnectionActor>> e : connectionHostMap.entrySet().stream()
                .sorted(Comparator.comparing((Map.Entry<ActorAddress, LinkedList<ConnectionActor>> a) -> a.getValue().size()).reversed())
                .collect(Collectors.toList())) {
            LinkedList<ConnectionActor> cons = e.getValue();
            int diff = cons.size() - n;
            while (diff > 0) {
                ConnectionActor ex = cons.removeFirst();
                ex.close(); //removed from connectionMap. remaining messages are re-sent by sendClockMessage(m). it might change the order of messages
                --diff;
            }
            while (diff < 0) {
                ConnectionActor a = createConnectionAndIncrement(e.getKey());
                cons.add(a);
                ++diff;
            }
        }
    }

    protected ConnectionActor createConnectionAndIncrement(ActorAddress addrHost) {
        ConnectionActor a = createConnection(addrHost);
        createdConnections.incrementAndGet();
        return a;
    }

    protected Message.MessageDataClock<Message<?>> createClockMessageForSend(Message<?> message) {
        Message.MessageDataClock<Message<?>> clockMessage = new Message.MessageDataClock<>(getClockNext(), message);
        getClockTable().compute((ActorRefRemote) message.getTarget(), (_t, last) -> last == null ?
                clockMessage.clock : Math.max(last, clockMessage.clock));
        return clockMessage;
    }

    public int getClockNext() {
        return clock.getAndIncrement();
    }

    public int getClockCurrent() {
        return clock.get();
    }

    /**
     * @return {remoteActor -&gt; sentClock}
     */
    public Map<ActorRefRemote, Integer> getClockTable() {
        return clockTable;
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
        if (msg instanceof MessageDataTransferred) {
            return ((MessageDataTransferred) msg).id;
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
    public boolean register(Actor actor) {
        return localSystem.register(actor);
    }

    @Override
    public boolean unregister(Actor actor) {
        return localSystem.unregister(actor);
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

    public synchronized void connectionClosed(ConnectionActor ca) {
        connectionMap.entrySet()
                .removeIf(e -> e.getValue().equals(ca));
        createdConnections.decrementAndGet();
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

    public static class ConnectionActor extends Actor {
        protected ActorSystemRemote remoteSystem;
        protected ActorAddress address;
        protected ObjectMessageClient.ObjectMessageConnection connection;

        protected int count;

        protected long recordSendMessages;
        protected Duration recordSendMessageTime;
        protected AtomicBoolean closed = new AtomicBoolean();

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
        public void offer(Message<?> message) {
            if (getRemoteSystem().isSpecialMessage(message)) {
                specialMailbox.offer(message); //TODO clock order is broken?
            } else {
                mailbox.offer(message); //no delayedMailbox: any messages are in order, sorted by clock
            }
        }


        @Override
        protected void processMessageSystemClock(Message.MessageDataClock<?> message) {
            if (message.body instanceof Message<?>) {
                processMessageClocked(message);
            } else {
                super.processMessageSystemClock(message);
            }
        }

        @SuppressWarnings("unchecked")
        protected void processMessageClocked(Message.MessageDataClock<?> message) {
            if (closed.get()) {
                resendClockMessage((Message.MessageDataClock<Message<?>>) message);
            } else {
                try {
                    send((Message.MessageDataClock<Message<?>>) message);
                } catch (ConnectionSendException sendEx) {
                    sendEx.getMessages().forEach(this::resendClockMessage);
                }
            }
        }

        protected void resendClockMessage(Message.MessageDataClock<Message<?>> message) {
            remoteSystem.sendClockedMessage(message);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void processMessage(Message<?> message) {
            processMessageClocked((Message.MessageDataClock<Message<?>>) message.getData());
        }

        @SuppressWarnings("unchecked")
        @Override
        public void processMessageSpecial(Message<?> message) {
            Instant start = Instant.now();
            try {
                writeSpecial((Message.MessageDataClock<Message<?>>) message.getData());
            } finally {
                recordSendMessageTime = Duration.between(start, Instant.now());
            }
        }

        @SuppressWarnings("unchecked")
        public void send(Message.MessageDataClock<Message<?>> clockMessage) {
            Instant start = Instant.now();
            try {
                if (remoteSystem.isSpecialMessage(clockMessage.body)) {
                    writeSpecial(clockMessage);
                } else {
                    if (isEmptyMailboxAll()) {
                        writeSingleMessage(clockMessage);
                    } else {
                        int maxBundle = getMaxBundle();
                        List<Message.MessageDataClock<Message<?>>> messageBundle = new ArrayList<>(Math.max(0, maxBundle));
                        messageBundle.add(clockMessage);
                        int i = 1;
                        while (i < maxBundle) {
                            Message<?> msg = pollNextMessage();
                            Message.MessageDataClock<Message<?>> clockMsg = (Message.MessageDataClock<Message<?>>) (msg == null ? null : msg.getData());
                            if (clockMsg != null && remoteSystem.isSpecialMessage(clockMsg.body)) {
                                write(messageBundle);
                                messageBundle.clear();

                                writeSpecial(clockMsg);
                                break;
                            } else if (msg != null) {
                                messageBundle.add(clockMsg); //the data of the msg is a Message for remote actor
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

        public Message<?> pollNextMessage() {
            Message<?> msg = mailbox.poll();
            if (msg != null) {
                return msg;
            } else {
                Mailbox dm = delayedMailbox;
                return dm == null ? null : dm.poll(); //never, for confirmation
            }
        }

        public int getMaxBundle() {
            return 30;
        }

        private void write(List<? extends Message.MessageDataClock<Message<?>>> messageBundle) {
            if (!messageBundle.isEmpty()) {
                writeNonEmpty(messageBundle);
            }
        }

        protected void writeSingleMessage(Message.MessageDataClock<Message<?>> message) {
            logMsg("%s write %s", this, message);
            try {
                connection.write(new MessageDataTransferred(currentCount(), remoteSystem.getServerAddress(), message));
            } catch (Exception ex) {
                throw new ConnectionSendException(ex, message);
            }
            ++count;
            ++recordSendMessages;
        }

        protected int currentCount() {
            return 0x0FFF_FFFF & count;
        }

        protected void writeNonEmpty(List<? extends Message.MessageDataClock<Message<?>>> messageBundle) {
            logMsg("%s write %,d messages: %s,...", this, messageBundle.size(), messageBundle.get(0));
            try {
                connection.write(new MessageDataTransferred(currentCount(), remoteSystem.getServerAddress(), messageBundle));
            } catch (Exception ex) {
                throw new ConnectionSendException(ex, messageBundle);
            }
            ++count;
            recordSendMessages += messageBundle.size();
        }

        protected void writeSpecial(Message.MessageDataClock<Message<?>> message) {
            logMsg("%s special", message);
            boolean close = (message.body.getData() instanceof ConnectionCloseNotice);
            int id = close ? ObjectMessageServer.RESULT_CLOSE : currentCount();
            if (!close || connection.isOpen()) {
                writeSpecial(message, id);
            }
            ++count;
            ++recordSendMessages;
            if (close) {
                close();
            }
        }

        protected void writeSpecial(Message.MessageDataClock<Message<?>> message, int id) {
            try {
                connection.write(new MessageDataTransferred(id, remoteSystem.getServerAddress(), message));
            } catch (Exception ex) {
                throw new ConnectionSendException(ex, message);
            }
        }

        protected void logMsg(String fmt, Object... args) {
            remoteSystem.getLogger().log(debugLogMsg, debugLogMsgColorConnect, fmt, args);
        }

        public void notifyAndClose() {
            remoteSystem.logDebug("%s %s notifyAncClose -> %s", remoteSystem, this, address);
            tell(new Message.MessageDataClock<>(remoteSystem.getClockNext(), new Message<Object>(
                    ActorRefRemote.get(remoteSystem, address.getActor(NAME_CONNECTION_ACTOR)),
                    new ConnectionCloseNotice(remoteSystem.getServerAddress()))));
            try {
                Thread.sleep(50);
            } catch (Exception ex) {
                remoteSystem.getLogger().log(debugLog, debugLogColor, ex, "notifyAndClose wait");
            }
            close();
        }

        public void close() {
            closed.set(true);
            if (connection.isOpen()) {
                logMsg("write empty for closing -> %s", address);
                connection.write(new MessageDataTransferred(ObjectMessageServer.RESULT_CLOSE,remoteSystem.getServerAddress(), new ArrayList<>()));
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

    public static class ConnectionSendException extends RuntimeException {
        protected List<? extends Message.MessageDataClock<Message<?>>> messages;

        public ConnectionSendException(Throwable cause, List<? extends Message.MessageDataClock<Message<?>>> messages) {
            super(cause);
            this.messages = messages;
        }

        public ConnectionSendException(Throwable cause, Message.MessageDataClock<Message<?>> message) {
            this(cause, Collections.singletonList(message));
        }

        public List<? extends Message.MessageDataClock<Message<?>>> getMessages() {
            return messages;
        }
    }

    public static final String NAME_CONNECTION_ACTOR = Actor.NAME_SYSTEM_SEPARATOR + "ConnectionClose";

    public static class ConnectionCloseNotice implements Serializable, Message.MessageDataSpecial {
        public static final long serialVersionUID = 1L;
        public ActorAddress address;

        public ConnectionCloseNotice() {}

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
        protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilder builder) {
            return builder.match(ConnectionCloseNotice.class, this::receive);
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
        protected void processMessageSystemClock(Message.MessageDataClock<?> message) {
            deliver(message);
        }

        @Override
        public void processMessage(Message<?> message) {
            deliver(message.getData());
        }

        @SuppressWarnings("unchecked")
        public void deliver(Object msg) {
            ActorAddress.ActorAddressRemote fromAddr = null;
            if (msg instanceof MessageDataTransferred) {
                MessageDataTransferred tMsg = (MessageDataTransferred) msg;
                msg = tMsg.body;
                fromAddr = tMsg.fromAddress; //serverAddress
            }
            int clock = -1;
            if (msg instanceof Message.MessageDataClock) {
                Message.MessageDataClock<?> cMsg = (Message.MessageDataClock<?>) msg;
                clock = cMsg.clock;
                msg = cMsg.body;
            }
            if (msg instanceof List<?>) { //message bundle: all sub-messages are clocked
                List<?> msgs = (List<?>) msg;
                logMsg("%s receive-remote: messages %,d", this, msgs.size());
                int i = 0;
                for (Object elem : msgs) {
                    Message.MessageDataClock<Message<?>> cMsg = (Message.MessageDataClock<Message<?>>) elem;
                    Message<?> msgElem = cMsg.body;
                    logMsg("%s receive-remote: [%,d] <%d:%s>", this, i, cMsg.clock, msgElem);
                    sendLocal(cMsg.clock, fromAddr, msgElem);
                    ++i;
                    receiveMessages.incrementAndGet();
                }
            } else if (msg instanceof Message<?>) {
                Message<?> m = (Message<?>)  msg;
                logMsg("%s receive-remote: <%d:%s>", this, clock, m);
                sendLocal(clock, fromAddr, m);
                receiveMessages.incrementAndGet();
            } else {
                logMsg("%s receive unintended object: %s", this, msg);
            }
        }

        protected void sendLocal(int clock, ActorAddress.ActorAddressRemote fromAddr, Message<?> msg) {
            ActorSystem localSys = remote.getLocalSystem();
            ActorRef target = remote.localize(msg.getTarget());
            localSys.send(new Message<>(target, new Message.MessageDataClock<>(clock, fromAddr))); //send activation clock from the host: non-special
            localSys.send(msg.renewTarget(target));
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

    public static class MessageDataTransferred implements Serializable, Message.MessageDataHolder<Object>, KryoSerializable {
        public static final long serialVersionUID = 1L;
        public int id;
        public ActorAddress.ActorAddressRemote fromAddress;
        public Object body;

        public MessageDataTransferred() {}

        public MessageDataTransferred(int id, ActorAddress.ActorAddressRemote fromAddress, Object body) {
            this.id = id;
            this.fromAddress = fromAddress;
            this.body = body;
        }

        @Override
        public Object getData() {
            return body;
        }

        @Override
        public String toString() {
            return "TransferredMessage{" +
                    "id=" + id +
                    ",from=" + fromAddress +
                    ", body=" + body +
                    '}';
        }
        @Override
        public void write(Kryo kryo, Output output) {
            output.writeInt(id);
            kryo.writeClassAndObject(output, fromAddress);
            kryo.writeClassAndObject(output, body);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            id = input.readInt();
            fromAddress = (ActorAddress.ActorAddressRemote) kryo.readClassAndObject(input);
            body = kryo.readClassAndObject(input);
        }
    }

    public static final boolean CLOSE_EACH_WRITE = false;

    public List<Function<ActorAddress, ActorAddress>> getAddressNormalizers() {
        return addressNormalizers;
    }

    public ActorAddress normalizeHostAddress(ActorAddress host) {
        for (Function<ActorAddress, ActorAddress> f : addressNormalizers) {
            ActorAddress h = f.apply(host);
            if (h != null) {
                host = h;
            }
        }
        return host;
    }

    public static class NormalizerLocalhost implements Function<ActorAddress, ActorAddress> {
        @Override
        public ActorAddress apply(ActorAddress actorAddress) {
            ActorAddress.ActorAddressRemote host = actorAddress.getHostAddress();
            if (host != null) {
                String h = host.getHost();
                if (h.equals("[0:0:0:0:0:0:0:1]") || h.equals("[::1]") ||
                        h.equals("0.0.0.0")) {
                    if (actorAddress instanceof ActorAddress.ActorAddressRemoteActor) {
                        return ActorAddress.ActorAddressRemoteActor.get("localhost", host.getPort(),
                                ((ActorAddress.ActorAddressRemoteActor) actorAddress).getActorName());
                    } else if (actorAddress instanceof ActorAddress.ActorAddressRemote) {
                        return ActorAddress.ActorAddressRemoteActor.get("localhost", host.getPort());
                    }
                }
            }
            return actorAddress;
        }
    }
}
