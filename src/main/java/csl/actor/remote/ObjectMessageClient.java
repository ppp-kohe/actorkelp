package csl.actor.remote;

import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCountUtil;

import java.io.Closeable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ObjectMessageClient implements Closeable {
    protected KryoBuilder.SerializerFunction serializer;

    protected Bootstrap bootstrap;
    protected int threads = 4;
    protected EventLoopGroup group;

    protected String host = "localhost";
    protected int port = 38888;
    protected boolean started;

    protected ActorSystem.SystemLogger logger;

    public static boolean debugTraceLog = System.getProperty("csl.actor.trace.client", "false").equals("true");
    public static int debugLogClientColor = ActorSystem.systemPropertyColor("csl.actor.client.color", 125);

    public ObjectMessageClient(ActorSystem.SystemLogger logger) {
        this.logger = logger;
    }

    public ObjectMessageClient() {
        this(new ActorSystemDefault.SystemLoggerErr());
    }

    public ObjectMessageClient setSerializer(KryoBuilder.SerializerFunction serializer) {
        this.serializer = serializer;
        return this;
    }

    public ObjectMessageClient setThreads(int threads) {
        this.threads = threads;
        return this;
    }

    public ObjectMessageClient setHost(String host) {
        this.host = host;
        return this;
    }

    public ObjectMessageClient setPort(int port) {
        this.port = port;
        return this;
    }

    public ObjectMessageClient setGroup(EventLoopGroup group) {
        this.group = group;
        return this;
    }

    /** @return implementation field getter */
    public int getThreads() {
        return threads;
    }

    /** @return implementation field getter */
    public EventLoopGroup getGroup() {
        return group;
    }

    /** @return implementation field getter */
    public String getHost() {
        return host;
    }

    /** @return implementation field getter */
    public int getPort() {
        return port;
    }

    public ActorSystem.SystemLogger getLogger() {
        return logger;
    }

    public ObjectMessageClient start() {
        initSerializer();
        initGroup();
        initBootstrap();
        started = true;
        return this;
    }

    protected void initSerializer() {
        if (serializer == null) {
            serializer = ObjectMessageServer.defaultSerializer;
            getLogger().log(ActorSystemRemote.debugLog, debugLogClientColor, "%s use default serializer", this);
        }
    }

    protected void initGroup() {
        if (group == null) {
            group = new NioEventLoopGroup(threads);
        }
    }

    protected void initBootstrap() {
        bootstrap = new Bootstrap();
        bootstrap.group(group)
            .option(ChannelOption.AUTO_CLOSE, false)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .channel(NioSocketChannel.class);
    }

    public boolean isStarted() {
        return started;
    }

    public KryoBuilder.SerializerFunction getSerializer() {
        return serializer;
    }

    public ObjectMessageConnection connect() {
        synchronized (this) {
            if (!isStarted()) {
                getLogger().log(ActorSystemRemote.debugLog, debugLogClientColor, "%s connect start", this);
                start();
            }
        }
        return initConnection()
                .setHost(host)
                .setPort(port);
    }

    protected ObjectMessageConnection initConnection() {
        return new ObjectMessageConnection(this);
    }

    public Bootstrap getBootstrap() {
        return bootstrap;
    }

    public void close() {
        if (group != null) {
            group.shutdownGracefully();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" +
                Integer.toHexString(System.identityHashCode(this)) + "(" + host + ':' + port + ")";
    }

    public static class ObjectMessageConnection implements Closeable {
        protected ObjectMessageClient client;
        protected String host;
        protected int port;
        protected Channel channel;

        protected ConcurrentLinkedQueue<ChannelFuture> last = new ConcurrentLinkedQueue<>();
        protected AtomicInteger lastSize = new AtomicInteger();

        public ObjectMessageConnection(ObjectMessageClient client) {
            this.client = client;
        }

        public ObjectMessageConnection setHost(String host) {
            this.host = host;
            return this;
        }

        public ObjectMessageConnection setPort(int port) {
            this.port = port;
            return this;
        }

        public ObjectMessageConnection open() throws InterruptedException {
            client.getLogger().log(ActorSystemRemote.debugLog, debugLogClientColor, "%s open", this);
            channel = client.getBootstrap()
                    .handler(new ClientInitializer(client, this))
                    .connect(host, port)
                    .sync()
                    .channel();
            return this;
        }

        public ObjectMessageConnection write(Object msg) {
            return write(msg, 0);
        }

        public ObjectMessageConnection write(Object msg, int retryCount) {
            if (channel == null || !channel.isOpen()) {
                try {
                    logWrite(retryCount, "before write re-open");
                    //close();
                    open();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
            logWrite(retryCount, "before write");
            try {
                clearResult();
                ChannelFuture f = channel.writeAndFlush(msg);
                checkResult(f);
            } catch (Exception c) {
                client.getLogger().log(true, debugLogClientColor, "%s write failure: %s %s", this, c, Thread.currentThread());
                logWrite(retryCount, String.format("write failure %s", c));
                if (retryCount < 10) {
                    return write(msg, retryCount + 1);
                } else {
                    return this; //drop
                }
            }
            return this;
        }

        private void logWrite(int retryCount, String msg) {
            if (ActorSystemRemote.debugLogMsg) {
                if (channel == null) {
                    client.getLogger().log(ActorSystemRemote.debugLogMsg, debugLogClientColor, "%s %s, retry=%d, channel=null",
                            this, msg, retryCount);
                } else {
                    client.getLogger().log(ActorSystemRemote.debugLogMsg, debugLogClientColor, "%s %s, retry=%d, open=%s, active=%s, writable=%s",
                            this, msg, retryCount,
                            channel.isOpen(), channel.isActive(),
                            channel.isWritable());
                }
            }
        }

        public void setResult(int result) {
            client.getLogger().log(ActorSystemRemote.debugLogMsg, debugLogClientColor, "%s result-code %d", this, result);

        }

        protected void clearResult() throws Exception {
            int max = 1;
            if (lastSize.incrementAndGet() > max) {
                while (lastSize.get() > max) {
                    ChannelFuture f = last.peek();
                    if (f == null) {
                        break;
                    }
                    if (f.isDone()) {
                        if (last.remove(f)) {
                            lastSize.decrementAndGet();
                        }
                    } else if (!f.await(10_000)) {
                        throw new RuntimeException(String.format("%s timeout", this));
                    } else {
                        if (last.remove(f)) {
                            lastSize.decrementAndGet();
                        }
                    }
                }
            }
        }

        protected void checkResult(ChannelFuture f) throws Exception {
            last.add(f);
            f.addListener(sf -> {
                client.getLogger().log(ActorSystemRemote.debugLogMsg, debugLogClientColor, "%s finish write", this);
                if (last.remove(f)) {
                    lastSize.decrementAndGet();
                }
            });
        }

        public void close() {
            if (channel != null && channel.isOpen()) {
                client.getLogger().log(ActorSystemRemote.debugLog, debugLogClientColor, "%s close", this);
                channel.close();
                channel = null;
            }
        }

        /** @return implementation field getter */
        public ObjectMessageClient getClient() {
            return client;
        }

        /** @return implementation field getter */
        public String getHost() {
            return host;
        }

        /** @return implementation field getter */
        public int getPort() {
            return port;
        }

        /** @return implementation field getter */
        public Channel getChannel() {
            return channel;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "@" +
                    Integer.toHexString(System.identityHashCode(this)) + "(" + host + ':' + port + ")";
        }
    }

    public static class ClientInitializer extends ChannelInitializer<SocketChannel> {
        protected ObjectMessageClient owner;
        protected ObjectMessageConnection connection;

        public ClientInitializer(ObjectMessageClient owner, ObjectMessageConnection connection) {
            this.owner = owner;
            this.connection = connection;
        }

        /**
         * append 4 bytes (BigEndian) int length of subsequent bytes,
         *  receives int response
         * @param ch a channel
         * @see ChannelPipeline
         */
        @Override
        protected void initChannel(SocketChannel ch) {
            ActorSystemRemote.settingsSocketChannel(ch);

            ChannelPipeline pipeline = ch.pipeline();
            if (debugTraceLog) {
                pipeline.addLast(new LoggingHandler(ObjectMessageClient.class, LogLevel.INFO));
            }
            pipeline.addLast(new LengthFieldPrepender(4, false),
                            new QueueClientHandler(owner.getLogger(), owner.getSerializer()),
                            new ResponseHandler(owner.getLogger(), connection::setResult));
        }

        /** @return implementation field getter */
        public ObjectMessageClient getOwner() {
            return owner;
        }
    }

    public static class QueueClientHandler extends MessageToByteEncoder<Object> {
        protected ActorSystem.SystemLogger logger;
        protected KryoBuilder.SerializerFunction serializer;
        protected boolean firstError;

        public QueueClientHandler(ActorSystem.SystemLogger logger, KryoBuilder.SerializerFunction serializer) {
            this.logger = logger;
            this.serializer = serializer;
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
            try {
                Output output = new Output(new ByteBufOutputStream(out));
                serializer.write(output, msg);
                output.flush();
            } catch (Exception ex) {
                if (firstError) {
                    logger.log(true, debugLogClientColor, ex, "encode: %s", logger.toStringLimit(msg));
                    firstError = false;
                }
                throw ex;
            }
        }

        /** @return implementation field getter */
        public KryoBuilder.SerializerFunction getSerializer() {
            return serializer;
        }
    }

    public static class ResponseHandler extends ChannelInboundHandlerAdapter {
        protected ActorSystem.SystemLogger logger;
        protected Consumer<Integer> resultHandler;
        protected boolean firstError = true;

        public ResponseHandler(ActorSystem.SystemLogger logger, Consumer<Integer> resultHandler) {
            this.logger = logger;
            this.resultHandler = resultHandler;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                try {
                    resultHandler.accept(buf.readInt());
                    if (ActorSystemRemote.CLOSE_EACH_WRITE) {
                        ctx.close();
                    }
                } finally {
                    ReferenceCountUtil.release(buf);
                }
            } else {
                logger.log(true, debugLogClientColor, "? %s", logger.toStringLimit(msg));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (firstError) {
                logger.log(true, debugLogClientColor, cause, "response-handler exception");
                firstError = false;
            } else {
                logger.log(true, debugLogClientColor, "response-handler exception: %s", cause);
            }
            super.exceptionCaught(ctx, cause);
        }
    }
}
