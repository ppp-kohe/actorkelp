package csl.actor.remote;

import com.esotericsoftware.kryo.io.Input;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ObjectMessageServer implements Closeable {
    public static void main(String[] args) {
        ObjectMessageServer server = new ObjectMessageServer();
        if (args.length > 0) {
            server.setPort(Integer.parseInt(args[0]));
        }
        server.setReceiver(o -> {System.out.println(o); return 200;})
                .start();
    }

    protected String host = null;
    protected int port = 38888;
    protected KryoBuilder.SerializerFunction serializer;

    protected int leaderThreads = 1;
    protected int workerThreads = 4;
    protected ServerBootstrap bootstrap;
    protected EventLoopGroup leaderGroup;
    protected EventLoopGroup workerGroup;
    protected ChannelFuture channel;

    protected Function<Object, Integer> receiver;

    protected ActorSystem.SystemLogger logger;

    protected AtomicLong recordReceiveCount = new AtomicLong();
    protected AtomicLong recordReceiveBytes = new AtomicLong();
    protected AtomicLong recordReceiveErrors = new AtomicLong();
    protected AtomicLong recordReceiveTimeNanos = new AtomicLong();

    public static boolean debugTraceLog = System.getProperty("csl.actor.trace.server", "false").equals("true");
    public static int debugLogServerColor = ActorSystem.systemPropertyColor("csl.actor.server.color", 162);

    public ObjectMessageServer(ActorSystem.SystemLogger logger) {
        this.logger = logger;
    }

    public ObjectMessageServer() {
        this(new ActorSystemDefault.SystemLoggerErr());
    }

    public ObjectMessageServer setLeaderThreads(int leaderThreads) {
        this.leaderThreads = leaderThreads;
        return this;
    }

    public ObjectMessageServer setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
        return this;
    }

    public ObjectMessageServer setWorkerGroup(EventLoopGroup workerGroup) {
        this.workerGroup = workerGroup;
        return this;
    }

    public ObjectMessageServer setLeaderGroup(EventLoopGroup leaderGroup) {
        this.leaderGroup = leaderGroup;
        return this;
    }

    public ObjectMessageServer setHost(String host) {
        this.host = host;
        return this;
    }

    public ObjectMessageServer setPort(int port) {
        this.port = port;
        return this;
    }

    public ObjectMessageServer setReceiver(Function<Object, Integer> receiver) {
        this.receiver = receiver;
        return this;
    }

    public ObjectMessageServer setSerializer(KryoBuilder.SerializerFunction serializer) {
        this.serializer = serializer;
        return this;
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
    public int getLeaderThreads() {
        return leaderThreads;
    }

    /** @return implementation field getter */
    public int getWorkerThreads() {
        return workerThreads;
    }

    /** @return implementation field getter */
    public ServerBootstrap getBootstrap() {
        return bootstrap;
    }

    /** @return implementation field getter */
    public EventLoopGroup getLeaderGroup() {
        return leaderGroup;
    }

    /** @return implementation field getter */
    public EventLoopGroup getWorkerGroup() {
        return workerGroup;
    }

    /** @return implementation field getter */
    public ChannelFuture getChannel() {
        return channel;
    }

    public ActorSystem.SystemLogger getLogger() {
        return logger;
    }

    public static KryoBuilder.SerializerPool defaultSerializer = new KryoBuilder.SerializerPoolDefault();

    protected void initSerializer() {
        if (serializer == null) {
            serializer = defaultSerializer;
            logger.log(ActorSystemRemote.debugLog, debugLogServerColor, "%s use default serializer", this);
        }
    }

    public ObjectMessageServer start() {
        initSerializer();
        initLeaderGroup();
        initWorkerGroup();
        try {
            initBootstrap();
            initChannel();
            channel.channel().closeFuture().sync();
            return this;
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        } finally {
            closeGroups();
        }
    }

    public ObjectMessageServer startWithoutWait() {
        initSerializer();
        initLeaderGroup();
        initWorkerGroup();
        try {
            initBootstrap();
            initChannel();
            return this;
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

    protected void initLeaderGroup() {
        if (leaderGroup == null) {
            leaderGroup = new NioEventLoopGroup(leaderThreads, new DefaultThreadFactory(getPoolNameHead() + "Leader", 10));
        }
    }

    protected String getPoolNameHead() {
        return getClass().getSimpleName();
    }

    protected void initWorkerGroup() {
        if (workerGroup == null) {
            workerGroup = new NioEventLoopGroup(workerThreads, new DefaultThreadFactory(getPoolNameHead() + "Client", 10));
        }
    }

    protected void initBootstrap() {
        bootstrap = new ServerBootstrap();
        bootstrap.group(leaderGroup, workerGroup)
                .option(ChannelOption.AUTO_CLOSE, false)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_BACKLOG, 120_000)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.AUTO_CLOSE, false)
                .channel(NioServerSocketChannel.class);
        if (debugTraceLog) {
            bootstrap.handler(new LoggingHandler(ObjectMessageServer.class, LogLevel.INFO));
        }
        initBootstrapInitializer();
    }

    protected void initBootstrapInitializer() {
        bootstrap.childHandler(new ServerInitializer(this));
    }

    protected void initChannel() throws InterruptedException {
        channel = (host == null ?
                    bootstrap.bind(port) :
                    bootstrap.bind(host, port))
                .sync();
    }

    protected void closeGroups() {
        if (leaderGroup != null) {
            leaderGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    public void close() {
        if (channel != null) {
            channel.channel().close();
        }
        closeGroups();
    }

    public KryoBuilder.SerializerFunction getSerializer() {
        return serializer;
    }

    public Function<Object, Integer> getReceiver() {
        return receiver;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" +
                Integer.toHexString(System.identityHashCode(this)) + "(" + host + ':' + port + ")";
    }

    public void recordReceiveBytesAndTime(long n, Duration time) {
        recordReceiveCount.incrementAndGet();
        recordReceiveBytes.addAndGet(n);
        recordReceiveTimeNanos.addAndGet(time.toNanos());
    }

    public void recordReceiveError(Throwable ex) {
        recordReceiveErrors.incrementAndGet();
    }

    public long getRecordReceiveBytes() {
        return recordReceiveBytes.get();
    }

    public long getRecordReceiveCount() {
        return recordReceiveCount.get();
    }

    public long getRecordReceiveErrors() {
        return recordReceiveErrors.get();
    }

    public Duration getRecordReceiveTime() {
        return Duration.ofNanos(recordReceiveTimeNanos.get());
    }

    public static class ServerInitializer extends ChannelInitializer<SocketChannel> {
        protected ObjectMessageServer owner;

        public ServerInitializer(ObjectMessageServer owner) {
            this.owner = owner;
        }

        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            ChannelHandler handler = initHandler(socketChannel);
            owner.getLogger().log(ActorSystemRemote.debugLog, debugLogServerColor,
                    "%s local:%s, remote:%s, handler:%s", this, socketChannel.localAddress(), socketChannel.remoteAddress(), handler);
            ActorSystemRemote.settingsSocketChannel(socketChannel);

            initPipeline(socketChannel, handler);
        }

        protected void initPipeline(SocketChannel socketChannel, ChannelHandler handler) {
            //length[4] + contents[length]
            ChannelPipeline pipeline = socketChannel.pipeline();
            if (debugTraceLog) {
                pipeline.addLast(new LoggingHandler(ObjectMessageServer.class, LogLevel.INFO));
            }
            pipeline.addLast(initFrameDecoder(socketChannel),
                    handler);
        }

        protected ChannelHandler initHandler(SocketChannel channel) {
            return new QueueServerHandler(owner, channel.remoteAddress(), owner.getLogger(), owner.getSerializer(), owner.getReceiver());
        }

        protected ChannelHandler initFrameDecoder(SocketChannel channel) {
            return new LengthFieldBasedFrameDecoder(
                    Integer.MAX_VALUE,
                    0, 4, 0, 0);
        }

        /** @return implementation field getter */
        public ObjectMessageServer getOwner() {
            return owner;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this)) + "(" + owner + ")";
        }
    }

    public static final int RESULT_CLOSE = 0xFFFF_FFFF;

    public static class QueueServerHandler extends ChannelInboundHandlerAdapter {
        protected ObjectMessageServer owner;
        protected ActorSystem.SystemLogger logger;
        protected KryoBuilder.SerializerFunction serializer;
        protected Function<Object,Integer> receiver;
        protected volatile boolean firstError = true;
        protected InetSocketAddress clientAddress;

        public QueueServerHandler(ObjectMessageServer owner, InetSocketAddress clientAddress, ActorSystem.SystemLogger logger, KryoBuilder.SerializerFunction serializer, Function<Object, Integer> receiver) {
            this.owner = owner;
            this.clientAddress = clientAddress;
            this.logger = logger;
            this.serializer = serializer;
            this.receiver = receiver;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            Instant start = Instant.now();
            long[] n = {0};
            try {
                if (msg instanceof ByteBuf) {
                    ByteBuf buf = (ByteBuf) msg;
                    int length = buf.readInt();
                    logger.log(ActorSystemRemote.debugLogMsg, debugLogServerColor, "%s %s bytes %,d  len %,d, serializer=%s", this, clientAddress, buf.readableBytes(), length, serializer);
                    int r = channelReadByteBuf(ctx, buf, length, n);
                    logger.log(ActorSystemRemote.debugLogMsg, debugLogServerColor, "%s %s read finish: %d", this, clientAddress, r);
                } else {
                    logger.log(ActorSystemRemote.debugLogMsg, debugLogServerColor, "%s %s ignore %s", this, clientAddress, msg);
                }
            } finally {
                if (owner != null) {
                    owner.recordReceiveBytesAndTime(n[0], Duration.between(start, Instant.now()));
                }
            }
        }

        protected int channelReadByteBuf(ChannelHandlerContext ctx, ByteBuf buf, int length, long[] n) {
            Input input = new Input(new ByteBufInputStream(buf, length));
            Object value = serializer.read(input);
            ReferenceCountUtil.release(buf);
            int r = 200;
            if (receiver != null) {
                r = receiver.apply(value);
            }

            n[0] += 4 + input.total();

            ByteBuf res = ctx.alloc().buffer(4);
            res.writeInt(r);
            ctx.writeAndFlush(res);
            if (r == RESULT_CLOSE || ActorSystemRemote.CLOSE_EACH_WRITE) {
                ctx.close();
            }
            return r;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (firstError) {
                logger.log(true, debugLogServerColor, cause, "%s %s exceptionCaught", this, clientAddress);
                firstError = false;
            } else {
                logger.log(ActorSystemRemote.debugLogMsg, debugLogServerColor, cause, "%s %s exceptionCaught", this, clientAddress);
            }
            ctx.close();
            if (owner != null) {
                owner.recordReceiveError(cause);
            }
        }

        /** @return implementation field getter */
        public KryoBuilder.SerializerFunction getSerializer() {
            return serializer;
        }

        /** @return implementation field getter */
        public Function<Object, Integer> getReceiver() {
            return receiver;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this)) + "(" + owner + ")";
        }
    }
}
