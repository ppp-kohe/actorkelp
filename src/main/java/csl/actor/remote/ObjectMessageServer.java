package csl.actor.remote;

import com.esotericsoftware.kryo.io.Input;
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

import java.io.Closeable;
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

    public static boolean debugTraceLog = System.getProperty("csl.actor.trace.server", "false").equals("true");

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

    public static KryoBuilder.SerializerPool defaultSerializer = new KryoBuilder.SerializerPoolDefault();

    protected void initSerializer() {
        if (serializer == null) {
            serializer = defaultSerializer;
            ActorSystemRemote.log(ActorSystemRemote.debugLog, 161, "%s use default serializer", this);
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
            leaderGroup = new NioEventLoopGroup(leaderThreads);
        }
    }

    protected void initWorkerGroup() {
        if (workerGroup == null) {
            workerGroup = new NioEventLoopGroup(workerThreads);
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
        bootstrap.childHandler(new ServerInitializer(this));
    }

    protected void initChannel() throws InterruptedException {
        channel = (host == null ?
                    bootstrap.bind(port) :
                    bootstrap.bind(host, port))
                .sync();
    }

    protected void closeGroups() {
        leaderGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    public void close() {
        channel.channel().close();
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

    public static class ServerInitializer extends ChannelInitializer<SocketChannel> {
        protected ObjectMessageServer owner;

        public ServerInitializer(ObjectMessageServer owner) {
            this.owner = owner;
        }

        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            QueueServerHandler handler = new QueueServerHandler(owner.getSerializer(), owner.getReceiver());
            ActorSystemRemote.log(ActorSystemRemote.debugLog, 161, "%s local:%s, remote:%s, handler:%s", this, socketChannel.localAddress(), socketChannel.remoteAddress(), handler);
            ActorSystemRemote.settingsSocketChannel(socketChannel);

            //length[4] + contents[length]
            ChannelPipeline pipeline = socketChannel.pipeline();
            if (debugTraceLog) {
                pipeline.addLast(new LoggingHandler(ObjectMessageServer.class, LogLevel.INFO));
            }
            pipeline.addLast(new LengthFieldBasedFrameDecoder(
                                    Integer.MAX_VALUE,
                                    0, 4, 0, 0),
                            handler);
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

    public static class QueueServerHandler extends ChannelInboundHandlerAdapter {
        protected KryoBuilder.SerializerFunction serializer;
        protected Function<Object,Integer> receiver;
        protected volatile boolean firstError = true;

        public QueueServerHandler(KryoBuilder.SerializerFunction serializer, Function<Object, Integer> receiver) {
            this.serializer = serializer;
            this.receiver = receiver;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                int length = buf.readInt();
                ActorSystemRemote.log(ActorSystemRemote.debugLogMsg, 161, "%s bytes %,d  len %,d, serializer=%s", this, buf.readableBytes(), length, serializer);
                Input input = new Input(new ByteBufInputStream(buf, length));
                Object value = serializer.read(input);
                ReferenceCountUtil.release(buf);
                int r = 200;
                if (receiver != null) {
                    r = receiver.apply(value);
                }

                ByteBuf res = ctx.alloc().buffer(4);
                res.writeInt(r);
                ctx.writeAndFlush(res);
                if (ActorSystemRemote.CLOSE_EACH_WRITE) {
                    ctx.close();
                }
                ActorSystemRemote.log(ActorSystemRemote.debugLogMsg, 161, "%s read finish: %d", this, r);
            } else {
                ActorSystemRemote.log(ActorSystemRemote.debugLogMsg, 161, "%s ignore %s", this, msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ActorSystemRemote.log(ActorSystemRemote.debugLogMsg, 161, "%s exceptionCaught %s", this, cause);
            System.err.println(String.format("%s exceptionCaught %s", this, cause));
            if (firstError && (cause.getMessage() == null || !cause.getMessage().contains("Connection reset by peer"))) {
                cause.printStackTrace();
                firstError = false;
            }
            ctx.close();
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
            return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this));
        }
    }
}
