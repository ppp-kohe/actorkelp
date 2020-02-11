package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.util.Pool;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ObjectMessageServer implements Closeable {
    public static void main(String[] args) {
        ObjectMessageServer server = new ObjectMessageServer();
        if (args.length > 0) {
            server.setPort(Integer.parseInt(args[0]));
        }
        server.setReceiver(System.out::println)
                .start();
    }

    protected String host = null;
    protected int port = 38888;
    protected Supplier<Kryo> serializer;

    protected int leaderThreads = 1;
    protected int workerThreads = 4;
    protected ServerBootstrap bootstrap;
    protected EventLoopGroup leaderGroup;
    protected EventLoopGroup workerGroup;
    protected ChannelFuture channel;

    protected Consumer<Object> receiver;

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

    public ObjectMessageServer setReceiver(Consumer<Object> receiver) {
        this.receiver = receiver;
        return this;
    }

    public ObjectMessageServer setSerializer(Supplier<Kryo> serializer) {
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

    public static Pool<Kryo> defaultSerializer = new Pool<Kryo>(true, false, 8) {
        @Override
        protected Kryo create() {
            return new Kryo();
        }
    };

    protected void initSerializer() {
        if (serializer == null) {
            serializer = defaultSerializer::obtain;
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

    public Supplier<Kryo> getSerializer() {
        return serializer;
    }

    public Consumer<Object> getReceiver() {
        return receiver;
    }

    public static class ServerInitializer extends ChannelInitializer<SocketChannel> {
        protected ObjectMessageServer owner;

        public ServerInitializer(ObjectMessageServer owner) {
            this.owner = owner;
        }

        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            //length[4] + contents[length]
            ChannelPipeline pipeline = socketChannel.pipeline();
            if (debugTraceLog) {
                pipeline.addLast(new LoggingHandler(ObjectMessageServer.class, LogLevel.INFO));
            }
            pipeline.addLast(new LengthFieldBasedFrameDecoder(
                                    Integer.MAX_VALUE,
                                    0, 4, 0, 0),
                            new QueueServerHandler(owner.getSerializer(), owner.getReceiver()));
        }

        /** @return implementation field getter */
        public ObjectMessageServer getOwner() {
            return owner;
        }
    }

    public static class QueueServerHandler extends SimpleChannelInboundHandler<Object> {
        protected Supplier<Kryo> serializer;
        protected Consumer<Object> receiver;

        public QueueServerHandler(Supplier<Kryo> serializer, Consumer<Object> receiver) {
            this.serializer = serializer;
            this.receiver = receiver;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                int length = buf.readInt();
                ActorSystemRemote.log("bytes %,d  len %,d", buf.readableBytes(), length);
                Input input = new Input(new ByteBufInputStream(buf, length));
                Object value = serializer.get().readClassAndObject(input);
                if (receiver != null) {
                    receiver.accept(value);
                }
                ByteBuf res = ctx.alloc().buffer(4);
                res.writeInt(200);
                ctx.writeAndFlush(res);

                ActorSystemRemote.log("read finish");
            } else {
                ActorSystemRemote.log("ignore %s", msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
        }

        /** @return implementation field getter */
        public Supplier<Kryo> getSerializer() {
            return serializer;
        }

        /** @return implementation field getter */
        public Consumer<Object> getReceiver() {
            return receiver;
        }

    }
}
