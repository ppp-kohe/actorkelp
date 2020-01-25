package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystem;
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
import java.util.function.Supplier;

public class ObjectMessageClient implements Closeable {
    protected Supplier<Kryo> serializer;

    protected Bootstrap bootstrap;
    protected int threads = 4;
    protected EventLoopGroup group;

    protected String host = "localhost";
    protected int port = 38888;
    protected boolean started;

    public static boolean debugTraceLog = System.getProperty("csl.actor.trace.client", "false").equals("true");

    public ObjectMessageClient setSerializer(Supplier<Kryo> serializer) {
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

    public ObjectMessageClient start() {
        initSerializer();
        initGroup();
        initBootstrap();
        started = true;
        return this;
    }

    protected void initSerializer() {
        if (serializer == null) {
            serializer = ObjectMessageServer.defaultSerializer::obtain;
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
            .channel(NioSocketChannel.class)
            .handler(new ClientInitializer(this));
    }

    public boolean isStarted() {
        return started;
    }

    public Supplier<Kryo> getSerializer() {
        return serializer;
    }

    public ObjectMessageConnection connect() {
        synchronized (this) {
            if (!isStarted()) {
                ActorSystemRemote.log("client-connect start %s", this);
                start();
            }
        }
        return new ObjectMessageConnection(this)
                .setHost(host)
                .setPort(port);
    }

    public Bootstrap getBootstrap() {
        return bootstrap;
    }

    public void close() {
        if (group != null) {
            group.shutdownGracefully();
        }
    }

    public static class ObjectMessageConnection implements Closeable {
        protected ObjectMessageClient client;
        protected String host;
        protected int port;
        protected Channel channel;

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
            channel = client.getBootstrap()
                    .connect(host, port)
                    .sync()
                    .channel();
            return this;
        }

        public ObjectMessageConnection write(Object msg) {
            if (channel == null || !channel.isWritable()) {
                try {
                    close();
                    open();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
            ActorSystemRemote.log("connection channel %s:%d open=%s, active=%s, writable=%s", host, port,
                    channel.isOpen(), channel.isActive(),
                    channel.isWritable());
            channel.writeAndFlush(msg);
            return this;
        }

        public void close() {
            if (channel != null && channel.isOpen()) {
                ActorSystemRemote.log("connection close: %s:%d", host, port);
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
    }

    public static class ClientInitializer extends ChannelInitializer<SocketChannel> {
        protected ObjectMessageClient owner;

        public ClientInitializer(ObjectMessageClient owner) {
            this.owner = owner;
        }

        /**
         * append 4 bytes (BigEndian) int length of subsequent bytes,
         *  receives int response
         * @param ch a channel
         * @see ChannelPipeline
         */
        @Override
        protected void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            if (debugTraceLog) {
                pipeline.addLast(new LoggingHandler(ObjectMessageClient.class, LogLevel.INFO));
            }
            pipeline.addLast(new LengthFieldPrepender(4, false),
                            new QueueClientHandler(owner.getSerializer()),
                            new ResponseHandler());
        }

        /** @return implementation field getter */
        public ObjectMessageClient getOwner() {
            return owner;
        }
    }

    public static class QueueClientHandler extends MessageToByteEncoder<Object> {
        protected Supplier<Kryo> serializer;

        public QueueClientHandler(Supplier<Kryo> serializer) {
            this.serializer = serializer;
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
            Output output = new Output(new ByteBufOutputStream(out));
            serializer.get().writeClassAndObject(output, msg);
            output.flush();
        }

        /** @return implementation field getter */
        public Supplier<Kryo> getSerializer() {
            return serializer;
        }
    }

    public static class ResponseHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                try {
                    int n = buf.readInt();
                    if (n != 200) {
                        System.err.println(n + " : " + ctx);
                    }
                    ctx.close();
                } finally {
                    ReferenceCountUtil.release(buf);
                }
            } else {
                System.err.println("? " + msg);
            }
        }
    }
}
