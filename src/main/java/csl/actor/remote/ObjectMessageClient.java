package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;

import java.io.Closeable;

public class ObjectMessageClient implements Closeable {
    protected Kryo serializer;

    protected Bootstrap bootstrap;
    protected int threads = 4;
    protected EventLoopGroup group;

    protected String host = "localhost";
    protected int port = 38888;

    public ObjectMessageClient setSerializer(Kryo serializer) {
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

    public ObjectMessageClient start() {
        initSerializer();
        initGroup();
        initBootstrap();
        return this;
    }

    protected void initSerializer() {
        if (serializer == null) {
            serializer = new Kryo();
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

    public Kryo getSerializer() {
        return serializer;
    }

    public ObjectMessageConnection connect() {
        return new ObjectMessageConnection(this)
                .setHost(host)
                .setPort(port);
    }

    public Bootstrap getBootstrap() {
        return bootstrap;
    }

    public void close() {
        group.shutdownGracefully();
    }

    public static class ObjectMessageConnection implements Closeable {
        protected ObjectMessageClient client;
        protected String host;
        protected int port;
        protected ChannelFuture channel;

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
            channel = client.getBootstrap().connect(host, port).sync();
            return this;
        }

        public ObjectMessageConnection write(Object msg) {
            if (channel == null) {
                try {
                    open();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
            channel.channel().writeAndFlush(msg);
            return this;
        }

        public void close() {
            if (channel != null) {
                channel.channel().closeFuture();
                channel = null;
            }
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
         * @param ch
         * @throws Exception
         * @see ChannelPipeline
         */
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline()
                    .addLast(
                            //new LoggingHandler(LogLevel.INFO),
                            new LengthFieldPrepender(4, false),
                            new QueueClientHandler(owner.getSerializer()),
                            new ResponseHandler());
        }
    }

    public static class QueueClientHandler extends MessageToByteEncoder<Object> {
        protected Kryo serializer;

        public QueueClientHandler(Kryo serializer) {
            this.serializer = serializer;
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
            Output output = new Output(new ByteBufOutputStream(out));
            serializer.writeClassAndObject(output, msg);
            output.flush();
            //System.err.println("writable " + out.writableBytes() + " " + out.writerIndex());
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
            }
        }
    }
}
