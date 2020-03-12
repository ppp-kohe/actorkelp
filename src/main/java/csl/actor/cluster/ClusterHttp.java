package csl.actor.cluster;

import csl.actor.ActorRef;
import csl.actor.ActorRefLocalNamed;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorRefRemoteSerializer;
import csl.actor.remote.ObjectMessageServer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;

import java.lang.reflect.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClusterHttp {
    protected ClusterDeployment<?,?> deployment;
    protected Map<String, List<HttpMethod>> methods;
    protected HttpServer server;
    protected ActorSystem.SystemLogger logger;
    protected ActorRefRemoteSerializer<ActorRef> actorToAddress;

    public static boolean logHttp = System.getProperty("csl.actor.logHttp", "true").equals("true");
    public static int logColorHttp = ActorSystem.systemPropertyColor("csl.actor.logHttp.color", 91);

    public ClusterHttp(ClusterDeployment<?, ?> deployment) {
        this.deployment = deployment;
        if (deployment.getSystem() != null) {
            logger = deployment.getSystem().getLogger();
            actorToAddress = new ActorRefRemoteSerializer<>(deployment.getSystem());
        } else { //for testing
            logger = new ActorSystemDefault.SystemLoggerErr();
            actorToAddress = new ActorRefRemoteSerializer<>(null);
        }
        initMethods();
    }

    protected void initMethods() {
        methods = new HashMap<>();
        Arrays.stream(deployment.getClass().getMethods())
                .filter(m -> m.isAnnotationPresent(ClusterDeployment.PropertyInterface.class))
                .map(this::initMethod)
                .filter(Objects::nonNull)
                .forEach(m -> methods.computeIfAbsent(m.getName(), n -> new ArrayList<>())
                        .add(m));
    }


    public void start(String host, int port) {
        server = (HttpServer) new HttpServer(this)
                .setHost(host).setPort(port)
                .startWithoutWait();
        getLogger().log(logHttp, logColorHttp, "http-server started: %s:%d methods=%,d", host, port, methods.size());
    }

    public ClusterDeployment<?, ?> getDeployment() {
        return deployment;
    }

    public ActorSystem.SystemLogger getLogger() {
        return logger;
    }

    public HttpMethod getMethod(String name, int paramCount) {
        return methods.getOrDefault(name, Collections.emptyList()).stream()
                .filter(m -> m.getArgumentSize() == paramCount)
                .findFirst()
                .orElse(null);
    }

    public static class HttpServer extends ObjectMessageServer {
        protected ClusterHttp owner;

        public HttpServer(ClusterHttp owner) {
            this.owner = owner;
        }

        @Override
        protected void initBootstrapInitializer() {
            bootstrap.childHandler(new HttpInitializer(owner, null));
        }
    }

    public static class HttpInitializer extends ChannelInitializer<SocketChannel> {
        protected ClusterHttp owner;
        protected SslContext ssl;

        public HttpInitializer(ClusterHttp owner, SslContext ssl) {
            this.owner = owner;
            this.ssl = ssl;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            if (ssl != null) {
                p.addLast(ssl.newHandler(ch.alloc()));
            }
            p.addLast(new HttpRequestDecoder());
            p.addLast(new HttpResponseEncoder());
            p.addLast(new HttpHandler(owner));
        }
    }

    public static class HttpHandler extends SimpleChannelInboundHandler<HttpObject> {
        protected ClusterHttp owner;
        protected ActorSystem.SystemLogger logger;
        public static boolean supportKeepAlive = true;

        public HttpHandler(ClusterHttp owner) {
            this.owner = owner;
            this.logger = owner.getLogger();
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
            if (msg instanceof HttpRequest) {
                HttpRequest req = (HttpRequest) msg;

                QueryStringDecoder qd = new QueryStringDecoder(req.uri());
                List<String> path = paths(qd.path());
                Map<String, String> query = qd.parameters().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));

                logger.log(logHttp, logColorHttp, "request url=<%s> path=%s query=%s", req.uri(), path, query);

                FullHttpResponse res = res(req.protocolVersion(), path, query);
                res.headers()
                        .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                        .set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");

                boolean ka = supportKeepAlive && HttpUtil.isKeepAlive(req);
                if (ka) {
                    res.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, res.content().readableBytes());
                    res.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                }

                ctx.write(res);
                if (!ka) {
                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                }
            }
        }

        public FullHttpResponse res(HttpVersion ver, List<String> path, Map<String,String> query) {
            Pattern pat = Pattern.compile("a\\d+");
            int size = (int) query.keySet().stream()
                    .filter(s -> pat.matcher(s).matches())
                    .count();
            HttpMethod m;
            if (path.size() == 1 && (m = owner.getMethod(path.get(0), size)) != null) {
                try {
                    Object json = m.invoke(owner.getDeployment(), query);
                    return new DefaultFullHttpResponse(ver, HttpResponseStatus.OK, encode(json));
                } catch (Exception ex) {
                    logger.log(logHttp, logColorHttp, ex, "invoke error: %s query=%s", m, query);
                    return new DefaultFullHttpResponse(ver, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            encode("error:" + ex + " method:" + m + " path:" + path + " query:" + query));
                }
            } else {
                return new DefaultFullHttpResponse(ver, HttpResponseStatus.NOT_FOUND,
                        encode("path:" + path + " query:" + query));
            }
        }

        public ByteBuf encode(Object json) {
            StringBuilder buf = new StringBuilder();
            owner.write(buf::append, json);
            return Unpooled.copiedBuffer(buf, StandardCharsets.UTF_8);
        }


        protected List<String> paths(String path) {
            String s = path;
            if (s.startsWith("/")) {
                s = s.substring(1);
            }
            if (s.endsWith("/")) {
                s = s.substring(0, s.length() - 1);
            }
            return Arrays.stream(s.split("/"))
                        .map(c -> URLDecoder.decode(c, StandardCharsets.UTF_8))
                        .collect(Collectors.toList());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.log(logHttp, logColorHttp, cause, "handler error");
            ctx.close();
        }
    }

    protected HttpMethod initMethod(Method m) {
        if (!Modifier.isStatic(m.getModifiers())) {
            return new HttpMethod(m.getAnnotation(ClusterDeployment.PropertyInterface.class).value(),
                    m,
                    jsonConverter(m.getGenericReturnType()),
                    Arrays.stream(m.getGenericParameterTypes())
                        .map(this::argumentParser)
                        .collect(Collectors.toList()));
        }
        return null;
    }

    public Function<Object,Object> jsonConverter(Type retType) {
        if (retType != null && retType.equals(Object.class)) {
            return o -> o == null ? null : jsonConverterRaw(o.getClass()).apply(o);
        } else {
            return jsonConverterRaw(retType);
        }
    }

    public Function<Object,Object> jsonConverterRaw(Type retType) {
        if (retType == null) {
            return Function.identity();
        } else if (matchType(retType, List.class)) {
            Function<Object, Object> paramConv = jsonConverter(getParamType(retType, 0));
            return o -> ((List<?>) o).stream()
                    .map(paramConv)
                    .collect(Collectors.toList());
        } else if (matchType(retType, Map.class)) {
            Function<Object, Object> keyConv = jsonConverter(getParamType(retType, 0));
            Function<Object, Object> valConv = jsonConverter(getParamType(retType, 1));
            return o -> ((Map<?,?>) o).entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> keyConv.apply(e.getKey()),
                                e -> valConv.apply(e.getValue())));
        } else if (matchType(retType, CompletableFuture.class)) {
            Function<Object, Object> paramConv = jsonConverter(getParamType(retType, 0));
            return o -> {
                try {
                    return ((CompletableFuture<?>) paramConv).get(30, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            };
        } else if (!(retType instanceof Class<?>)) {
            return jsonConverter(getRawType(retType));
        } else if (retType.equals(Class.class)) {
            return o -> o == null ? null : ((Class<?>) o).getName();
        } else if (retType.equals(String.class)) {
            return o -> o == null ? null : Objects.toString(o);
        } else if (retType.equals(int.class) || retType.equals(Integer.class)) {
            return o -> o == null ? null : ((Integer) o).longValue();
        } else if (retType.equals(long.class) || retType.equals(Long.class)) {
            return Function.identity();
        } else if (retType.equals(boolean.class) || retType.equals(Boolean.class)) {
            return Function.identity();
        } else if (retType.equals(double.class) || retType.equals(Double.class)) {
            return Function.identity();
        } else if (retType.equals(float.class) || retType.equals(Float.class)) {
            return o -> o == null ? null : ((Float) o).doubleValue();
        } else if (retType.equals(void.class)) {
            return o -> null;
        } else if (ToJson.class.isAssignableFrom(getRawType(retType))) {
            return o -> o == null ? null : ((ToJson) o).toJson(jsonConverter(Object.class));
        } else if (ActorRef.class.isAssignableFrom(getRawType(retType))) {
            return o -> o == null ? null : toJsonActor((ActorRef) o);
        } else if (Instant.class.isAssignableFrom(getRawType(retType))) {
            return o -> o == null ? null : Objects.toString(o);
        } else if (Duration.class.isAssignableFrom(getRawType(retType))) {
            return o -> o == null ? null : Objects.toString(o);
        } else {
            return o -> o == null ? null : Objects.toString(o);
        }
    }

    public Object toJsonActor(ActorRef ref) {
        return Objects.toString(actorToAddress.getAddress(ref));
    }

    public interface ToJson {
        Map<String,Object> toJson(Function<Object,Object> valueConverter);

        default Object toJson(Function<Object,Object> valueConverter, Object o) {
            return valueConverter.apply(o);
        }

        default Object toJson(Function<Object,Object> valueConverter, Object o, Object nullVal) {
            return o == null ? valueConverter.apply(nullVal) : valueConverter.apply(o);
        }
    }

    public boolean matchType(Type type, Class<?> baseType, Class<?>... paramTypes) {
        if (type instanceof ParameterizedType) {
            ParameterizedType pType = (ParameterizedType) type;
            Type[] argTypes = pType.getActualTypeArguments();
            return pType.getRawType().equals(baseType) && //not isAssignableFrom but equals
                    IntStream.range(0, paramTypes.length)
                        .allMatch(i -> matchType(argTypes[i], paramTypes[i]));
        } else if (paramTypes.length == 0) {
            if (type instanceof Class<?>) { //for raw-type assignableFrom
                return baseType.isAssignableFrom((Class<?>) type);
            } else if (type instanceof TypeVariable<?>) {
                Type[] bs = ((TypeVariable<?>) type).getBounds();
                if (bs.length > 0) {
                    return matchType(bs[0], baseType);
                } else {
                    return baseType.equals(Object.class);
                }
            } else if (type instanceof WildcardType) {
                Type[] bs = ((WildcardType) type).getUpperBounds();
                if (bs.length > 0) {
                    return matchType(bs[0], baseType); //only checks the first "extends T"
                } else {
                    return baseType.equals(Object.class);
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public Class<?> getRawType(Type type) {
        if (type instanceof ParameterizedType) {
            return (Class<?>) ((ParameterizedType) type).getRawType();
        } else if (type instanceof TypeVariable) {
            Type[] bs = ((TypeVariable<?>) type).getBounds();
            if (bs.length > 0) {
                return getRawType(bs[0]);
            } else {
                return Object.class;
            }
        } else if (type instanceof WildcardType) {
            Type[] bs = ((WildcardType) type).getUpperBounds();
            if (bs.length > 0) {
                return getRawType(bs[0]);
            } else {
                return Object.class;
            }
        } else if (type instanceof Class<?>){
            return (Class<?>) type;
        } else {
            return Object.class;
        }
    }

    public Type getParamType(Type type, int n) {
        if (type instanceof ParameterizedType) {
            Type paramType = ((ParameterizedType) type).getActualTypeArguments()[n];
            if (paramType instanceof TypeVariable) {
                Type[] bs = ((TypeVariable<?>) paramType).getBounds();
                if (bs.length > 0) {
                    return bs[0];
                } else {
                    return Object.class;
                }
            } else if (paramType instanceof WildcardType) {
                Type[] bs = ((WildcardType) paramType).getUpperBounds();
                if (bs.length > 0) {
                    return bs[0];
                } else {
                    return Object.class;
                }
            } else {
                return paramType;
            }
        } else {
            return Object.class;
        }
    }

    public Function<String, Object> argumentParser(Type type) {
        Class<?> raw = getRawType(type);
        if (type.equals(String.class)) {
            return s -> s;
        } else if (raw != null && ActorAddress.class.isAssignableFrom(raw)) {
            return s -> {
                int n = s.indexOf("/");
                if (n < 0) {
                    return ActorAddress.get(s);
                } else {
                    String hostAndPort = s.substring(0, n);
                    String actor = s.substring(n + 1);
                    return ActorAddress.get(hostAndPort).getActor(actor);
                }
            };
        } else if (type.equals(ActorRef.class)) {
            Function<String,Object> addr = argumentParser(ActorAddress.class);
            return s -> {
                int n = s.indexOf('/');
                if (n < 0) {
                    return ActorRefLocalNamed.get(deployment.getSystem(), s);
                } else {
                    return ActorRefRemote.get(deployment.getSystem(), (ActorAddress) addr.apply(s));
                }
            };
        } else if (type.equals(int.class)) {
            return Integer::parseInt;
        } else if (type.equals(long.class)) {
            return Long::parseLong;
        } else if (type.equals(boolean.class)) {
            return Boolean::parseBoolean;
        } else if (type.equals(float.class)) {
            return Float::parseFloat;
        } else if (type.equals(double.class)) {
            return Double::parseDouble;
        } else {
            throw new RuntimeException("invalid: " + type);
        }
    }

    public static class HttpMethod {
        protected String name;
        protected Method method;
        protected Function<Object,Object> returnConverter;
        protected List<Function<String,Object>> argumentParsers;

        public HttpMethod(String name, Method method, Function<Object, Object> returnConverter, List<Function<String, Object>> argumentParsers) {
            this.name = name;
            this.method = method;
            this.returnConverter = returnConverter;
            this.argumentParsers = argumentParsers;
        }

        public String getName() {
            return name;
        }

        public int getArgumentSize() {
            return argumentParsers.size();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "name='" + name + '\'' +
                    ", method=" + method +
                    '}';
        }

        public Object invoke(Object target,
                             Map<String,String> params) throws Exception {
            return returnConverter.apply(method.invoke(target, IntStream.range(0, argumentParsers.size())
                    .mapToObj(i -> argumentParsers.get(i)
                            .apply(params.get("a" + i)))
                    .toArray()));
        }
    }

    public void write(Consumer<String> out, Object json) {
        if (json instanceof List<?>) {
            out.accept("[");
            boolean first = true;
            for (Object o : (List<?>) json) {
                if (first) {
                    first = false;
                } else {
                    out.accept(",");
                }
                write(out, o);
            }
            out.accept("]");
        } else if (json instanceof Map<?,?>) {
            out.accept("{");
            boolean first = true;
            for (Map.Entry<?,?> e : ((Map<?,?>) json).entrySet()) {
                if (first) {
                    first = false;
                } else {
                    out.accept(",");
                }
                write(out, e.getKey());
                out.accept(":");
                write(out, e.getValue());
            }
            out.accept("}");
        } else if (json instanceof String) {
            out.accept(new ClusterCommands.CommandToken(ClusterCommands.CommandTokenType.String, (String) json).toSource());
        } else if (json instanceof Number) {
            out.accept(Objects.toString(json));
        } else if (json instanceof Boolean) {
            out.accept(Objects.toString(json));
        } else {
            out.accept(new ClusterCommands.CommandToken(ClusterCommands.CommandTokenType.String, Objects.toString(json)).toSource());
        }
    }
}
