package csl.actor.example;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorBehavior;
import csl.actor.ActorDefault;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.remote.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

public class SerializeExample {
    public static void main(String[] args)throws Exception {
        new SerializeExample().run();
    }

    public void run() throws Exception {
        ActorSystemRemote sys = new ActorSystemRemote();
        sys.setServerAddress(ActorAddress.get("hello-world", 12345));
        KryoBuilder.SerializerFunction k = sys.getSerializer();

        writeRead(k, "hello");
        writeRead(k, 12345);
        writeRead(k, Boolean.TRUE);
        writeRead(k, new int[] {10, 20, 30}, Arrays::equals);
        writeRead(k, new String[] {"hello", "world"}, Arrays::equals);
        writeRead(k, new File("hello/world"));
        writeRead(k, new BigInteger(Long.MAX_VALUE + "" + Long.MAX_VALUE));
        writeRead(k, getClass());
        writeRead(k, OffsetDateTime.now());
        writeRead(k, ZonedDateTime.now());
        writeRead(k, Instant.now());
        writeRead(k, Duration.ofSeconds(Integer.MAX_VALUE, 1000));
        writeRead(k, Arrays.asList("hello", "world"));
        writeRead(k, new HashSet<>(Arrays.asList("hello", "world")));

        writeRead(k, null);

        Map<String,Object> map = new HashMap<>();
        map.put("hello", 123);
        map.put("world", 456);
        writeRead(k, map);
        writeRead(k, new URL("http://www.w3c.org"));
        writeRead(k, ByteBuffer.wrap(new byte[] {1, 2, 3}));
        writeRead(k, Pattern.compile("hello.*?world"), (l,r) -> l.toString().equals(r.toString()));

        EnumMap<MyEnum,String> em = new EnumMap<>(MyEnum.class);
        em.put(MyEnum.Hello, "hello");
        em.put(MyEnum.World, "world");
        writeRead(k, em);

        BitSet bs = new BitSet(130);
        bs.set(10);
        bs.set(20);
        bs.set(129);
        writeRead(k, bs);

        writeRead(k, ActorAddress.get("hello", 12345));

        ActorRef a = new ExampleActor(sys, "hello");
        writeRead(k, a, (l,r) -> {
            System.out.println("ExampleActor de-serialized form: " + l + " vs " + r);
            if (r instanceof ActorRefRemote) {
                ActorAddress ad = ((ActorRefRemote) r).getAddress();
                return ad.equals(sys.getServerAddress().getActor("hello"));
            } else {
                return l.equals(r);
            }
        });

        ActorSystemRemote.TransferredMessage msg = new ActorSystemRemote.TransferredMessage(123,
                ActorRefRemote.get(sys, "hello-world", 33333, "hello"));
        writeRead(k, msg, (l,r) ->
            l.id == r.id && l.body.equals(r.body));
    }

    public static class ExampleActor extends ActorDefault {
        public ExampleActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder().build();
        }
    }

    enum MyEnum {
        Hello, World
    }

    public <E> void writeRead(KryoBuilder.SerializerFunction k, E obj) {
        writeRead(k, obj, Objects::equals);
    }

    @SuppressWarnings("unchecked")
    public <E> void writeRead(KryoBuilder.SerializerFunction k, E obj, BiPredicate<E,E> p) {
        System.out.println("----------- " + (obj == null ? "null" : obj.getClass().getName()));
        byte[] data = write(o -> k.write(o, obj));
        print(data);
        E r = (E) read(data, k::read);
        System.out.println(r);
        System.out.println(p.test(obj, r) ? formatColor(76,"[OK]") : formatColor(196, "DIFF"));
    }

    private String formatColor(int c, String s) {
        return String.format("\033[38;5;%dm%s\033[0m",c, s);
    }

    public byte[] write(Consumer<Output> p) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            Output output = new Output(out);
            p.accept(output);
            output.flush();
            out.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return out.toByteArray();
    }

    public void print(byte[] data) {
        StringBuilder buf = new StringBuilder();
        for (byte b : data) {
            buf.append(String.format("%2h", 0xFF & b)).append(":");
            char c = (char) b;
            if (Character.isLetterOrDigit(c)) {
                buf.append("'");
                buf.append(c);
                buf.append("'");
            } else {
                buf.append(b);
            }
            buf.append("  ");
        }
        System.out.println(buf);
    }

    public Object read(byte[] d, Function<Input, Object> gen) {
        Input input = new Input(new ByteArrayInputStream(d));
        return gen.apply(input);
    }
}
