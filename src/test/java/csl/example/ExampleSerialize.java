package csl.example;

import csl.actor.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;

import java.io.File;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.regex.Pattern;

public class ExampleSerialize {
    public static void main(String[] args)throws Exception {
        new ExampleSerialize().run();
    }

    public void run() throws Exception {
        ActorSystemRemote sys = new ActorSystemRemote();
        sys.setServerAddress(ActorAddress.get("hello-world", 12345));
        KryoBuilder.SerializerFunction k = sys.getSerializer();

        TestToolSerialize ts = new TestToolSerialize();

        ts.writeRead(k, "hello");
        ts.writeRead(k, 12345);
        ts.writeRead(k, Boolean.TRUE);
        ts.writeRead(k, new int[] {10, 20, 30}, Arrays::equals);
        ts.writeRead(k, new String[] {"hello", "world"}, Arrays::equals);
        ts.writeRead(k, new File("hello/world"));
        ts.writeRead(k, new BigInteger(Long.MAX_VALUE + "" + Long.MAX_VALUE));
        ts.writeRead(k, getClass());
        ts.writeRead(k, OffsetDateTime.now());
        ts.writeRead(k, ZonedDateTime.now());
        ts.writeRead(k, Instant.now());
        ts.writeRead(k, Duration.ofSeconds(Integer.MAX_VALUE, 1000));
        ts.writeRead(k, Arrays.asList("hello", "world"));
        ts.writeRead(k, new HashSet<>(Arrays.asList("hello", "world")));
        ts.writeRead(k, Integer.valueOf(12345678));

        ts.writeRead(k, null);

        Map<String,Object> map = new HashMap<>();
        map.put("hello", 123);
        map.put("world", 456);
        ts.writeRead(k, map);
        ts.writeRead(k, new URL("http://www.w3c.org"));
        ts.writeRead(k, ByteBuffer.wrap(new byte[] {1, 2, 3}));
        ts.writeRead(k, Pattern.compile("hello.*?world"), (l,r) -> l.toString().equals(r.toString()));

        EnumMap<MyEnum,String> em = new EnumMap<>(MyEnum.class);
        em.put(MyEnum.Hello, "hello");
        em.put(MyEnum.World, "world");
        ts.writeRead(k, em);

        BitSet bs = new BitSet(130);
        bs.set(10);
        bs.set(20);
        bs.set(129);
        ts.writeRead(k, bs);

        ts.writeRead(k, ActorAddress.get("hello", 12345));

        ActorRef a = new ExampleActor(sys, "hello");
        ts.writeRead(k, a, (l,r) -> {
            System.err.println("ExampleActor de-serialized form: " + l + " vs " + r);
            if (r instanceof ActorRefRemote) {
                ActorAddress ad = ((ActorRefRemote) r).getAddress();
                return ad.equals(sys.getServerAddress().getActor("hello"));
            } else {
                return l.equals(r);
            }
        });

        ActorSystemRemote.TransferredMessage msg = new ActorSystemRemote.TransferredMessage(123,
                ActorRefRemote.get(sys, "hello-world", 33333, "hello"));
        ts.writeRead(k, msg, (l,r) ->
            l.id == r.id && l.body.equals(r.body));

        ActorSystemRemote.TransferredMessage msg2 = new ActorSystemRemote.TransferredMessage(123,
                new Message<>(ActorRefRemote.get(sys, "hello-world", 33333, "hello"), null, "hello"));
        ts.writeRead(k, msg2, (l,r) ->
                l.id == r.id && ((Message<?>)l.body).getTarget().equals(((Message<?>)r.body).getTarget())
                        && ((Message<?>)l.body).getData().equals(((Message<?>)r.body).getData()));

        ////
        Container c = new Container();
        c.items.add(new ContElem("aaa"));
        c.items.add(new ContElemEx("bbb"));
        ts.writeRead(k, c, (l, r) ->
            l.items.size() == r.items.size() &&
                    l.items.get(0).value.equals(r.items.get(0).value) &&
                    l.items.get(1).value.equals(r.items.get(1).value) &&
                    ((ContElemEx) l.items.get(1)).exValue.value.equals(((ContElemEx) r.items.get(1)).exValue.value));
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

    public enum MyEnum {
        Hello, World
    }

    public static class Container {
        public List<ContElem> items = new ArrayList<>();
    }

    public static class ContElem {
        public String value;

        public ContElem(String value) {
            this.value = value;
        }
    }

    public static class ContElemEx extends ContElem {
        public ContElem exValue;
        public ContElemEx(String value) {
            super(value);
            exValue = new ContElem("<" + value + ">");
        }
    }
}
