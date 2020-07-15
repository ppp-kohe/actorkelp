package csl.actor.example;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorBehavior;
import csl.actor.ActorDefault;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.kelp2.ActorRefShuffle;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.function.Consumer;
import java.util.function.Function;

public class ExampleSerializeActorRefShuffle {
    public static void main(String[] args) {
        new ExampleSerializeActorRefShuffle().run();
    }

    public void run() {
        ActorSystemRemote sys = new ActorSystemRemote();
        sys.setServerAddress(ActorAddress.get("localhost", 3000));
        KryoBuilder.SerializerFunction k = sys.getSerializer();

        ActorRefShuffle s = ActorRefShuffle.createRef(10, n->
                new TestActor(sys, "a" + n));

        byte[] data = write(o -> k.write(o, s));
        ActorRefShuffle r = (ActorRefShuffle) read(data, k::read);
        System.out.println(r.getClass() + " : " + r);

        int i = 0;
        for (ActorRef sub : r.getActors()) {
            System.out.println("  " + i + " : " + sub);
            ++i;
        }
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

    public Object read(byte[] d, Function<Input, Object> gen) {
        Input input = new Input(new ByteArrayInputStream(d));
        return gen.apply(input);
    }

    public static class TestActor extends ActorDefault {
        public TestActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, this::receive)
                    .build();
        }

        public void receive(String str) {
            System.out.println(str);
        }
    }
}
