package csl.example.kelp;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorBehavior;
import csl.actor.ActorSystem;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ActorKelpSerializable;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.behavior.KeyHistogramsPersistable;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.util.StagingActor;

import java.util.Arrays;

public class ExampleActorKelpSerializable {
    public static void main(String[] args) throws Exception {
        ActorSystemRemote r = new ActorSystemKelp();

        ConfigKelp conf = new ConfigKelp();
        conf.persist = true;
        TestActor m = new TestActor(r, "r", conf);
        for (int i = 0; i < 100; ++i) {
            m.tell("hello" + i);
            m.tell("world" + i);
        }
        StagingActor.staging(r).start(m).get();

        ActorKelpSerializable<TestActor> s = m.toSerializable();

        Output out = new Output(4096 * 1000);
        r.getSerializer().write(out, s);

        Input in = new Input(out.getBuffer());
        ActorKelpSerializable<?> s2 = (ActorKelpSerializable<?>) r.getSerializer().read(in);
        ActorKelp<?> a = s2.restore(r, 2, s2.config);

        KeyHistogramsPersistable.HistogramTreePersistable p = (KeyHistogramsPersistable.HistogramTreePersistable) a.getMailboxAsKelp().getHistogram(0);
        System.out.println(Arrays.toString(p.getHistory().totalMean()));

        StagingActor.staging(r).start(a).get();
        r.close();
    }

    public static class TestActor extends ActorKelp<TestActor> {
        @TransferredState int value;
        public TestActor(ActorSystem system, String name, ConfigKelp config) {
            super(system, name, config);
        }


        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(String.class, k->k)
                    .forEach(this::receive)
                    .build();
        }

        void receive(String s) {
            value += s.hashCode();
        }
    }
}
