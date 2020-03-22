package csl.actor.example.keyaggregate;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorBehavior;
import csl.actor.ActorSystem;
import csl.actor.cluster.PhaseShift;
import csl.actor.keyaggregate.ActorKeyAggregation;
import csl.actor.keyaggregate.Config;
import csl.actor.keyaggregate.KeyHistogramsPersistable;
import csl.actor.remote.ActorSystemRemote;

import java.io.Serializable;
import java.util.Arrays;

public class ExamplePersist {
    public static void main(String[] args) throws Exception {
        ActorSystemRemote r = new ActorSystemRemote();

        Config conf = new Config();
        conf.persist = true;
        TestActor m = new TestActor(r, "r", conf);
        for (int i = 0; i < 100; ++i) {
            m.tell("hello" + i);
            m.tell("world" + i);
        }
        PhaseShift.start(r, m).get();

        ActorKeyAggregation.ActorKeyAggregationSerializable s = m.toSerializable(1);

        Output out = new Output(4096 * 1000);
        r.getSerializer().write(out, s);

        Input in = new Input(out.getBuffer());
        ActorKeyAggregation.ActorKeyAggregationSerializable s2 = (ActorKeyAggregation.ActorKeyAggregationSerializable) r.getSerializer().read(in);
        ActorKeyAggregation a = s2.create(r, 2);

        KeyHistogramsPersistable.HistogramTreePersistable p = (KeyHistogramsPersistable.HistogramTreePersistable) a.getMailboxAsKeyAggregation().getHistogram(0);
        System.out.println(Arrays.toString(p.getHistory().totalMean()));

        PhaseShift.start(r, a).get();
        r.close();
    }

    public static class TestActor extends ActorKeyAggregation {
        int value;
        public TestActor(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        public TestActor(ActorSystem system, String name, Config config, State state) {
            super(system, name, config, state);
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

        @Override
        protected Serializable toSerializableInternalState() {
            return value;
        }

        @Override
        protected void initSerializedInternalState(Serializable s) {
            this.value = (Integer) s;
        }
    }
}
