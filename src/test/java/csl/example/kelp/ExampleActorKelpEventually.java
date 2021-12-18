package csl.example.kelp;

import csl.actor.ActorBehaviorBuilder;
import csl.actor.ActorSystem;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.behavior.ActorBehaviorBuilderKelp;
import csl.actor.kelp.persist.HistogramTreePersistable;
import csl.actor.kelp.persist.KeyHistogramsPersistable;
import csl.actor.kelp.persist.PersistentConditionActor;
import csl.actor.persist.PersistentConditionMailbox;
import csl.example.TestTool;

import java.util.*;
import java.util.stream.IntStream;

public class ExampleActorKelpEventually {
    public static void main(String[] args) throws Exception {
        new ExampleActorKelpEventually().run();
        serializerFlag = true;
        new ExampleActorKelpEventually().run();
    }

    static boolean serializerFlag = false;

    public void run() throws Exception {
        ConfigKelp conf = new ConfigKelp();
        conf.shufflePartitions = 2;
        conf.mailboxPath = "target/debug/%a";
        try (ActorSystemKelp.ActorSystemDefaultForKelp k = ActorSystemKelp.createLocal(conf)) {
            InputGen in = new InputGen(k, "in", conf);
            MyActor a = new MyActor(k, "test", conf);
            var as = in.connects(a.asCheckpoint());

            int n = 100;
            in.stageGraph()
                    .startAwaitTell(n)
                    .get();

            var res = as.merge().data;
            TestTool.assertEquals("size", n, res.size());
            for (int i = 0; i < n; ++i)  {
                String ke = "v" + i;
                TestTool.assertEquals(ke, Set.of(1,2,3), res.get(ke));
            }
        }
    }

    public static final class Tuple {
        final String key;
        final int value;

        public Tuple(String key, int value) {
            this.key = key;
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public String getKey() {
            return key;
        }
        public static Tuple entry(String k, int v) {
            return new Tuple(k, v);
        }
    }

    public static class MyActor extends ActorKelp<MyActor> {
        @TransferredState(mergeType = MergerOpType.Add)
        public Map<String, Set<Integer>> data = new HashMap<>();
        public MyActor(ActorSystem system, String name, ConfigKelp config) {
            super(system, name, config);
        }

        @Override
        protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilderKelp builder) {
            var b = builder.matchKey(Tuple.class, Tuple::getKey, Tuple::getValue)
                    .eventually();
            if (serializerFlag) {
                b = b
                        .keyType(String.class).valueType(Integer.class);
            }
            return b
                    .forEachKeyValue(this::receive);
        }

        protected void receive(String k, Integer v) {
            getLogger().log("receive %s : %s", k, v);
            data.computeIfAbsent(k, _k -> new HashSet<>()).add(v);
        }

        @Override
        protected PersistentConditionActor initMemoryCondition() {
            PersistentConditionActor.SystemLoggerConditionActor logger = new PersistentConditionActor.SystemLoggerConditionActor(getSystem().getLogger(), this);
            return new PersistentConditionActor.PersistentConditionActorDefault(
                    new PersistentConditionMailbox.PersistentConditionMailboxNever(),
                    new KeyHistogramsPersistable.PersistentConditionHistogram() {
                        @Override
                        public KeyHistogramsPersistable.HistogramPersistentOperation needToPersist(String callerInfo, HistogramTreePersistable tree) {
                            return tree.getTreeSizeOnMemory() >= 110 ?
                                    KeyHistogramsPersistable.HistogramPersistentOperationType.FullTree:
                                    KeyHistogramsPersistable.HistogramPersistentOperationType.None;
                        }

                        @Override
                        public ActorSystem.SystemLogger getLogger() {
                            return logger;
                        }
                    },
                    new PersistentConditionActor.ReducedSizeDefault(logger), logger);
        }
    }

    public static class InputGen extends ActorKelp<InputGen> {
        public InputGen(ActorSystem system, String name, ConfigKelp config) {
            super(system, name, config);
        }

        @Override
        protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilderKelp builder) {
            return builder.match(Integer.class, v -> {
                IntStream.range(0, v).forEach(i -> nextStageTell(Tuple.entry("v" + i, 1)));
                IntStream.range(0, v).forEach(i -> nextStageTell(Tuple.entry("v" + i, 2)));
                IntStream.range(0, v).forEach(i -> nextStageTell(Tuple.entry("v" + i, 3)));
            });
        }
    }
}
