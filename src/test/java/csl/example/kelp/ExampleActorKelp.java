package csl.example.kelp;

import csl.actor.ActorBehavior;
import csl.actor.ActorSystem;
import csl.actor.kelp.*;
import csl.example.TestTool;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ExampleActorKelp {
    public static void main(String[] args) throws Exception {
        new ExampleActorKelp().runUnit();
        new ExampleActorKelp().runRouter();
    }

    public void runUnit() throws Exception {
        System.err.println("-------- runUnit");
        ActorSystem system = new ActorSystemKelp.ActorSystemDefaultForKelp();
        MyActor a = new MyActor(system, "a");
        test(a);
    }

    public void runRouter() throws Exception {
        System.err.println("-------- runRouter");
        ActorSystem system = new ActorSystemKelp.ActorSystemDefaultForKelp();
        MyActor a = new MyActor(system, "a");
        test(a.shuffle());
    }

    public void test(KelpStage<MyActor> a) throws Exception {
        ActorSystem system = a.getSystem();

        KelpStageGraphActor sg = a.stageGraph()
                .withLogPeriodic(Duration.ofMillis(500))
                .withStageEndHandler((g,n) -> g.logStatusDetail())
                .start();

        String key = "abcdefghijk";
        for (int i = 0; i < 100; ++i) {
            String k = "" + key.charAt(i % key.length());
            a.tellAndFlush(k + i);
            Thread.sleep(50);
        }

        sg.await().get();

        MyActor am = a.merge();

        am.record.forEach((k, v)->
                System.err.println(k + ": " + v));
        TestTool.assertEquals("count", 100, am.count);
        system.close();
    }

    private String formatColor(int c, String s) {
        return String.format("\033[38;5;%dm%s\033[0m",c, s);
    }

    public static class MyActor extends ActorKelp<MyActor> {
        @TransferredState(mergeType = MergerOpType.Add) public Map<String, List<String>> record = new LinkedHashMap<>();
        @TransferredState(mergeType = MergerOpType.Add) public int count;

        public MyActor(ActorSystem system, String name, ConfigKelp config) {
            super(system, name, config);
        }

        public MyActor(ActorSystem system, String name) {
            this(system, name, new ConfigKelp());
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(String.class, k -> "" + k.charAt(0))
                    .forEachKeyValue(this::process)
                    .build();
        }

        @Override
        public void initRestoreUnit() {
            record = new LinkedHashMap<>();
            count = 0;
        }

        public void process(String k, String v) {
            record.computeIfAbsent(k, (_k) -> new ArrayList<>())
                    .add(v);
            count++;
        }
    }
}
