package csl.actor.example.kelp;

import csl.actor.ActorBehavior;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.kelp_old.ActorKelp;
import csl.actor.kelp_old.Config;
import csl.actor.kelp_old.PhaseShift;

import java.util.*;
import java.util.function.BiPredicate;

public class ExampleActorKelp {
    public static void main(String[] args) throws Exception {
        new ExampleActorKelp().runUnit();
        new ExampleActorKelp().runRouter();
    }

    public void runUnit() throws Exception {
        System.err.println("-------- runUnit");
        ActorSystem system = new ActorSystemDefault();
        MyActor a = new MyActor(system, "a");
        a.setAsUnit();
        test(a);
    }

    public void runRouter() throws Exception {
        System.err.println("-------- runRouter");
        ActorSystem system = new ActorSystemDefault();
        MyActor a = new MyActor(system, "a");
        a.routerSplit(2);
        test(a);
    }

    public void test(MyActor a) throws Exception {
        ActorSystem system = a.getSystem();
        String key = "abcdefghijk";
        for (int i = 0; i < 100; ++i) {
            String k = "" + key.charAt(i % key.length());
            a.tell(k + i);
        }

        PhaseShift.start(system, a).get();

        a.routerSplitOrMerge(0).get();

        a.record.forEach((k, v)->
                System.err.println(k + ": " + v));
        check(a.count, 100, Objects::equals);
        system.close();
    }

    private <E> void check(E r, E obj, BiPredicate<E, E> p) {
        System.err.println(r);
        System.err.println(p.test(r, obj) ? formatColor(76,"[OK]") :  (formatColor(196, "DIFF") + " : " + obj));
    }

    private String formatColor(int c, String s) {
        return String.format("\033[38;5;%dm%s\033[0m",c, s);
    }

    public static class MyActor extends ActorKelp<MyActor> {
        public Map<String, List<String>> record = new LinkedHashMap<>();
        public int count;

        public MyActor(ActorSystem system, String name, Config config, State state) {
            super(system, name, config, state);
        }

        public MyActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(String.class, k -> "" + k.charAt(0))
                    .forEachKeyValue(this::process)
                    .build();
        }

        @Override
        protected void initClone(MyActor original) {
            record = new LinkedHashMap<>();
            count = 0;
        }

        @Override
        protected void initMerged(MyActor m) {
            record.putAll(m.record);
            count += m.count;
        }

        public void process(String k, String v) {
            record.computeIfAbsent(k, (_k) -> new ArrayList<>())
                    .add(v);
            count++;
        }
    }
}
