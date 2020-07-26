package csl.actor.example.kelp;

import csl.actor.ActorBehavior;
import csl.actor.ActorSystem;
import csl.actor.kelp_old.PhaseShift;
import csl.actor.kelp_old.ActorKelp;
import csl.actor.kelp_old.ActorPlacementKelp;
import csl.actor.kelp_old.ClusterKelp;
import csl.actor.kelp_old.Config;

import java.nio.file.Paths;
import java.util.Scanner;
import java.util.function.Function;

public class ExampleCluster {
    public static void main(String[] args) throws Exception {
        String dir = Paths.get("").toAbsolutePath().toString() + "/target/debug";
        String debugFlag = "false";

        ClusterKelp d = ClusterKelp.create();
        ActorPlacementKelp place = d.deploy(d.primary()
                    .edit(c -> c.getDeploymentConfig().baseDir = dir)
                    .edit(c -> c.getAppConfig().routerAutoMerge = false)
                    .edit(c -> c.getDeploymentConfig().httpHost = "0.0.0.0"),
                d.node("localhost", 30001)
                    .edit(c -> c.getDeploymentConfig().baseDir = dir)
                    .edit(c -> c.getDeploymentConfig().java = "java -Dcsl.actor.debug=" + debugFlag + " -Dcsl.actor.debugMsg=" + debugFlag + " %s %s %s"),
                d.node("localhost", 30002)
                    .edit(c -> c.getDeploymentConfig().baseDir = dir)
                    .edit(c -> c.getDeploymentConfig().java = "java -Dcsl.actor.debug=" + debugFlag + " -Dcsl.actor.debugMsg=" + debugFlag + " %s %s %s"));

        TestSource s = new TestSource(d.getSystem(), "source", d.getPrimaryConfig());
        TestActor a = new TestActor(d.getSystem(), "test", d.getPrimaryConfig());
//        Random rand = new Random();

        place.connectStage(s, a).get();

        Scanner scn = new Scanner(System.in);
        while (true) {
            System.out.print(">");
            String line = scn.nextLine();
            if (line.equals("exit") || line.equals("quit")) {
                break;
            } else if (line.startsWith("test ")) {
                int n = Integer.parseInt(line.split(" ")[1]);
                /*
                try {
                    for (int i = 0; i < n; ++i) {
                        a.tell(Integer.toString(rand.nextInt(n)));
                    }
                    PhaseShift.start(d.getSystem(), a);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }*/
                s.tell(n);
                PhaseShift.start(d.getSystem(), s).get();
            } else if (line.startsWith("split ")) {
                int n = Integer.parseInt(line.split(" ")[1]);
                a.routerSplitOrMerge(n).get();

            } else if (line.startsWith("stats ")) {
                String p = line.split(" ")[1];
                ClusterKelp.RouterSplitStat o = d.getSplit(a, p);
                ClusterKelp.ActorStat as = d.getActorStat(o.actor);
                Object json = d.getHttp().jsonConverter(Object.class).apply(as);
                System.out.println(json);
            }
        }
        d.shutdownAll();
    }

    public static class TestSource extends ActorKelp<TestSource> {
        public TestSource(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        public TestSource(ActorSystem system, String name, Config config, State state) {
            super(system, name, config, state);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(Integer.class, this::start)
                    .build();
        }

        public void start(int n) {
            try {
                for (int i = 0; i < n; ++i) {
                    nextStage().tell("n" + i);
                    Thread.sleep(1000);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static class TestActor extends ActorKelp<TestActor> {
        public TestActor(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        public TestActor(ActorSystem system, String name, Config config, State state) {
            super(system, name, config, state);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(String.class, Function.identity(), s -> new Tuple(s, 1))
                    .fold((k,v) -> v.stream().reduce((a,b) -> new Tuple(a.value, a.count + b.count))
                            .orElse(new Tuple("",0)))
                    .forEach(o -> {})
                    .build();
        }
    }

    public static class Tuple {
        String value;
        int count;

        public Tuple(String value, int count) {
            this.value = value;
            this.count = count;
        }

        @Override
        public String toString() {
            return "Tuple{" +
                    "value=" + value +
                    ", count=" + count +
                    '}';
        }
    }
}
