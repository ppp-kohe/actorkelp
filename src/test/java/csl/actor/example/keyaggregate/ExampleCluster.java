package csl.actor.example.keyaggregate;

import csl.actor.ActorBehavior;
import csl.actor.ActorSystem;
import csl.actor.cluster.PhaseShift;
import csl.actor.keyaggregate.ActorKeyAggregation;
import csl.actor.keyaggregate.ClusterKeyAggregation;
import csl.actor.keyaggregate.Config;

import java.nio.file.Paths;
import java.util.Random;
import java.util.Scanner;
import java.util.function.Function;

public class ExampleCluster {
    public static void main(String[] args) {
        String dir = Paths.get("").toAbsolutePath().toString() + "/target/debug";
        String debugFlag = "false";

        ClusterKeyAggregation d = ClusterKeyAggregation.create();
        d.deploy(d.master()
                    .edit(c -> c.getDeploymentConfig().baseDir = dir)
                    .edit(c -> c.getAppConfig().routerAutoMerge = false)
                    .edit(c -> c.getDeploymentConfig().httpHost = "0.0.0.0"),
                d.node("localhost", 30001)
                    .edit(c -> c.getDeploymentConfig().baseDir = dir)
                    .edit(c -> c.getDeploymentConfig().java = "java -Dcsl.actor.debug=" + debugFlag + " -Dcsl.actor.debugMsg=" + debugFlag + " %s %s %s"),
                d.node("localhost", 30002)
                    .edit(c -> c.getDeploymentConfig().baseDir = dir)
                    .edit(c -> c.getDeploymentConfig().java = "java -Dcsl.actor.debug=" + debugFlag + " -Dcsl.actor.debugMsg=" + debugFlag + " %s %s %s"));

        TestActor a = new TestActor(d.getSystem(), "test", d.getMasterConfig());
        Random rand = new Random();

        Scanner scn = new Scanner(System.in);
        while (true) {
            System.out.print(">");
            String line = scn.nextLine();
            if (line.equals("exit") || line.equals("quit")) {
                break;
            } else if (line.startsWith("test ")) {
                try {
                    int n = Integer.parseInt(line.split(" ")[1]);
                    for (int i = 0; i < n; ++i) {
                        a.tell(Integer.toString(rand.nextInt(n)));
                    }
                    PhaseShift.start(d.getSystem(), a);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            } else if (line.startsWith("stats ")) {
                String p = line.split(" ")[1];
                ClusterKeyAggregation.RouterSplitStat o = d.getSplit(a, p);
                ClusterKeyAggregation.ActorStat as = d.getActorStat(o.actor);
                Object json = d.getHttp().jsonConverter(Object.class).apply(as);
                System.out.println(json);
            }
        }
        d.shutdownAll();
    }

    public static class TestActor extends ActorKeyAggregation {
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
