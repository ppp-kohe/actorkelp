package csl.example.kelp;

import csl.actor.ActorBehavior;
import csl.actor.ActorSystem;
import csl.actor.kelp.*;
import csl.actor.util.StagingActor;

import java.nio.file.Paths;

public class ExampleCluster {
    public static void main(String[] args) throws Exception {
        int n = args.length == 0 ? 10_000 : Integer.parseInt(args[0].replaceAll("_", ""));

        String dir = Paths.get("").toAbsolutePath().toString() + "/target/debug";
        String debugFlag = "false";

        ClusterKelp<ConfigKelp> d = ClusterKelp.create();
        d.deployUnits(d.primary()
                    .edit(c -> c.getDeploymentConfig().baseDir = dir)
                    .edit(c -> c.getDeploymentConfig().httpHost = "0.0.0.0"),
                d.node("localhost", 30001)
                    .edit(c -> c.getDeploymentConfig().baseDir = dir)
                    .edit(c -> c.getDeploymentConfig().javaVmOption = "-Dcsl.actor.debug=" + debugFlag + " -Dcsl.actor.debugMsg=" + debugFlag),
                d.node("localhost", 30002)
                    .edit(c -> c.getDeploymentConfig().baseDir = dir)
                    .edit(c -> c.getDeploymentConfig().javaVmOption = "-Dcsl.actor.debug=" + debugFlag + " -Dcsl.actor.debugMsg=" + debugFlag));

        TestSource s = new TestSource(d.getSystem(), "source", d.getPrimaryConfig());
        TestActor a = new TestActor(d.getSystem(), "test", d.getPrimaryConfig());

        KelpStage<TestActor> tests = s.connects(a);

        ///input
        s.tell(n);

        StagingActor.staging(d.getSystem())
                .start(s)
                .get();

        ///merge
        tests.merge();
//            String p = line.split(" ")[1];
//            ClusterKelp.RouterSplitStat o = d.getSplit(a, p);
//            ClusterKelp.ActorStat as = d.getActorStat(o.actor);
//            Object json = d.getHttp().jsonConverter(Object.class).apply(as);
//            System.out.println(json);

        d.shutdownAll();
    }

    public static class TestSource extends ActorKelp<TestSource> {
        public TestSource(ActorSystem system, String name, ConfigKelp config) {
            super(system, name, config);
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
                    nextStageActor().tell("n" + i);
                    //Thread.sleep(100);
                }
                flush();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static class TestActor extends ActorKelp<TestActor> {
        public TestActor(ActorSystem system, String name, ConfigKelp config) {
            super(system, name, config);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(String.class, ActorKelpFunctions.KeyExtractorFunction.identity(), s -> new Tuple(s, 1))
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
