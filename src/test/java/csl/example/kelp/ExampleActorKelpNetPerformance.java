package csl.example.kelp;

import csl.actor.ActorBehaviorBuilder;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ClusterKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.behavior.ActorBehaviorBuilderKelp;
import csl.actor.persist.PersistentConditionMailbox;
import csl.example.TestTool;

import java.io.Serializable;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ExampleActorKelpNetPerformance {
    public static void main(String[] args) throws Exception {
        ClusterKelp<ConfigKelp> c = ClusterKelp.create();

        String dir = Paths.get("").toAbsolutePath().toString() + "/target/debug";
//        var k = c.deploy();
        var k = c.deployUnits(
                c.primary()
                        .edit(u -> u.getDeploymentConfig().baseDir = dir)
                        .edit(u -> u.getAppConfig().shufflePartitions = 1),
                c.node("localhost", 30001)
                        .edit(u -> u.getDeploymentConfig().baseDir = dir)
                        .edit(u -> u.getAppConfig().shufflePartitions = 1),
                        //.edit(u -> u.getDeploymentConfig().javaVmOption = "-Dcsl.actor.kelp.debug=true"),
                c.node("localhost", 30002)
                        .edit(u -> u.getDeploymentConfig().baseDir = dir)
                        .edit(u -> u.getAppConfig().shufflePartitions = 1));
                        //.edit(u -> u.getDeploymentConfig().javaVmOption = "-Dcsl.actor.kelp.debug=true"));

        TestSrc sl = new TestSrc(k, "src", c.config());
        TestDest dl = new TestDest(k, "dst", c.config());

        var s = sl.shuffle();
        var d = s.connects(dl);

        Thread.sleep(3000);

        k.getLogger().log("SRC=%s, DST=%s", s, d);

        var time = s.stageGraph()
                .withLogPeriodic(Duration.ofSeconds(2))
                .startAwaitTell(5000)
                .get().getStageElapsedTime();

        k.getLogger().log("time: %s", time);

        TestTool.assertEquals("input:5000 x dataSize:10_000", 50_000_000L, d.merge().sum);

        c.shutdownAll();
    }

    public static class TestSrc extends ActorKelp<TestSrc> {
        public TestSrc(ActorSystem system, String name, ConfigKelp config) {
            super(system, name, config);
        }

        @Override
        protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilderKelp builder) {
            return builder.match(ActorRef.class, this::setNextStage)
                            .match(Integer.class, this::receive);
        }

        public void receive(Integer vs) {
            for (int i = 0; i < vs; ++i) {
                try {
                    List<String> data = new ArrayList<>();
                    for (int j = 0; j < 10_000; ++j) {
                        data.add(String.format("data %010d", j));
                    }
                    nextStageTell(new TestData(i, data));
                    flush();
                } catch (Throwable ex) {
                    getLogger().log("error: %,d", i);
                    ex.printStackTrace();
                }
            }
            getLogger().log("src finish %,d : %s", vs, nextStage);
        }

        @Override
        public void stageEnd() {
            super.stageEnd();
            getLogger().log("src end");
        }
    }

    public static class TestData implements Serializable {
        public int n;
        public List<String> data;

        public TestData() {}

        public TestData(int n, List<String> data) {
            this.n = n;
            this.data = data;
        }
    }

    public static class TestDest extends ActorKelp<TestDest> {
        @TransferredState public long sum;

        PersistentConditionMailbox.SampleTiming logTiming = new PersistentConditionMailbox.SampleTiming();
        public TestDest(ActorSystem system, String name, ConfigKelp config) {
            super(system, name, config);
        }

        @Override
        protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilderKelp builder) {
            return builder.match(TestData.class, this::receive);
        }

        public void receive(TestData d) {
            if (logTiming.next()) getLogger().log(165, "receive %,d : %,d", d.n, d.data.size());
            sum += d.data.size();
        }

        @Override
        public void stageEnd() {
            super.stageEnd();
            getLogger().log(165,"TestDest %,d", sum);
        }
    }
}
