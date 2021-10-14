package csl.example.kelp;

import csl.actor.*;
import csl.actor.cluster.ActorPlacement;
import csl.example.TestToolRemote;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ActorPlacementKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.util.ResponsiveCalls;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;
import csl.actor.util.StagingActor;

import java.util.Arrays;

public class ExampleActorPlacementKelp {
    public static ActorSystemRemote createSystem() {
        return new ActorSystemRemote(new ActorSystemDefault.ActorSystemDefaultUnlimited(), KryoBuilder.builder());
    }

    public static void main(String[] args) throws Exception {
        ActorSystemRemote system = createSystem();
        ResponsiveCalls.initCallableTarget(system);
        int serverPort = 10000;
        system.startWithoutWait(serverPort);

        ActorPlacementKelp<ConfigKelp> p = new ActorPlacementKelp<>(system,
                new ActorPlacement.PlacementStrategyRoundRobin(0));

        TestToolRemote.setMvnClasspath();
        TestToolRemote.launchJava("-Dcsl.actor.debug.color=106", Follower.class.getName(), "10001", Integer.toString(serverPort));
        Thread.sleep(5000);

        ActorRefRemote.get(system, "localhost", 10001, "recv")
                .tell(ActorRefRemote.get(system, "localhost", 10001, "recv"), null);

        TestActor a = new TestActor(system, "hello", new ConfigKelp());

        ActorRef ref = ResponsiveCalls.sendTask(system, a, TestActor::move).get();
        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < 20; ++j) {
                ref.tell(i, null);
            }
        }

        StagingActor.staging(system).start(ref).get();
        p.close();
        Thread.sleep(3000);
        system.close();
    }

    public static class TestActor extends ActorKelp<TestActor> {
        @TransferredState long[] model;
        public TestActor(ActorSystem system, String name, ConfigKelp config) {
            super(system, name, config);
            model = new long[1000];
            Arrays.fill(model, 123);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(Integer.class, i-> i)
                    .forEachKeyList(5, (k,vs) -> config.log("%s : %s -> %s", getSystem(), k, vs))
                    .matchWithSender(String.class, this::info)
                    .build();
        }

        public ActorRef move() {
            ActorRef ref = getPlacement().place(this);
            config.log("%s : place -> %s", this, ref);
            return ref;
        }

        public void info(String n, ActorRef sender) {
            System.err.println(getSystem() + ": " + n + " from: " + sender);
        }
    }


    public static class Follower {
        public static void main(String[] args) throws Exception {
            ActorSystemRemote system = createSystem();
            ResponsiveCalls.initCallableTarget(system);
            int port = Integer.parseInt(args[0]);
            int joinPort = Integer.parseInt(args[1]);
            system.startWithoutWait(port);

            new RecvActor(system, "recv");

            ActorPlacementKelp<ConfigKelp> p = new ActorPlacementKelp<>(system,
                    new ActorPlacement.PlacementStrategyUndertaker());
            p.setShutdownWaitMsAfterAllMembersLeft(2000);
            Thread.sleep(3000);
            p.join(ActorAddress.get("localhost", joinPort));
        }
    }

    static class RecvActor extends ActorDefault {
        public RecvActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchWithSender(ActorRef.class, this::recv)
                    .build();
        }

        void recv(ActorRef r, ActorRef s) {
            getSystem().getLogger().log("%s ! %s from %s", this, r, s);
        }
    }
}
