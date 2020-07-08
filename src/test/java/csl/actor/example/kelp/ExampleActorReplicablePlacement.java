package csl.actor.example.kelp;

import csl.actor.*;
import csl.actor.cluster.ActorPlacement;
import csl.actor.cluster.PhaseShift;
import csl.actor.cluster.ResponsiveCalls;
import csl.actor.example.ExampleRemote;
import csl.actor.kelp.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;

import java.util.Arrays;

public class ExampleActorReplicablePlacement {
    public static ActorSystemRemote createSystem() {
        return new ActorSystemRemote(new ActorSystemDefault.ActorSystemDefaultUnlimited(), KryoBuilder.builder());
    }

    public static void main(String[] args) throws Exception {
        ActorSystemRemote system = createSystem();
        ResponsiveCalls.initCallableTarget(system);
        int serverPort = 10000;
        system.startWithoutWait(serverPort);

        ActorPlacementKelp p = new ActorPlacementKelp(system,
                new ActorPlacement.PlacementStrategyRoundRobin(0));

        ExampleRemote.setMvnClasspath();
        ExampleRemote.launchJava("-Dcsl.actor.debug.color=106", Follower.class.getName(), "10001", Integer.toString(serverPort));
        Thread.sleep(5000);

        ActorRefRemote.get(system, "localhost", 10001, "recv")
                .tell(ActorRefRemote.get(system, "localhost", 10001, "recv"), null);

        TestActor a = new TestActor(system, "hello", new Config());

        a.routerSplit(2).get();

        ActorRef ref = ResponsiveCalls.sendTask(system, a, TestActor::move).get();
        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < 20; ++j) {
                ref.tell(i, null);
            }
        }

        PhaseShift.start(system, ref).get();
        p.close();
        Thread.sleep(3000);
        system.close();
    }

    public static class TestActor extends ActorKelp<TestActor> {
        long[] model;
        public TestActor(ActorSystem system, String name, Config config) {
            super(system, name, config);
            model = new long[1000];
            Arrays.fill(model, 123);
        }

        public TestActor(ActorSystem system, String name, Config config, State state) {
            super(system, name, config, state);
            model = new long[1000];
            Arrays.fill(model, 123);
        }

        @Override
        protected void initClone(TestActor original) {
            super.initClone(original);
            System.err.println("clone");
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(Integer.class, i-> i)
                    .forEachKeyList(5, (k,vs) -> System.out.println(getSystem() + " : " + vs))
                    .matchWithSender(String.class, this::info)
                    .build();
        }

        @Override
        public ActorKelpSerializable toSerializable(long num) {
            System.err.println("toSerializable");
            return super.toSerializable(num);
        }

        @Override
        protected ActorKelpSerializable newSerializableState() {
            return new ExampleActorReplicablePlacement.State(model);
        }

        public ActorRef move() {
            ActorRef ref = place(getPlacement(), this);
            System.out.println(getSystem() + " : place " + ref);
            return ref;
        }

        public void info(String n, ActorRef sender) {
            System.err.println(getSystem() + ": " + n + " from: " + sender);
        }
    }

    static class State extends ActorKelp.ActorKelpSerializable {
        public static final long serialVersionUID = 1L;
        long[] model;

        public State(long[] model) {
            this.model = model;
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

            ActorPlacementKelp p = new ActorPlacementKelp(system,
                    new ActorPlacement.PlacementStrategyUndertaker());
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
            System.out.println(this + " ! " + r + " from " + s);
        }
    }
}
