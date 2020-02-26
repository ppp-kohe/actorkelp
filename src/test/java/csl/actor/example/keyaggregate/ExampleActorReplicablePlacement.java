package csl.actor.example.keyaggregate;

import csl.actor.ActorBehavior;
import csl.actor.ActorDefault;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.cluster.ActorPlacement;
import csl.actor.cluster.ResponsiveCalls;
import csl.actor.example.ExampleRemote;
import csl.actor.keyaggregate.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;

import java.util.Arrays;

public class ExampleActorReplicablePlacement {
    public static void main(String[] args) throws Exception {
        ActorSystemRemote system = new ActorSystemRemote();
        ResponsiveCalls.initCallableTarget(system);
        int serverPort = 10000;
        system.startWithoutWait(serverPort);

        new ActorPlacementKeyAggregation(system,
                new ActorPlacement.PlacementStrategyRoundRobin(0));

        ExampleRemote.setMvnClasspath();
        ExampleRemote.launchJava("-Dcsl.actor.debug.color=106", Follower.class.getName(), "10001", Integer.toString(serverPort));
        Thread.sleep(15000);

        ActorRefRemote.get(system, "localhost", 10001, "recv")
                .tell(ActorRefRemote.get(system, "localhost", 10001, "recv"), null);

        TestActor a = new TestActor(system, "hello", new Config());
        ResponsiveCalls.sendTask(system, a, (s, f) -> {((TestActor) s).move(); return "";});

        a.routerSplit(2);

        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < 20; ++j) {
                a.tell(i, null);
            }
        }

    }

    public static class TestActor extends ActorKeyAggregation {
        long[] model;
        public TestActor(ActorSystem system, String name, Config config) {
            super(system, name, config);
            model = new long[1000];
            Arrays.fill(model, 123);
        }

        @Override
        protected void initClone(ActorKeyAggregation original) {
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
        public ActorKeyAggregationSerializable toSerializable(long num) {
            System.err.println("toSerializable");
            return super.toSerializable(num);
        }

        @Override
        protected ActorKeyAggregationSerializable newSerializableState() {
            return new ExampleActorReplicablePlacement.State(model);
        }

        public void move() {
            ActorRef ref = place(getPlacement(), this);
            System.err.println(getSystem() + ": " + ref);
            ref.tell("hello", this);
        }

        public void info(String n, ActorRef sender) {
            System.err.println(getSystem() + ": " + n + " from: " + sender);
        }
    }

    static class State extends ActorKeyAggregation.ActorKeyAggregationSerializable {
        long[] model;

        public State(long[] model) {
            this.model = model;
        }
    }


    public static class Follower {
        public static void main(String[] args) throws Exception {
            ActorSystemRemote system = new ActorSystemRemote();
            ResponsiveCalls.initCallableTarget(system);
            int port = Integer.parseInt(args[0]);
            int joinPort = Integer.parseInt(args[1]);
            system.startWithoutWait(port);

            new RecvActor(system, "recv");

            ActorPlacementKeyAggregation p = new ActorPlacementKeyAggregation(system,
                    new ActorPlacement.PlacementStrategyUndertaker());
            Thread.sleep(10000);
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
