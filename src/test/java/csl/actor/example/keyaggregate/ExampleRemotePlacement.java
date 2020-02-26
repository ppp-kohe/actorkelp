package csl.actor.example.keyaggregate;

import csl.actor.*;
import csl.actor.cluster.ActorPlacement;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;

import java.io.Serializable;
import java.time.Instant;

public class ExampleRemotePlacement {
    public static void main(String[] args) throws Exception {
        ActorPlacement.ActorPlacementDefault.debugLog = true;
        ActorSystemRemote.debugLog = true;

        //send callable
        try (ActorSystemRemote host1 = new ActorSystemRemote().startWithoutWait(50000);
             ActorSystemRemote host2 = new ActorSystemRemote().startWithoutWait(50001)) {
            MyPlacementActor p1 = new MyPlacementActor(host1);

            MyPlacementActor p2 = new MyPlacementActor(host2);
            p2.join(host1.getServerAddress());

            Thread.sleep(1000);

            System.out.println(Instant.now() + ": p1.cluster: "+  p1.getCluster());
            System.out.println(Instant.now() + ": p2.cluster: "+  p2.getCluster());

            MyActor a = new MyActor(host1, "a1");
            ActorRef r1 = p1.place(a);
            System.out.println(Instant.now() + ": " + r1);

            System.out.println(Instant.now() + ": finish");
        }
    }

    static class MyPlacementActor extends ActorPlacement.ActorPlacementDefault {
        public MyPlacementActor(ActorSystem system) {
            super(system);
        }

        @Override
        protected PlacementStrategy initStrategy() {
            return new PlacementStrategyRoundRobin(0);
        }

        @Override
        public Serializable toSerializable(Actor a, long num, Serializable previous, ActorAddress.ActorAddressRemoteActor target) {
            return a.getName();
        }

        @Override
        public Actor fromSerializable(Serializable s, long num) {
            String d = (String) s;
            return new MyActor(getSystem(), "deserialized-" + d);
        }
    }

    static class MyActor extends ActorDefault {
        public MyActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(Object.class, this::run)
                    .build();
        }

        void run(Object r) {
            System.out.println(Instant.now() + ": " + this + " receive " + r);
        }
    }
}
