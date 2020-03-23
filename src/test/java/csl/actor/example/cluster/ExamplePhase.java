package csl.actor.example.cluster;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.keyaggregate.ActorKeyAggregation;
import csl.actor.cluster.PhaseShift;

import java.time.Instant;

public class ExamplePhase {
    public static void main(String[] args) {

        ActorSystem system = new ActorSystemDefault();

        MyActor2 a = new MyActor2(system);
        a.tell("aaa");
        a.tell("bbb");
        a.tell("ccc");
        a.tell(new PhaseShift("end"));


        MyActor b = new MyActor(system);
        b.next = a;
        b.tell("aaa");
        b.tell("bbb");
        b.tell("ccc");
        new PhaseShift.PhaseTerminalActor(system, true).start("end2", b, Instant.now());
    }

    public static class MyActor extends ActorKeyAggregation {
        ActorRef next;
        public MyActor(ActorSystem system) {
            super(system);
        }

        long count;

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, s -> count += s.length())
                    .match(PhaseShift.PhaseCompleted.class, s -> s.redirectTo(next))
                    .build();
        }
    }

    public static class MyActor2 extends ActorKeyAggregation {
        public MyActor2(ActorSystem system) {
            super(system);
        }

        long count;

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, s -> count+=s.length())
                    .match(PhaseShift.PhaseCompleted.class, PhaseShift.PhaseCompleted::sendToTarget)
                    .build();
        }
    }
}
