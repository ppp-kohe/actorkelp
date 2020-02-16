package csl.actor.example.wordcount;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.msgassoc.ActorAggregationReplicable;
import csl.actor.msgassoc.PhaseShift;

public class PhaseExample {
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
        new PhaseShift.PhaseFinishActor(system, true).start("end2", b);
    }

    public static class MyActor extends ActorAggregationReplicable {
        ActorRef next;
        public MyActor(ActorSystem system) {
            super(system);
        }

        long count;

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, s -> count += s.length())
                    .match(PhaseShift.PhaseShiftCompleted.class, s -> s.redirectTo(next))
                    .build();
        }
    }

    public static class MyActor2 extends ActorAggregationReplicable {
        public MyActor2(ActorSystem system) {
            super(system);
        }

        long count;

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, s -> count+=s.length())
                    .match(PhaseShift.PhaseShiftCompleted.class, PhaseShift.PhaseShiftCompleted::sendToTarget)
                    .build();
        }
    }
}
