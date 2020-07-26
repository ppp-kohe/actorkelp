package csl.actor.example.cluster;

import csl.actor.*;
import csl.actor.kelp_old.ActorKelp;
import csl.actor.kelp_old.PhaseShift;

import java.time.Instant;

public class ExamplePhase {
    public static void main(String[] args) throws Exception {

        ActorSystem system = new ActorSystemDefault();

        MyActor a = new MyActor(system);
        a.tell("aaa");
        a.tell("bbb");
        a.tell("ccc");
        a.tell(new PhaseShift("end"));

        PhaseShift.start(system, a).get(); //UUID key

        System.err.println("--------------");

        MyActor b = new MyActor(system);
        b.setNextStage(a);
        b.tell("aaa");
        b.tell("bbb");
        b.tell("ccc");
        new PhaseShift.PhaseTerminalActor(system, false, (sys,comp) -> {
            sys.getLogger().log("completed with handler: %s", comp);
        }).start("end2", b, Instant.now()).get();

        System.err.println("--------------");

        MyActor2 c = new MyActor2(system);
        c.setNextStage(new MyActor3(system));
        c.tell("aaa");
        c.tell("bbb");
        c.tell("ccc");
        PhaseShift.start(c.getSystem(), c).get();

        system.close();
    }

    public static class MyActor extends ActorKelp<MyActor> {
        public MyActor(ActorSystem system) {
            super(system);
        }

        long count;

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, s -> count+=s.length())
                    .build();
        }
    }


    public static class MyActor2 extends ActorKelp<MyActor2> {
        public MyActor2(ActorSystem system) {
            super(system);
        }

        long count;

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, s -> count += s.length())
                    .match(PhaseShift.PhaseCompleted.class, this::complete)
                    .build();
        }

        public void complete(PhaseShift.PhaseCompleted comp) { //custom handler
            log("completed: %s, this=%s, count=%,d", comp, this, count);
            comp.accept(this);
        }
    }

    public static class MyActor3 extends ActorDefault implements PhaseShift.StageSupported {
        ActorRef next;
        public MyActor3(ActorSystem system) {
            super(system);
            next = new MyActor2(system);
        }

        @Override
        public ActorRef nextStage() {
            return next;
        }

        @Override
        public void logPhase(String str, Object... args) {
            system.getLogger().log(String.format(str, args));
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
//                    .matchWithSender(PhaseShift.class, (m,s) -> {
//                        system.getLogger().log("shift: %s from %s", m, s);
//                        m.accept(this);
//                    })
                    .matchWithSender(PhaseShift.PhaseCompleted.class, (m,s) -> {
                        system.getLogger().log("comp: %s from %s", m, s);
                        m.accept(this);
                    })
                    .build();
        }
    }
}
