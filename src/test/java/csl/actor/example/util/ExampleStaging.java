package csl.actor.example.util;

import csl.actor.*;
import csl.actor.cluster.PhaseShift;
import csl.actor.util.StagingActor;

import java.time.Instant;

public class ExampleStaging {
    public static void main(String[] args) throws Exception {

        ActorSystem system = new ActorSystemDefault();

        MyActor a = new MyActor(system);
        a.tell("aaa");
        a.tell("bbb");
        a.tell("ccc");

        StagingActor.staging(system)
                .start(a).get();

        System.err.println("--------------");

        MyActor b = new MyActor(system);
        b.setNext(a);
        b.tell("aaa");
        b.tell("bbb");
        b.tell("ccc");
        StagingActor.staging(system)
                .withStartTime(Instant.now())
                .withHandler((self,comp) -> {
                    self.getSystem().getLogger().log("completed with handler: %s", comp);
                }).start(b).get();

        System.err.println("--------------");

        MyActor2 c = new MyActor2(system);
        c.setNext(new MyActor3(system));
        c.tell("aaa");
        c.tell("bbb");
        c.tell("ccc");
        StagingActor.staging(c.getSystem())
                .start(c).get();

        system.close();
    }

    public static class MyActor extends ActorDefault implements StagingActor.StagingSupported {
        protected ActorRef next;
        public MyActor(ActorSystem system) {
            super(system);
        }

        public void setNext(ActorRef next) {
            this.next = next;
        }

        @Override
        public ActorRef nextStageActor() {
            return next;
        }

        long count;

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, s -> count+=s.length())
                    .build();
        }
    }


    public static class MyActor2 extends ActorDefault implements StagingActor.StagingSupported  {
        protected ActorRef next;
        public MyActor2(ActorSystem system) {
            super(system);
        }

        long count;

        public void setNext(ActorRef next) {
            this.next = next;
        }

        @Override
        public ActorRef nextStageActor() {
            return null;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, s -> count += s.length())
                    .match(StagingActor.StagingCompleted.class, this::complete)
                    .build();
        }

        public void complete(StagingActor.StagingCompleted comp) { //custom handler
            getSystem().getLogger().log("completed: %s, this=%s, count=%,d", comp, this, count);
            comp.accept(this);
        }
    }

    public static class MyActor3 extends ActorDefault implements StagingActor.StagingSupported {
        ActorRef next;
        public MyActor3(ActorSystem system) {
            super(system);
            next = new MyActor2(system);
        }

        public void setNext(ActorRef next) {
            this.next = next;
        }

        @Override
        public ActorRef nextStageActor() {
            return next;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchWithSender(StagingActor.StagingCompleted.class, (m, s) -> {
                        system.getLogger().log("comp: %s from %s", m, s);
                        m.accept(this);
                    })
                .build();
        }
    }
}
