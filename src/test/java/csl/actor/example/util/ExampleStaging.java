package csl.actor.example.util;

import csl.actor.*;
import csl.actor.example.TestTool;
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

        TestTool.assertEquals("MyActor", 9L, a.count);

        System.err.println("--------------");

        a.count = 0;
        MyActor b = new MyActor(system);
        b.setNext(a);
        b.tell("aaa");
        b.tell("bbb");
        b.tell("ccc");
        StagingActor.staging(system)
                .withStartTime(Instant.now())
                .withHandler((self,comp) -> {
                    handlerExecuted1 = true;
                    self.getSystem().getLogger().log("completed with handler: %s", comp);
                }).start(b).get();

        TestTool.assertEquals("b.count", 9L, b.count);
        TestTool.assertTrue("handler", handlerExecuted1);

        System.err.println("--------------");

        MyActor2 c = new MyActor2(system);
        MyActor3 d = new MyActor3(system);
        c.setNext(d);
        c.tell("aaa");
        c.tell("bbb");
        c.tell("ccc");
        StagingActor.staging(c.getSystem())
                .withHandler(MyActor3.class, (self) -> {
                    handlerExecuted2 = true;
                    self.getSystem().getLogger().log("completed with handler: %s", self);
                })
                .start(c).get();

        TestTool.assertTrue("c.completed", c.completed);
        TestTool.assertTrue("d.completed", d.completed);
        TestTool.assertTrue("c.next.completed", ((MyActor2) d.next).completed);
        TestTool.assertTrue("handler", handlerExecuted2);
        system.close();
    }

    static volatile boolean handlerExecuted1 = false;
    static volatile boolean handlerExecuted2 = false;

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
        volatile boolean completed;

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
                    .match(String.class, s -> count += s.length())
                    .match(StagingActor.StagingCompleted.class, this::complete)
                    .build();
        }

        public void complete(StagingActor.StagingCompleted comp) { //custom handler
            getSystem().getLogger().log("completed: %s, this=%s, count=%,d", comp, this, count);
            completed = true;
            comp.accept(this);
        }
    }

    public static class MyActor3 extends ActorDefault implements StagingActor.StagingSupported {
        ActorRef next;
        volatile boolean completed;

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
                        completed = true;
                        m.accept(this);
                    })
                .build();
        }
    }
}
