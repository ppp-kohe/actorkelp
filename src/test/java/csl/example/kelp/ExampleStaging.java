package csl.example.kelp;

import csl.actor.*;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.kelp.KelpStageGraphActor;
import csl.actor.util.Staging;
import csl.example.TestTool;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ExampleStaging {
    public static void main(String[] args) throws Exception {
        ActorSystem system = new ActorSystemKelp.ActorSystemDefaultForKelp();

        MyActor a = new MyActor(system, "a");
        a.tell("aaa");
        a.tell("bbb");
        a.tell("ccc");

        KelpStageGraphActor.get(system, a)
                .startAwait().get();

        TestTool.assertEquals("MyActor", 9L, a.count.get());

        System.err.println("--------------");

        a.count.set(0);
        MyActor b = new MyActor(system, "b");
        b.setNext(a);
        b.tell("aaa");
        b.tell("bbb");
        b.tell("ccc");

        KelpStageGraphActor.get(system, b)
                .withStageEndHandler((g,n) -> {
                    handlerExecuted1 = true;
                    g.logStatusAll("completed with handler A");
                    g.logStatusDetail();
                })
                .startAwait().get();

        TestTool.assertEquals("b.count", 9L, b.count.get());
        TestTool.assertTrue("handler", handlerExecuted1);

        System.err.println("--------------");

        MyActor2 c = new MyActor2(system, "stage2");
        MyActor3 d = new MyActor3(system);
        c.setNext(d);
        c.tell("aaa");
        c.tell("bbb");
        c.tell("ccc");

        KelpStageGraphActor.get(system, c)
                        .withStageEndHandler((g, n) -> {
                            if (n.getName().equals("stage3")) {
                                handlerExecuted2 = true;
                                g.logStatusAll("completed with handler B");
                            }
                            n.getActors().forEach(ma -> ma.tell(new OptionalMsg()));
                            g.logStatusDetail();
                        })
                        .startAwait().get();

        TestTool.assertTrue("c.completed", c.completed.get());
        TestTool.assertTrue("d.completed", d.completed.get());
        TestTool.assertTrue("c.next.completed", ((MyActor2) d.next).completed.get());
        TestTool.assertTrue("handler", handlerExecuted2);
        system.close();
    }

    static volatile boolean handlerExecuted1 = false;
    static volatile boolean handlerExecuted2 = false;

    public static class MyActor extends ActorDefault implements Staging.StagingSupported {
        protected ActorRef next;
        public MyActor(ActorSystem system, String name) {
            super(system, name);
        }

        public void setNext(ActorRef next) {
            this.next = next;
        }

        @Override
        public ActorRef nextStageActor() {
            return next;
        }

        AtomicLong count = new AtomicLong();

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, s -> count.addAndGet(s.length()))
                    .build();
        }
    }


    public static class MyActor2 extends ActorDefault implements Staging.StagingSupported {
        protected ActorRef next;
        public MyActor2(ActorSystem system, String name) {
            super(system, name);
        }

        AtomicLong count = new AtomicLong();
        AtomicBoolean completed = new AtomicBoolean();

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
                    .match(String.class, s -> count.addAndGet(s.length()))
                    .match(OptionalMsg.class, this::complete)
                    .build();
        }

        public void complete(OptionalMsg comp) { //custom handler
            getSystem().getLogger().log("completed: %s, this=%s, count=%,d %s", comp, this, count.get(),
                    Thread.currentThread());
            completed.set(true);
        }
    }

    public static class MyActor3 extends ActorDefault implements Staging.StagingSupported {
        ActorRef next;
        AtomicBoolean completed = new AtomicBoolean();

        public MyActor3(ActorSystem system) {
            super(system, "stage3");
            next = new MyActor2(system, "stage3-2");
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
                    .match(OptionalMsg.class, m -> {
                        system.getLogger().log("comp: %s", m);
                        completed.set(true);
                    })
                .build();
        }
    }
    public static class OptionalMsg { }
}
