package csl.actor.example;

import csl.actor.*;

public class Example {
    public static void main(String[] args) {
        ActorSystemDefault sys = new ActorSystemDefault();
        ActorRef root = new MyActor(sys, null);
        ActorRef last = root;
        for (int i = 0; i < 100; ++i) {
            last = new MyActor(sys, last);
        }
        root.tell(last, root);
        root.tell(0, root);
    }

    static class MyActor extends ActorDefault {
        ActorRef ref;
        public MyActor(ActorSystem system, ActorRef link) {
            super(system);
            this.ref = link;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(Integer.class, this::hello)
                    .match(ActorRef.class, this::link)
                    .build();
        }

        public void link(ActorRef ref) {
            this.ref = ref;
        }

        public void hello(Integer n) {
            System.err.println("hello " + n + " " + this +" : " + Thread.currentThread().getName());
            try {
                Thread.sleep(500);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            if (n >= 50) {
                System.err.println("end");
                return;
            }
            if (ref != null) {
                ref.tell(n + 1, this);
            }
        }
    }
}
