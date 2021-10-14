package csl.example.kelp;

import csl.actor.ActorBehavior;
import csl.actor.ActorDefault;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.remote.ActorRefRemote;

public class ExampleActorSystemKelp {
    public static void main(String[] args) throws Exception {
        ActorSystemKelp s1 = new ActorSystemKelp();
        ActorSystemKelp s2 = new ActorSystemKelp();

        //ActorSystemRemote.debugLogMsg = true;
        s1.startWithoutWait(30001);
        s2.startWithoutWait(30002);

        Thread.sleep(1000);

        TestActor a1 = new TestActor(s1, "a1");
        TestActor a2 = new TestActor(s2, "a2");

        a1.next = ActorRefRemote.get(s1, "localhost", 30002, "a2");
        a2.next = ActorRefRemote.get(s2, "localhost", 30001, "a1");

        a1.tell("start");
    }

    public static class TestActor extends ActorDefault {
        public ActorRef next;
        public TestActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, this::receive)
                    .build();
        }

        public void receive(String s) {
            if (s.equals("start")) {
                getSystem().getLogger().log("start: %s", this);
                next.tell("");
            } else if (s.length() < 10) {
                getSystem().getLogger().log("receive: %s <%s>[%d]", this, s, s.length());
                next.tell(s + ".");
                if (s.length() >= 9) {
                    getSystem().close();
                }
            } else {
                getSystem().getLogger().log("finish: %s <%s>[%d]", this, s, s.length());
                getSystem().close();
            }
        }
    }
}
