package csl.example.kelp;

import csl.actor.*;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.remote.ActorRefRemote;

import java.io.Serializable;

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

    public static class Special implements Message.MessageDataSpecial, Serializable {
        @Override
        public String toString() {
            return "Special";
        }
    }


    public static class TestActor extends ActorDefault {
        public ActorRef next;
        public TestActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(Special.class, this::runSpecial)
                    .match(String.class, this::receive)
                    .build();
        }

        public void receive(String s) {
            if (s.equals("start")) {
                getSystem().getLogger().log(true, 10, "start: %s %s clock=%s", this, Thread.currentThread(),getClocks());
                next.tell("");
                next.tell(new Special());
            } else if (s.length() < 10) {
                getSystem().getLogger().log(true, 10, "receive: %s <%s>[%d] %s clock=%s", this, s, s.length(), Thread.currentThread(),getClocks());
                next.tell(s + ".");
                if (s.length() >= 9) {
                    getSystem().close();
                }
            } else {
                getSystem().getLogger().log(true, 10, "finish: %s <%s>[%d] %s clock=%s", this, s, s.length(), Thread.currentThread(),getClocks());
                getSystem().close();
            }
        }

        public void runSpecial(Special s) {
            getSystem().getLogger().log(true, 10,
                    "ACTOR: %s special %s : %s %s clock=%s", getName(), s, system, Thread.currentThread(), getClocks());
        }

        @Override
        protected void processMessageSystemClock(Message.MessageDataClock<?> message) {
            super.processMessageSystemClock(message);
            getSystem().getLogger().log(true, 10,
                    "ACTOR: %s process clock %s : %s %s clock=%s", getName(), message, system, Thread.currentThread(), getClocks());
        }
    }
}
