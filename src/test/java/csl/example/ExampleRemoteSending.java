package csl.example;

import csl.actor.ActorBehavior;
import csl.actor.ActorDefault;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.ObjectMessageClient;
import csl.actor.remote.ObjectMessageServer;

import java.io.Serializable;

public class ExampleRemoteSending {
    public static void main(String[] args) throws Exception {
        ActorSystemRemote.debugLog = true;
        ActorSystemRemote.debugLogMsg = true;
        ObjectMessageServer.debugTraceLog = true;
        ObjectMessageClient.debugTraceLog = true;
        ActorSystemRemote sys1 = new ActorSystemRemote();
        ActorSystemRemote sys2 = new ActorSystemRemote();

        sys1.startWithoutWait(30000);
        sys2.startWithoutWait(30001);

        MyActor a = new MyActor(sys1, "a");
        MyActor b = new MyActor(sys2, "b");

        Thread.sleep(1000);
        a.tell("#30001/b");
        Thread.sleep(5000);
        a.tell("#30001/b");
        System.err.println("after sending 2 messages");

        Thread.sleep(10_000);
        sys1.close();
        sys2.closeAfterOtherConnectionsClosed();
    }

    public static class Special implements Message.MessageDataSpecial, Serializable {
        @Override
        public String toString() {
            return "Special";
        }
    }

    public static class MyActor extends ActorDefault {
        public MyActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(Special.class, this::runSpecial)
                    .matchAny(this::run)
                    .build();
        }

        public void run(Object msg) {
            getSystem().getLogger().log(true, 10,
                    "ACTOR: %s receive %s : %s %s", getName(), msg, system, Thread.currentThread());
            if (msg instanceof String) {
                String s = msg.toString();
                if (s.startsWith("#")) {
                    String[] t = s.substring(1).split("/");
                    int port = Integer.parseInt(t[0]);
                    String name = t[1];
                    ActorRefRemote.get(getSystem(), "localhost", port, name)
                        .tell("hello");

                    ActorRefRemote.get(getSystem(), "localhost", port, name)
                            .tell(new Special());
                }
            }
        }


        public void runSpecial(Special s) {
            getSystem().getLogger().log(true, 10,
                    "ACTOR: %s special %s : %s %s", getName(), s, system, Thread.currentThread());
        }

        @Override
        protected void processMessageSystemClock(Message.MessageDataClock<?> message) {
            getSystem().getLogger().log(true, 10,
                    "ACTOR: %s process clock %s : %s %s", getName(), message, system, Thread.currentThread());
            super.processMessageSystemClock(message);
        }
    }
}
