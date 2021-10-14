package csl.example;

import csl.actor.ActorBehavior;
import csl.actor.ActorDefault;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.ObjectMessageClient;
import csl.actor.remote.ObjectMessageServer;

public class ExampleRemoteSending {
    public static void main(String[] args) throws Exception {
        ActorSystemRemote.debugLog = true;
        ObjectMessageServer.debugTraceLog = true;
        ObjectMessageClient.debugTraceLog = true;
        ActorSystemRemote sys1 = new ActorSystemRemote();
        ActorSystemRemote sys2 = new ActorSystemRemote();

        sys1.startWithoutWait(30000);
        sys2.startWithoutWait(30001);

        MyActor a = new MyActor(sys1, "a");
        MyActor b = new MyActor(sys2, "b");

        Thread.sleep(1000);
        a.tell("#30001/b", null);
        Thread.sleep(5000);
        a.tell("#30001/b", null);
        System.out.println("after sending 2 messages");

        Thread.sleep(10_000);
        sys1.close();
        sys2.closeAfterOtherConnectionsClosed();
    }

    public static class MyActor extends ActorDefault {
        public MyActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder().matchAny(this::run)
                    .build();
        }

        public void run(Object msg, ActorRef sender) {
            System.out.println(getName() + " : receive " + msg  + " : " + system);
            if (msg instanceof String) {
                String s = msg.toString();
                if (s.startsWith("#")) {
                    String[] t = s.substring(1).split("/");
                    int port = Integer.parseInt(t[0]);
                    String name = t[1];
                    ActorRefRemote.get(getSystem(), "localhost", port, name)
                        .tell("hello", this);
                }
            }
        }
    }
}
