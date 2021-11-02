package csl.example.util;

import csl.actor.*;
import csl.example.TestTool;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.util.ResponsiveCalls;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

public class ExampleResponsiveCalls {
    public static void main(String[] args) throws Exception {
        ActorSystemRemote.debugLog = true;
        try (ActorSystemDefault sys = new ActorSystemDefault()) {
            new ResActor(sys, "res");

            //send to named-actor: non-MessageDataCallable will be MessageDataPacket
            String data = ResponsiveCalls.<String>send(sys, ActorRefLocalNamed.get(sys, "res"), "hello")
                    .get();
            System.out.println(Instant.now() + ": response: " + data);
            TestTool.assertEquals("send.get", "res:hello", data);

            //dead-letter
            try {
                ResponsiveCalls.<String>send(sys, ActorRefLocalNamed.get(sys, "no-such-actor"), "hello")
                        .get();
            } catch (ExecutionException e) {
                System.out.println(Instant.now() + ": dead-letter: " + e.getCause());
            }
        }

        //send callable
        try (ActorSystemRemote host1 = new ActorSystemRemote().startWithoutWait(50000);
             ActorSystemRemote host2 = new ActorSystemRemote().startWithoutWait(50001)) {

            ResponsiveCalls.initCallableTarget(host2);
            String s = ResponsiveCalls.sendHostTask(host1, host2.getServerAddress(), (a) -> "hello").get();
            System.out.println(Instant.now() + ": callable: " + s);
            TestTool.assertEquals("sendHostTask", "hello", s);

            System.out.println(Instant.now() + ": finish");
        }
    }

    static class ResActor extends ActorDefault {
        public ResActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchWithSender(String.class, this::receive)
                    .build();
        }

        public void receive(String msg, ActorRef sender) {
            System.out.println(Instant.now() + ": receive: " + msg + " from " + sender);

            sender.tell("res:" + msg); //return to the sender
        }
    }
}
