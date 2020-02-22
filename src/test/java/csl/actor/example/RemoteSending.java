package csl.actor.example;

import csl.actor.ActorBehavior;
import csl.actor.ActorDefault;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RemoteSending {
    public static void main(String[] args) throws Exception {
        int msgBytes = 1000_000;
        int msgs = 1_000;
        ActorSystemRemote s2 = new ActorSystemRemote();
        s2.startWithoutWait(20001);

        MasterActor masterActor = new MasterActor(s2);

        ExampleRemote.setMvnClasspath();
        ExampleRemote.launchJava(Receiver.class.getName(), Integer.toString(msgs));

        List<long[]> data = new ArrayList<>();
        Random r = new Random(12345);
        for (int i = 0; i < msgs; ++i) {
            long[] ls = r.longs().limit(msgBytes / 8).toArray();
            ls[0] = i;
            data.add(ls);
        }
        System.err.println("created inputs");

        masterActor.getConnected().get();

        ActorRef rec = ActorAddress.get("localhost", 20000).getActor("r").ref(s2);

        System.err.println("remote target: " + rec);
        rec.tell("start", null);

        for (long[] d : data) {
            rec.tell(d, null);
        }

        rec.tell("end");
        s2.closeAfterOtherConnectionsClosed();
    }

    static class MasterActor extends ActorDefault {
        CompletableFuture<Object> connected;
        MasterActor(ActorSystem s) {
            super(s, "master");
            connected = new CompletableFuture<>();
        }

        public CompletableFuture<Object> getConnected() {
            return connected;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, this::receive)
                    .build();
        }

        void receive(Object m) {
            System.err.println(m);
            connected.complete(m);
        }
    }

    public static class Receiver {
        public static void main(String[] args) {
            int msgs = Integer.parseInt(args[0]);
            ActorSystemRemote s1 = new ActorSystemRemote();
            s1.startWithoutWait(20000);
            new ActorReceiver(s1, "r", msgs);

            ActorAddress.get("localhost", 20001).getActor("master").ref(s1)
                    .tell("start");
        }
    }

    static class ActorReceiver extends ActorDefault {
        long receivedBytes;
        long receivedMessages;
        long maxMessages;
        Instant prevTime = Instant.now();
        LinkedList<Long> nums = new LinkedList<>();
        ScheduledExecutorService service;

        long nextExtension = 1;

        ActorReceiver(ActorSystem s, String name, long maxMessages) {
            super(s, name);
            this.maxMessages = maxMessages;
            service = Executors.newSingleThreadScheduledExecutor();
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(long[].class, this::receive)
                    .matchWithSender(String.class, this::info)
                    .build();
        }

        void receive(long[] array) {
            receivedBytes += array.length * 8L;
            receivedMessages++;
            nums.add(array[0]);
            if (receivedMessages > 10) {
                nums.removeFirst();
            }
            if ((receivedMessages % (maxMessages / 10)) == 0) {
                printInfo(String.format("receive: %s", nums));
            }
        }

        void info(String msg, ActorRef sender) {
            printInfo(msg);
            prevTime = Instant.now();
            if (msg.equals("end") || msg.equals("extension")) {
                if (receivedMessages < maxMessages) {
                    service.schedule(() -> tell("extension", this), nextExtension, TimeUnit.SECONDS);
                    nextExtension *= 2L;
                }
                if (msg.equals("end")) {
                    System.out.println("END " + system);
                    getSystem().close();
                }
            }
        }

        void printInfo(String msg) {
            Instant time = Instant.now();
            Duration diff = Duration.between(prevTime, time);

            double speed = receivedBytes / (double) diff.toMillis() * 1000.0;

            double mSpeed = receivedMessages / (double) diff.toMillis() * 1000.0;

            System.out.println(String.format("[%s] %s: %s: %,d bytes, %,6.3f B/s, %,d msgs, %,6.3f msg/s",
                    time, diff, msg, receivedBytes, speed, receivedMessages, mSpeed));
        }
    }
}
