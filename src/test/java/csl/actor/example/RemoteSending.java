package csl.actor.example;

import csl.actor.ActorBehavior;
import csl.actor.ActorDefault;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class RemoteSending {
    public static void main(String[] args) {
        ActorSystemRemote s1 = new ActorSystemRemote();
        s1.startWithoutWait(10000);

        ActorSystemRemote s2 = new ActorSystemRemote();
        s2.startWithoutWait(10001);

        List<long[]> data = new ArrayList<>();
        Random r = new Random(12345);
        int msgSize = 10000;
        int msgs = 500;
        for (int i = 0; i < msgs; ++i) {
            long[] ls = r.longs().limit(msgSize).toArray();
            ls[0] = i;
            data.add(ls);
        }
        System.err.println("created inputs");

        new ActorReceiver(s1, "r");

        ActorAddress addr = s1.getServerAddress().getActor("r");
        ActorRef rec = ActorRefRemote.get(s2, addr);
        rec.tell("start", null);

        for (long[] d : data) {
            rec.tell(d, null);
        }

        rec.tell("end", null);

    }

    static class ActorReceiver extends ActorDefault {
        long receivedBytes;
        long receivedMessages;
        Instant prevTime = Instant.now();
        LinkedList<Long> nums = new LinkedList<>();

        ActorReceiver(ActorSystem s, String name) {
            super(s, name);
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
            if (nums.size() > 30) {
                nums.removeFirst();
            }
            if ((receivedMessages % 10) == 0 || receivedMessages > 90) {
                printInfo(String.format("receive: %s", nums));
            }
        }

        void info(String msg, ActorRef sender) {
            printInfo(msg);
            prevTime = Instant.now();
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
