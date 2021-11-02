package csl.example.kelp;

import csl.actor.*;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;
import csl.example.TestTool;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ExampleClusterManyConnections {
    public static void main(String[] args) throws Exception {
        ActorSystemKelp s1 = new TestSys();
        ActorSystemKelp s2 = new TestSys();
        ActorSystemKelp s3 = new TestSys();
        ActorSystemKelp s4 = new TestSys();

        s1.startWithoutWait(30001);
        s2.startWithoutWait(30002);
        s3.startWithoutWait(30003);
        s4.startWithoutWait(30004);

        Thread.sleep(2_000);

        TestActor s1a = new TestActor(s1, "s1");

        List<TestActor> actors = new ArrayList<>();

        List<ActorRefRemote> s2s = new ArrayList<>();
        for (int i = 0; i < 20; ++i) {
            String name = "s2_" + i;
            actors.add(new TestActor(s2, name));
            s2s.add(ActorRefRemote.get(s1, "localhost", 30002, name));
        }

        List<ActorRefRemote> s3s = new ArrayList<>();
        for (int i = 0; i < 20; ++i) {
            String name = "s3_" + i;
            actors.add(new TestActor(s3, name));
            s3s.add(ActorRefRemote.get(s1, "localhost", 30003, name));
        }

        List<ActorRefRemote> s4s = new ArrayList<>();
        for (int i = 0; i < 20; ++i) {
            String name = "s3_" + i;
            actors.add(new TestActor(s4, name));
            s4s.add(ActorRefRemote.get(s1, "localhost", 30004, name));
        }

        s1a.tell(Arrays.asList(s2s, s3s, s4s));
        Thread.sleep(5_000);
        var rec = actors.stream()
                .flatMap(a -> a.messages.stream())
                .sorted()
                .collect(Collectors.toList());
        var exp = s1a.messages.stream()
                .sorted()
                .collect(Collectors.toList());
        s1.close();
        s2.close();
        s3.close();
        s4.close();

        TestTool.assertEquals("result", exp, rec);
    }

    public static class TestSys extends ActorSystemKelp {
        @Override
        protected int getConnectionMax() {
            return 10;
        }

        @Override
        protected ActorSystemRemote.ConnectionActor createConnection(ActorAddress addr) {
            if (addr instanceof ActorAddress.ActorAddressRemote) {
                try {
                    return new TestCon(localSystem, this,
                            hostMap.computeIfAbsent(addr.getHostAddress(), this::createConnectionHost),
                            (ActorAddress.ActorAddressRemote) addr);
                } catch (InterruptedException ex) {
                    getLogger().log(true, debugLogColor, ex, "createConnection: %s", addr);
                    return null;
                }
            } else {
                return null;
            }
        }

        @Override
        public void sendClockedMessage(Message.MessageDataClock<Message<?>> message) {
            Message<?> m = message.body;
            if (m.getData() instanceof Msg) {
                Msg msg = (Msg) m.getData();
                msg.count++;
            }

            ActorRef target = message.body.getTarget();
            if (target instanceof ActorRefRemote) {
                ActorAddress addrActor = ((ActorRefRemote) target).getAddress();
                ActorAddress addrHost = addrActor.getHostAddress();
                ActorAddress addr = getAddressForConnection(addrActor);
                ConnectionActor a = connection(addr, addrHost);
                if (a != null) {
                    ((TestCon) a).log(message.body, " resendTell");
                    a.tell(message);
                } else {
                    System.err.println("dead" + message);
                    localSystem.sendDeadLetter(message.body);
                }
            } else {
                System.err.println("illegal target " + message);
                send(message.body);
            }
        }

        @Override
        protected void setupConnectionMapRestructure() {
            super.setupConnectionMapRestructure();
        }
    }

    public static AtomicInteger cons = new AtomicInteger();

    public static class TestCon extends ActorSystemKelp.ConnectionActorKelp {
        int num;
        public TestCon(ActorSystem system, ActorSystemRemote remoteSystem, ActorSystemKelp.ConnectionHost host, ActorAddress.ActorAddressRemote address) throws InterruptedException {
            super(system, remoteSystem, host, address);
            num = cons.incrementAndGet();
        }

        @Override
        public String toString() {
            return "TestCon(" + remoteSystem.getServerAddress() + ", " + num + ")";
        }

        @Override
        public void close() {
            System.err.println("close " + this);
            super.close();
        }

        @Override
        protected void processMessageClocked(Message.MessageDataClock<?> message) {
            Message<?> m = (Message<?>) message.body;
            log(m, "process");
            super.processMessageClocked(message);
        }

        @Override
        protected void resendClockMessage(Message.MessageDataClock<Message<?>> message) {
            Message<?> m = message.body;
            log(m, "resend");
            super.resendClockMessage(message);
        }

        @Override
        public void offer(Message<?> message) {
            if (message.getData() instanceof Message.MessageDataClock<?>) {
                Message<?> m = (Message<?>) ((Message.MessageDataClock<?>) message.getData()).body;
                log(m, "offer");
            }
            super.offer(message);
        }

        @Override
        public void send(Message.MessageDataClock<Message<?>> clockMessage) {
            Message<?> m = clockMessage.body;
            log(m, "send");
            super.send(clockMessage);
        }

        public void log(Message<?> m, String text) {
            if (m != null && m.getData() instanceof Msg) {
                Msg msg = ((Msg) m.getData());
//                if (msg.data.equals("a88") /*|| msg.count > 0*/) {
                    System.err.println(this + " " + text  + " " + msg + " " + this.processMessageLocked());
//                }
            }
        }

        @Override
        public Message<?> pollNextMessage() {
            Message<?> m = super.pollNextMessage();
            log(m, "pollNext");
            return m;
        }


        @Override
        protected void writeSingleMessage(Message.MessageDataClock<Message<?>> message) {
            try {
                var f = getSystem().getScheduledExecutor().schedule(() -> {
                            System.err.println(this + " TIMEOUT");
                        }
                        ,
                        3, TimeUnit.SECONDS);
                super.writeSingleMessage(message);
                f.cancel(true);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }



    public static class Msg implements Serializable {
        public String data;
        public int count;

        public Msg() {}

        public Msg(String data, int count) {
            this.data = data;
            this.count = count;
        }

        @Override
        public String toString() {
            return "Msg{" +
                    "data='" + data + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

    public static class TestActor extends ActorDefault {
        public List<String> messages = new ArrayList<>();

        public TestActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilder builder) {
            return builder.match(List.class, this::receive)
                    .match(Msg.class, this::receiveStr);
        }
        @SuppressWarnings("unchecked")
        public void receive(List<?> l) {
            List<ActorRefRemote> s2s = (List<ActorRefRemote>) l.get(0);
            List<ActorRefRemote> s3s = (List<ActorRefRemote>) l.get(1);
            List<ActorRefRemote> s4s = (List<ActorRefRemote>) l.get(2);

            for (int i = 0, n= Math.min(s2s.size(), s3s.size()); i < n; ++i) {
                s2s.get(i).tell(create("a" + i));
                s3s.get(i).tell(create("b" + i));
            }
            System.err.println("finish send a,b");

            for (int i = 0; i < s4s.size(); ++i) {
                s4s.get(i).tell(create("c" + i));
                if (i % 2 == 0) {
                    if (i < s2s.size()) {
                        s2s.get(i).tell(create("A" + i));
                    }
                } else {
                    if (i < s3s.size()) {
                        s3s.get(i).tell(create("B" + i));
                    }
                }
            }
            System.err.println("finish send c");
        }

        protected Msg create(String d) {
            Msg m = new Msg(d, 0);
            messages.add(m.data);
            return m;
        }

        public void receiveStr(Msg msg) {
            System.err.println(this + ": " + msg);
            messages.add(msg.data);
        }
    }


}

