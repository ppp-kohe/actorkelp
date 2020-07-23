package csl.actor.example.kelp;

import csl.actor.*;
import csl.actor.cluster.PersistentFileManager;
import csl.actor.example.ExampleSerialize2;
import csl.actor.kelp.Config;
import csl.actor.cluster.MailboxPersistable;
import csl.actor.kelp.PhaseShift;
import csl.actor.util.ResponsiveCalls;
import csl.actor.remote.KryoBuilder;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

public class ExampleMailboxPersistableTest {
    public static void main(String[] args) throws Exception {
        new ExampleMailboxPersistableTest().run();
    }
    public void run() throws Exception {
        runPersistentFileManager();
        runPersistentFileManagerRecursive();
        runMailboxPersistable();
        runMailboxPersistableSpeed();
    }

    private void runPersistentFileManager() throws Exception {
        System.err.println("------------ runPersistentFileManager");
        try (ActorSystem system = new ActorSystemDefault()) {
            KryoBuilder.SerializerPool p = new KryoBuilder.SerializerPoolDefault(system);
            PersistentFileManager manager = new PersistentFileManager(
                    "target/debug-persist", p, Paths::get, system.getLogger());
            MailboxPersistable.MessagePersistentFile mp = new MailboxPersistable.MessagePersistentFile(manager);

            ExampleSerialize2.MyActor a = new ExampleSerialize2.MyActor(system, "hello", Config.CONFIG_DEFAULT);

            MailboxPersistable.MessageOnStorage s;
            try (MailboxPersistable.MessagePersistentWriter w = mp.get()) {
                s = w.reader();
                w.save(new Message<>(a, a, "hello"));
                w.save(new Message<>(a, a, 1234));
                w.save(new Message<>(a, a, CallableMessage.callableMessage((self) -> "hello")));
            }

            Message<?> m = s.readNext();
            check(m.getTarget(), a, (act, exp) -> act.asLocal().equals(exp));
            check(m.getSender(), a, (act, exp) -> act.asLocal().equals(exp));
            check(m.getData(), "hello");
            m = s.readNext();
            check(m.getTarget(), a, (act, exp) -> act.asLocal().equals(exp));
            check(m.getSender(), a, (act, exp) -> act.asLocal().equals(exp));
            check(m.getData(), 1234);
            m = s.readNext();
            check(m.getTarget(), a, (act, exp) -> act.asLocal().equals(exp));
            check(m.getSender(), a, (act, exp) -> act.asLocal().equals(exp));
            check(m.getData(), (Object) "hello",
                    (act,exp) -> ((CallableMessage<?,?>) act).call(null, null).equals("hello"));

            m = s.readNext();
            check(m, null, (act,exp) -> act == exp);
        }
    }

    private void runPersistentFileManagerRecursive() throws Exception {
        System.err.println("------------ runPersistentFileManagerRecursive");
        try (ActorSystem system = new ActorSystemDefault()) {
            KryoBuilder.SerializerPool p = new KryoBuilder.SerializerPoolDefault(system);
            PersistentFileManager manager = new PersistentFileManager(
                    "target/debug-persist", p, Paths::get, system.getLogger());
            MailboxPersistable.MessagePersistentFile mp = new MailboxPersistable.MessagePersistentFile(manager);

            ExampleSerialize2.MyActor a = new ExampleSerialize2.MyActor(system, "hello", Config.CONFIG_DEFAULT);

            int blockSize = 3;

            MailboxPersistable.MessageOnStorage s;
            try (MailboxPersistable.MessagePersistentWriter w = mp.get()) {
                s = w.reader();
                for (int i = 0; i < blockSize; ++i) {
                    w.save(new Message<>(a, a, "" + i));
                }
            }

            MailboxPersistable.MessageOnStorage s2;
            try (MailboxPersistable.MessagePersistentWriter w = mp.get()) {
                s2 = w.reader();
                for (int i = 0; i < blockSize; ++i) {
                    w.save(new Message<>(a, a, "r1-" + i));
                }
                w.save(s);
                for (int i = 0; i < blockSize; ++i) {
                    w.save(new Message<>(a, a, "r2-" + i));
                }
            }

            MailboxPersistable.MessageOnStorage s3;
            try (MailboxPersistable.MessagePersistentWriter w = mp.get()) {
                s3 = w.reader();
                for (int i = 0; i < blockSize; ++i) {
                    w.save(new Message<>(a, a, "r3-" + i));
                }
                w.save(s2);
                for (int i = 0; i < blockSize; ++i) {
                    w.save(new Message<>(a, a, "r4-" + i));
                }
            }

            for (int i = 0; i < blockSize; ++i) {
                Message<?> m = s3.readNext();
                check(m, a, a, "r3-" + i);
            }
            for (int i = 0; i < blockSize; ++i) {
                Message<?> m = s3.readNext();
                check(m, a, a, "r1-" + i);
            }
            for (int i = 0; i < blockSize; ++i) {
                Message<?> m = s3.readNext();
                check(m, a, a, "" + i);
            }
            for (int i = 0; i < blockSize; ++i) {
                Message<?> m = s3.readNext();
                check(m, a, a, "r2-" + i);
            }
            for (int i = 0; i < blockSize; ++i) {
                Message<?> m = s3.readNext();
                check(m, a, a, "r4-" + i);
            }
            Message<?> m = s3.readNext();
            check(m, null, (act,exp) -> act == exp);
        }
    }

    private void check(Message<?> m, ActorRef a, ActorRef s, Object obj) {
        check(m.getTarget(), a, (act, exp) -> act.asLocal().equals(exp));
        check(m.getSender(), a, (act, exp) -> act.asLocal().equals(exp));
        check(m.getData(), obj);
    }

    private <E> void check(E r, E obj) {
        check(r, obj, Objects::equals);
    }
    private <E> void check(E r, E obj, BiPredicate<E, E> p) {
        System.out.println(r);
        System.out.println(p.test(r, obj) ? formatColor(76,"[OK]") :  (formatColor(196, "DIFF") + " : " + obj));
    }

    private String formatColor(int c, String s) {
        return String.format("\033[38;5;%dm%s\033[0m",c, s);
    }


    private void runMailboxPersistable() throws Exception {
        System.err.println("------------ runMailboxPersistable");
        ActorSystemDefault system = new ActorSystemDefault();
        TestActor a = new TestActor(system, "a");

        int num = 100_000;
        List<Object> inputs = new ArrayList<>(num);
        Instant start = Instant.now();
        for (int i = 0; i < num; ++i) {
            a.tell(i);
            inputs.add(i);
        }
        System.err.println(String.format("%s: finish input", Duration.between(start, Instant.now())));

        PhaseShift.PhaseTerminalActor finishActor = new PhaseShift.PhaseTerminalActor(system, false,
                (sys, comp) -> {
                    try {
                        ResponsiveCalls.<TestActor>sendTaskConsumer(sys, a, (self) -> {
                                TestMailboxPersistable  m = (TestMailboxPersistable) self.getMailbox();
                                System.err.println("persist: " + m.persistCount);
                                self.log();

                                System.out.println(self.received.equals(inputs) ? formatColor(76,"[OK]") :
                                        (formatColor(196, "DIFF") + " : " + self.received.size() + " vs " + inputs.size()));
                                System.err.println("  equals as sets: " + new HashSet<>(self.received).equals(new HashSet<>(inputs)));
                                for (int i = 0; i < num; ++i) {
                                    if (!inputs.get(i).equals(self.received.get(i))) {
                                        System.err.println("  DIFF: [" + i + "] " + self.received.get(i) + " vs " + inputs.get(i));
                                        break;
                                    }
                                }
                        }).get();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    System.err.println("finish");
                    sys.close();
                });

        a.tell(new PhaseShift("", finishActor));
        system.getExecutorService().awaitTermination(1, TimeUnit.HOURS);
    }

    public static class TestActor extends ActorDefault {
        long n;
        public List<Object> received = new ArrayList<>();
        public TestActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected Mailbox initMailbox() {
            return new TestMailboxPersistable(PersistentFileManager.getPersistentFile(system, () -> "target/debug-persist"),
                    5_000,
                    100);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(Integer.class, this::receive)
                    .build();
        }

        void receive(Object msg) {
            received.add(msg);
            ++n;
            if (n % 1000 == 0) {
                //MailboxPersistable mp = (MailboxPersistable) mailbox;
                try {
                    Thread.sleep(10);
                } catch (Exception ex) {
                    //
                }
            }
        }

        public void log() {
            System.err.println(String.format("n=%,d, data=%d", n, received.size()));
        }
    }

    public static class TestMailboxPersistable extends MailboxPersistable {
        public long persistCount;
        Message<?> prev;

        public TestMailboxPersistable(PersistentFileManager manager, long sizeLimit, long onMemorySize) {
            super(manager, sizeLimit, onMemorySize);
        }

        @Override
        protected void persistLocked() {
            ++persistCount;
            super.persistLocked();
        }

        @Override
        protected Message<?> pollByReadNext(MessageOnStorage m) {
            if (m != prev) {
                System.err.println("storage: " + m);
            }
            prev = m;
            return super.pollByReadNext(m);
        }

        @Override
        protected void pollClose(MessageOnStorage mOnS) {
            super.pollClose(mOnS);
            System.err.println("  close: " + mOnS);
        }
    }

    private void runMailboxPersistableSpeed() throws Exception {
        System.err.println("------------ runMailboxPersistableSpeed: no persistent");
        runPersistSpeed(false);
        System.err.println("------------ runMailboxPersistableSpeed");
        runPersistSpeed(true);
    }

    private void runPersistSpeed(boolean p) throws Exception {
        ActorSystemDefault system = new ActorSystemDefault();
        TestActorForSpeed a = new TestActorForSpeed(system, "a", p);

        int num = 10_000_000;
        Instant start = Instant.now();
        for (int i = 0; i < num; ++i) {
            a.tell(i);
        }
        System.err.println(String.format("%s: finish input", Duration.between(start, Instant.now())));

        PhaseShift.PhaseTerminalActor finishActor = new PhaseShift.PhaseTerminalActor(system, false,
                (sys, comp) -> {
                    try {
                        ResponsiveCalls.<TestActorForSpeed>sendTaskConsumer(sys, a, (self) -> {
                            if (self.getMailbox() instanceof TestMailboxPersistable) {
                                TestMailboxPersistable m = (TestMailboxPersistable) self.getMailbox();
                                System.err.println("persist: " + m.persistCount);
                            }
                            self.log();
                        }).get();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    System.err.println("finish");
                    sys.close();
                });

        a.tell(new PhaseShift("", finishActor));
        system.getExecutorService().awaitTermination(1, TimeUnit.HOURS);
    }

    public static class TestActorForSpeed extends ActorDefault {
        long n;
        boolean persist;
        Instant start = Instant.now();
        public TestActorForSpeed(ActorSystem system, String name, boolean persist) {
            super(system, name, null, null);
            this.persist = persist;
            behavior = initBehavior();
            mailbox = initMailbox();
            start = Instant.now();
        }

        @Override
        protected Mailbox initMailbox() {
            if (persist) {
                return new TestMailboxPersistable(PersistentFileManager.getPersistentFile(system, () -> "target/debug-persist"),
                        100_000,
                        100);
            } else {
                return new MailboxDefault();
            }
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(Integer.class, this::receive)
                    .build();
        }

        void receive(Object msg) {
            ++n;
        }
        public void log() {
            System.err.println(String.format("%s: n=%,d", Duration.between(start, Instant.now()), n));
        }
    }
}
