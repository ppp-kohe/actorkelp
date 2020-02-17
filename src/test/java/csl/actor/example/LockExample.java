package csl.actor.example;

import csl.actor.*;
import csl.actor.example.LockExample.TreeActor;
import csl.actor.example.msgassoc.DebugBehavior;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LockExample {
    public static void main(String[] args) throws Exception {
        for (int n = 0; n < 1000; ++n) {
            System.err.println("loop: "+ n);
            ActorSystemDefault system = new ActorSystemDefault();

            long max = 10_000_000;

            TreeActor t = new TreeActor(system, "root1");
            TreeActor t2 = new TreeActor(system, "root2");
            EndActor e = new EndActor(system, "end", max);

            t.set(new Setting(0, 5, t2));
            t2.set(new Setting(0, 5, e));
            Thread.sleep(100);

            for (int i = 0; i < max; i++) {
                t.tell("hello-" + i);
            }

            system.getExecutorService().awaitTermination(1, TimeUnit.HOURS);
        }
    }

    public static class EndActor extends ActorDefault {
        long val;
        long max;
        long count;
        long next;
        final DebugBehavior.DebugThreadChecker checker = new DebugBehavior.DebugThreadChecker(this);
        public EndActor(ActorSystem system, String name, long max) {
            super(system, name);
            this.max = max;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, this::receive)
                    .build();
        }
        void receive(String s) {
            boolean t = checker.before();
            try {
                val += s.hashCode();
                ++count;
                if (count > next) {
                    System.err.println(String.format("count: %,d (%03.1f)%%", count, (count / (double)max * 100)));
                    next += (max / 4);
                }
                if (count >= max) {
                    System.err.println(String.format("END  : %,d", count));
                    getSystem().close();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                getSystem().close();
                throw ex;
            } finally {
                if (!checker.after(t)) {
                    getSystem().close();
                }
            }
        }
    }
    static class Setting {
        public int current;
        public int max;
        public ActorRef root;

        public Setting(int current, int max, ActorRef root) {
            this.current = current;
            this.max = max;
            this.root = root;
        }
    }

    public static class TreeActor extends ActorDefault {
        TreeActor left;
        TreeActor right;
        Random rand = new Random();
        ActorRef root;
        int level;
        volatile boolean isRoot;
        final DebugBehavior.DebugThreadChecker checker = new DebugBehavior.DebugThreadChecker(this);

        public TreeActor(ActorSystem system, String name) {
            super(system, name);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, this::receive)
                    .match(Setting.class, this::set)
                    .build();
        }

        void set(Setting s) {
            isRoot = s.current == 0;
            root = s.root;
            level = s.current;
            if (s.current < s.max) {
                left = new TreeActor(system, null);
                right = new TreeActor(system, null);
                left.tell(new Setting(s.current + 1, s.max, s.root));
                right.tell(new Setting(s.current + 1, s.max, s.root));
            }
        }

        @Override
        protected void initMailbox() {
            mailbox = new MailboxCount();
        }

        volatile boolean parallel1;
        volatile boolean parallel2;

        public boolean isParallel() {
            return parallel1 || parallel2;
        }

        void receive(String s) {
            boolean t = checker.before();
            try {
                if (!isParallel() && isRoot) {
                    int c = ((MailboxCount) mailbox).getCount();
                    if (c > 10_000) {
                        parallel1 = true;
                        parallel2 = true;
                        //System.err.println("parallel boost:  " + c);
                        getSystem().execute(() -> parallel(true));
                        getSystem().execute(() -> parallel(false));
                    }
                }
                route(s);
            } catch (Exception ex) {
                ex.printStackTrace();
                getSystem().close();
                throw ex;
            } finally {
                if (!checker.after(t)) {
                    getSystem().close();
                }
            }
        }
        Set<String> data = new HashSet<>();
        DebugBehavior.DebugThreadChecker leafChecker = new DebugBehavior.DebugThreadChecker(this);
        void route(String s) {
            try {
                if (left != null && right != null) {
                    if (rand.nextBoolean()) {
                        left.tell(s);
                    } else {
                        right.tell(s);
                    }
                } else {
                    boolean t = leafChecker.before();
                    try {
                        data.add(s);
                        root.tell(s);
                    } finally {
                        if (!leafChecker.after(t)) {
                            getSystem().close();
                        }
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                getSystem().close();
                throw ex;
            }
        }

        void parallel(boolean b) {
            int n = 0;
            while (!mailbox.isEmpty() && n < 50_000) {
                Message<?> m = mailbox.poll();
                if (m == null) {
                    break;
                }
                if (m.getData() instanceof String) {
                    route((String) m.getData());
                }
                ++n;
            }
            if (b) {
                parallel1 = false;
            } else {
                parallel2 = false;
            }
            if (!isParallel()) {
                //System.err.println("finish parallel: " + ((MailboxCount) mailbox).getCount());
            }
        }

    }
    static class MailboxCount extends MailboxDefault {
        AtomicInteger count = new AtomicInteger();

        public int getCount() {
            return count.get();
        }

        @Override
        public void offer(Message<?> message) {
            count.incrementAndGet();
            super.offer(message);
        }

        @Override
        public Message<?> poll() {
            Message<?> m = super.poll();
            if (m != null) {
                count.decrementAndGet();
            }
            return m;
        }
    }
}
