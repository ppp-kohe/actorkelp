package csl.actor.example;

import csl.actor.*;
import csl.actor.example.msgassoc.DebugBehavior;
import csl.actor.msgassoc.ActorBehaviorBuilderKeyValue;
import csl.actor.msgassoc.KeyHistograms;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LockExample {
    static AtomicBoolean error = new AtomicBoolean();
    public static void main(String[] args) throws Exception {
        for (int n = 0; n < 1000; ++n) {
            if (error.get()) {
                System.err.println("error");
                break;
            }
            System.err.println("loop: "+ n);
            ActorSystemDefault system = new ActorSystemDefault();

            long max = 5_000_000;

            TreeActor t = new TreeActor(system, "root1");
            TreeActor t2 = new TreeActor(system, "root2");
            EndActor e = new EndActor(system, "end", max);

            t.set(new Setting(10, t2));
            t2.set(new Setting(10, e));
            Thread.sleep(100);

            int ts = system.getThreads();
            max /= ts;
            max *= ts;

            for (int i = 0; i < ts; ++i) {
                long blk = max / ts;
                system.execute(() -> {
                    Random rand = new Random();
                    for (long j = 0; j < blk; ++j) {
                        t.tell("hello-" + rand.nextInt());
                    }
                });
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
                checker.error(t, ex);
                ex.printStackTrace();
                error.set(true);
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
        public int max;
        public ActorRef end;

        public Setting(int max, ActorRef end) {
            this.max = max;
            this.end = end;
        }
    }

    public static class TreeActor extends ActorDefault {
        Random rand = new Random();
        final DebugBehavior.DebugThreadChecker checker = new DebugBehavior.DebugThreadChecker(this);

        List<ActorRef> children = new ArrayList<>();

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
            for (int i = 0; i < s.max; ++i) {
                children.add(new LeafActor(system, null, s.end));
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
                if (!isParallel()) {
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
                checker.error(t, ex);
                ex.printStackTrace();
                error.set(true);
                getSystem().close();
                throw ex;
            } finally {
                if (!checker.after(t)) {
                    error.set(true);
                    getSystem().close();
                }
            }
        }
        void route(String s) {
            try {
                children.get(rand.nextInt(children.size()))
                        .tell(s);
            } catch (Exception ex) {
                ex.printStackTrace();
                error.set(true);
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

    static class LeafActor extends ActorDefault {
        ActorRef end;
        KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
        public LeafActor(ActorSystem system, String name, ActorRef end) {
            super(system, name);
            this.end = end;
            tree = new KeyHistograms.HistogramTree(new ActorBehaviorBuilderKeyValue.KeyComparatorDefault<>());
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, this::receive).build();
        }
        DebugBehavior.DebugThreadChecker leafChecker = new DebugBehavior.DebugThreadChecker(this);

        KeyHistograms.HistogramTree tree;


        void receive(String s) {
            boolean t = leafChecker.before();
            try {
                ctx.putValue = s;
                ctx.putPosition = 0;
                ctx.putRequiredSize = 1;
                tree.put(s, ctx);
                end.tell(s);
            } catch (Exception ex) {
                leafChecker.error(t, ex);
                ex.printStackTrace();
                error.set(true);
                getSystem().close();
                throw ex;
            } finally {
                if (!leafChecker.after(t)) {
                    error.set(true);
                    getSystem().close();
                }
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
