package csl.actor.example.msgassoc;

import csl.actor.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadTest {
    public static void main(String[] args) {
        new ThreadTest().run(args);
    }

    public void run(String... args) {
        int n = Integer.parseInt(args[0].replaceAll("[_,]", ""));
        int ps = Runtime.getRuntime().availableProcessors();
        create().run(n, ps, ps / 2);
        create().run(n, ps, 1);
        create().runActor(n, ps, 1, 1);
        create().runActor(n, ps, ps / 2, 1);
        create().runActor(n, ps, ps / 2, 2);
        create().runActor(n, ps, ps / 2, 4);
    }

    public ThreadTest create() {
        return new ThreadTest();
    }

    int num;
    ConcurrentLinkedQueue<Object> queue;
    ExecutorService service;
    Instant start;
    AtomicInteger i = new AtomicInteger();

    public void run(int n, int threads, int readThreads) {
        log(String.format("%s ============ start n=%,d, th=%,d, rth=%,d", getClass().getSimpleName(), n, threads, readThreads));
        this.num = n;
        service = Executors.newFixedThreadPool(threads);
        queue = new ConcurrentLinkedQueue<>();
        start = Instant.now();
        service.execute(() -> {
            for (int i = 0; i < num; ++i) {
                queue.offer("item" + i);
            }
            log(String.format("finish offer %,d: %s", num, Duration.between(start, Instant.now())));
        });

        List<ThreadComp> cs = new ArrayList<>();
        for (int i = 0; i < readThreads; ++i) {
            ThreadComp tc = createThreadComp(i);
            cs.add(tc);
            service.execute(() -> {
                while (true) {
                    Object msg = queue.poll();
                    if (msg == null) {
                        break;
                    }
                    Object o = process(tc, msg);
                    service.execute(() -> count(tc, o));
                }
                log(String.format("finish poll th-%d %,d: %s", tc.th, tc.polls, Duration.between(start, Instant.now())));
            });
        }

        try {
            service.awaitTermination(1, TimeUnit.HOURS);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        log(String.format("   threads: %s", cs));
    }

    public Object process(ThreadComp self, Object o) {
        self.polls++;
        return o;
    }

    public void count(ThreadComp self, Object o) {
        if (i.incrementAndGet() >= num) {
            log(String.format("finish: %,d: %s", i.get(), Duration.between(start, Instant.now())));
            service.shutdownNow();
        }
    }
    protected ThreadComp createThreadComp(int th) {
        return new ThreadComp(th);
    }

    public static class ThreadComp {
        public int th;
        public int polls;
        public ThreadComp(int th) {
            this.th = th;
        }

        @Override
        public String toString() {
            return String.format("(%d, %,d)", th, polls);
        }
    }

    public static void log(String s) {
        System.err.println(s);
    }

    ////////

    public void runActor(int n, int threads, int readThreads, int routerThreads) {
        num = n;
        log(String.format("%s ============ start actor n=%,d, th=%,d, rth=%,d, routerThreads=%,d", getClass().getSimpleName(), n, threads, readThreads, routerThreads));
        int th = threads;
        ActorSystemDefault sys = new ActorSystemDefault() {
            @Override
            protected void initSystemThreads() {
                threads = th;
            }
        };
        start = Instant.now();

        FinishActor fa = new FinishActor(sys, num);

        List<ReadActor> as = new ArrayList<>();
        for (int r = 0; r < readThreads; ++r) {
            as.add(newReadActor(sys, r, fa));
        }
        RouterActor ra = newRouterActor(sys, as);
        ra.th = routerThreads;

        for (int i = 0; i < num; ++i) {
            ra.tell("item" + i, null);
        }
        log(String.format("finish offer %,d: %s", num, Duration.between(start, Instant.now())));

        try {
            sys.getExecutorService().awaitTermination(1, TimeUnit.HOURS);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        log(String.format("finish router: %s", Duration.between(start, ra.last)));
        log(String.format("   threads: %s", as));
    }

    ReadActor newReadActor(ActorSystem sys, int r, ActorRef fa) {
        return new ReadActor(sys, r, fa);
    }

    RouterActor newRouterActor(ActorSystem sys, List<ReadActor> as) {
        return new RouterActor(sys, as);
    }


    static class FinishActor extends ActorDefault {
        int count;
        int num;
        Instant start;
        public FinishActor(ActorSystem system, int num) {
            super(system);
            this.num = num;
            start = Instant.now();
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(Object.class, this::receive)
                    .build();
        }

        public void receive(Object c) {
            ++count;
            if (count >= num) {
                log(String.format("finish: %,d: %s", count, Duration.between(start, Instant.now())));
                getSystem().close();
            }
        }
    }

    static class ReadActor extends ActorDefault {
        int n;
        ActorRef target;
        long empties;
        int polls;

        public ReadActor(ActorSystem system, int n, ActorRef target) {
            super(system);
            this.n = n;
            this.target = target;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(Object.class, this::receive)
                    .build();
        }

        void receive(Object s) {
            target.tell(s, this);
            ++polls;
            if (mailbox.isEmpty()) {
                empties++;
            }
        }

        @Override
        public String toString() {
            return String.format("(%d, %,d, emp=%,d)", n, polls, empties);
        }
    }
    static class RouterActor extends ActorDefault {
        List<ReadActor> actors;
        int i = 0;
        public volatile Instant last;
        public int th = 1;

        public RouterActor(ActorSystem system, List<ReadActor> actors) {
            super(system);
            this.actors = actors;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, this::receive)
                    .build();
        }

        public void receive(String s) {
            receiveItem(s);
            for (int i = 0; i < th - 1; ++i) {
                system.execute(() -> {
                    while (!mailbox.isEmpty()) {
                        Message<?> o = mailbox.poll();
                        if (o != null) {
                            receiveItem((String) o.getData());
                        }
                    }
                });
            }
        }

        protected void receiveItem(String s) {
            last = Instant.now();
            actors.get(i % actors.size()).tell(s, this);
            ++i;
        }
    }

}
