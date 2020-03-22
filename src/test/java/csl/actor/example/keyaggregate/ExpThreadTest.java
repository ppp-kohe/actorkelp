package csl.actor.example.keyaggregate;

import csl.actor.*;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class ExpThreadTest {
    public static void main(String[] args) {
        new ExpThreadTest().run(args);
    }

    public void run(String... args) {
        int n = Integer.parseInt(args[0].replaceAll("[_,]", ""));
        int ps = Runtime.getRuntime().availableProcessors();
        for (int i = 2; i <= ps * 2; ++i) {
            create().runActor(n, ps, i, 2);
        }
        create().run(n, ps, ps / 2);
        create().run(n, ps, 1);
        create().runActor(n, ps, 1, 1);
        create().runActor(n, ps, ps / 2, 1);
        create().runActor(n, ps, ps / 2, 2);
        create().runActor(n, ps, ps / 2, 4);

    }

    public ExpThreadTest create() {
        return new ExpThreadTest();
    }

    int num;
    ConcurrentLinkedQueue<Object> queue;
    ExecutorService service;
    Instant start;
    AtomicInteger i = new AtomicInteger();
    volatile boolean finish = false;

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

        String title = title("t", threads, readThreads, 0);
        List<ThreadComp> cs = new ArrayList<>();
        IntStream.range(0, readThreads).forEach(i -> cs.add(createThreadComp(i, title)));

        for (ThreadComp tc : cs) {
            service.execute(() -> {
                while (!queue.isEmpty() || !finish) {
                    Object msg = queue.poll();
                    if (msg != null) {
                        Object o = process(tc, msg);
                        service.execute(() -> count(tc, o));
                    }
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
            Duration d = Duration.between(start, Instant.now());
            log(String.format("finish: %,d: %s", i.get(), d));
            service.shutdownNow();
            finish = true;
            save(self.title, d);
        }
    }
    protected ThreadComp createThreadComp(int th, String title) {
        return new ThreadComp(th, title);
    }

    public static class ThreadComp {
        public int th;
        public int polls;
        public String title;
        public ThreadComp(int th, String title) {
            this.th = th;
            this.title = title;
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

        FinishActor fa = new FinishActor(sys, num, title("a", threads, readThreads, routerThreads));

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

    String title(String type, int threads, int readThreads, int routerThreads) {
        return String.format("%s-%s-%s-th%d-cth%d-rt%d",
                getClass().getSimpleName(), type, Integer.toString(num)
                        .replaceAll("000000$", "m")
                        .replaceAll("000$", "k"),
                threads, readThreads, routerThreads);
    }

    static String file = "target/msgassoc-time.csv";
    static void save(String title, Duration d) {
        long milli = d.getNano() / 1_000_000L;
        double s = d.getSeconds() + (milli / (double) 1000);
        try (PrintWriter w = new PrintWriter(new FileWriter(file, true))) {
            w.println(String.format("%s,%5.2f", title, s));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    static class FinishActor extends ActorDefault {
        int count;
        int num;
        Instant start;
        String title;
        public FinishActor(ActorSystem system, int num, String title) {
            super(system);
            this.num = num;
            start = Instant.now();
            this.title = title;
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
                Duration d = Duration.between(start, Instant.now());
                log(String.format("finish: %,d: %s", count, d));
                getSystem().close();
                save(title, d);
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
