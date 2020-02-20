package csl.actor.example.wordcount;

import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;
import csl.actor.example.msgassoc.DebugBehavior;
import csl.actor.msgassoc.*;
import csl.actor.remote.KryoBuilder;

import java.io.*;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WordCount {
    public static void main(String[] args) throws Exception {
        String src = args[0];
        String dst = args[1];
        ActorSystemDefault system = new ActorSystemDefault();
        pool = new KryoBuilder.SerializerPoolDefault(system);

        Config conf = Config.readConfig(System.getProperties());
        conf.log(conf.toString());

        PhaseShift.PhaseTerminalActor finisher = new PhaseShift.PhaseTerminalActor(system, true);

        FileMapper fileReader = new FileMapper(system, "fileReader", conf, FileSplitter.getWithSplitCount(10));
        WordCountMapper mapper = new WordCountMapper(system, "mapper", conf);
        WordCountReducer reducer = new WordCountReducer(system, "reducer", conf, dst);

        fileReader.setNextStage(mapper).get();
        mapper.setNextStage(reducer).get();
        fileReader.routerSplit(3);
        mapper.routerSplit(2);

        fileReader.tell(new FileSplitter.FileSplit(src), finisher);

//        system.getScheduledExecutor().scheduleAtFixedRate(() -> {
//            ResponsiveCalls.sendTaskConsumer(mapper, ActorVisitor.visitorNoSender(a -> a.printStatus("(scheduled) mapper:")));
//            ResponsiveCalls.sendTaskConsumer(reducer, ActorVisitor.visitorNoSender(a -> a.printStatus("(scheduled) reducer:")));
//            if (finisher.getCompletedCount(src) > 0) {
//                try {
//                    Thread.sleep(2_000);
//                } catch (Exception ex) { ex.printStackTrace(); }
//                system.close();
//            }
//        }, 10, 10, TimeUnit.SECONDS);

        //system.getExecutorService().awaitTermination(1, TimeUnit.HOURS);

    }

    public static class FileMapper extends ActorAggregationReplicable {
        FileSplitter splitter;
        long splitCount;

        public FileMapper(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        public FileMapper(ActorSystem system, String name, Config config, FileSplitter splitter) {
            this(system, name, config);
            this.splitter = splitter;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchWithSender(FileSplitter.FileSplit.class, this::read)
                    .build();
        }

        @Override
        protected void initMerged(ActorAggregationReplicable m) {
            FileMapper fm = (FileMapper) m;
            splitCount = Math.max(splitCount, fm.splitCount);
        }

        void read(FileSplitter.FileSplit s, ActorRef sender) {
            try {
                if (s.fileLength == 0) {
                    splitter.splitIterator(s.path)
                            .forEachRemaining(this::tell);
                    router().tell(new PhaseShift(s.path, sender));
                } else {
                    splitCount = Math.max(splitCount, s.splitIndex);
                    splitter.openLineIterator(s)
                            .forEachRemaining(nextStage()::tell);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static class WordCountMapper extends ActorAggregationReplicable {
        public WordCountMapper(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, line -> Arrays.stream(line.split("\\W+"))
                            .forEach(w -> nextStage().tell(new Count(w, 1), this)))
                    .build();
        }
    }

    public static class WordCountReducer extends ActorAggregationReplicable {
        PrintWriter writer;
        String dst;
        ScheduledFuture<?> flushTask;

        public WordCountReducer(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        public WordCountReducer(ActorSystem system, String name, Config config, String dst) {
            super(system, name, config);
            this.dst = dst;
        }

//        @Override
//        protected StateSplitRouter initStateRouter() {
//            return new KeyHistogramSizeChecker.StateSplitRouterSizeDebug();
//        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKeyFactory(new DebugBehavior.DebugFactory())
                    .matchKey(Count.class, Count::getWord)
                        .fold((k,vs) -> vs.stream().reduce(new Count(k, 0), Count::add))
                        .eventually()
                    .forEach(this::write)
                    .build();
        }

        @Override
        protected void initClone(ActorAggregationReplicable original) {
            super.initClone(original);
            initDebug();
        }

        private void initDebug() {
            state1 = null;
            state2 = null;
            thread1 = null;
            thread2 = null;
            proc = false;
        }

        volatile boolean proc;
        volatile Thread thread1;
        volatile Thread thread2;
        volatile State state1;
        volatile State state2;

        AtomicLong count = new AtomicLong();

        @Override
        protected void processMessage(Message<?> message) {
            boolean err = false;
            if (proc) {
                if (!isRouterParallelRouting()) {
                    err = true;
                    state2 = state;
                    thread2 = Thread.currentThread();
                    System.err.println(String.format("%s: %s vs %s : %s vs %s", this, thread1, thread2, state1, state2));
                    printStack();
                }
            } else {
                thread1 = Thread.currentThread();
                state1 = state;
            }
            proc = true;
            try {
                long n = count.incrementAndGet();
                if (n % 1000_000L == 0 && state instanceof StateLeaf) {
                    save(getMailboxAsReplicable().getTable(0), String.format("%%05d-proc-%d.obj", n));
                }

                super.processMessage(message);
            } catch (Exception ex) {
                System.err.println(this + " :" + ex + " thread1:" + thread1 + " thread2:" + thread2);
                printStack();
                throw ex;
            }
            proc = false;
            if (!err && thread2 != null) {
                try {
                    while (thread2 != null) {
                        System.err.println("just a moment");
                        Thread.sleep(100);
                    }
                } catch (Exception ex) {

                }
            }
            thread2 = null;
            thread1 = null;
            state1 = null;
            state2= null;
        }
        private void printStack() {
            synchronized (System.err) {
                System.err.println("thread1 stack: ");
                if (thread1 != null) {
                    for (StackTraceElement e : thread1.getStackTrace()) {
                        System.err.println("   " + e);
                    }
                } else {
                    System.err.println("null");
                }
                System.err.println("---------------");
                System.err.println("thread2 stack: ");
                if (thread2 != null) {
                    for (StackTraceElement e : thread2.getStackTrace()) {
                        System.err.println("   " + e);
                    }
                } else {
                    System.err.println("null");
                }
                System.err.println("===============");
            }
        }

        @Override
        protected void initMerged(ActorAggregationReplicable m) {
            ((WordCountReducer) m).close();
            initDebug();
        }

        void close() {
            if (flushTask != null) {
                flushTask.cancel(true);
                flushTask = null;
            }
            if (writer != null) {
                writer.close();
                writer = null;
            }
        }

        void write(Count count) {
            try {
                if (writer == null) {
                    File dir = new File(dst);
                    dir.mkdirs();
                    writer = new PrintWriter(new FileWriter(new File(dir, String.format("output-%d.txt", System.identityHashCode(this))))); //TODO
                    flushTask = getSystem().getScheduledExecutor().scheduleAtFixedRate(() ->
                            this.tell(CallableMessage.callableMessageConsumer((a,s) -> writer.flush())), 3, 3, TimeUnit.SECONDS);
                }

                writer.println(count);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public void processPhaseEnd(Object phaseKey) {
            super.processPhaseEnd(phaseKey);
            printStatus("phaseEnd: " + phaseKey);
        }

        @Override
        public SplitLeaf newSplitLeaf(ActorRef actor, int depth) {
            return new DebugSplitLeaf(actor, depth);
        }

        @Override
        public void internalMerge(ActorAggregationReplicable merged) {
            getMailboxAsReplicable().lockRemainingProcesses();
            merged.getMailboxAsReplicable().lockRemainingProcesses();

            save(new Object[] {
                    getMailboxAsReplicable().getTable(0),
                    merged.getMailboxAsReplicable().getTable(0)}, "%05d-merge-A.obj");

            getMailboxAsReplicable()
                    .merge(merged.getMailboxAsReplicable());

            save(getMailboxAsReplicable().getTable(0), "%05d-merge-B.obj");

            merged.internalCancel();
            try {
                initMerged(merged);
            } finally {
                merged.getMailboxAsReplicable().unlockRemainingProcesses(merged);
                getMailboxAsReplicable().unlockRemainingProcesses(this);
            }
        }
    }

    public static class Count implements Serializable {
        public String word;
        public long count;

        public Count(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public Count add(Count b) {
            count += b.count;
            return this;
        }

        @Override
        public String toString() {
            return "(" + word + "," + count + ")";
        }
    }

    static KryoBuilder.SerializerPool pool;
    static AtomicInteger saveCount = new AtomicInteger();

    public static class DebugSplitLeaf extends ActorAggregationReplicable.SplitLeaf {
        public DebugSplitLeaf(ActorRef actor, int depth) {
            super(actor, depth);
        }

        @Override
        protected ActorAggregationReplicable.Split split(ActorAggregationReplicable router, int height, ActorAggregationReplicable self) {
            try {
                self.getMailboxAsReplicable().lockRemainingProcesses();
                save(self.getMailboxAsAggregation().getTable(0), "%05d-split-A.obj");

                ActorRef routerRef = router.router();
                ActorAggregationReplicable a1 = self.internalCreateClone(routerRef);
                ActorAggregationReplicable a2 = self.internalCreateClone(routerRef);
                List<Object> splitPoints = self.getMailboxAsReplicable()
                        .splitMessageTableIntoReplicas(a1.getMailboxAsReplicable(), a2.getMailboxAsReplicable());

                save(new Object[]{a1.getMailboxAsAggregation().getTable(0),
                                a2.getMailboxAsAggregation().getTable(0)},
                        "%05d-split-B.obj");


                if (router != self) {
                    self.internalCancel();
                }
                return router.internalCreateSplitNode(splitPoints, a1, a2, depth, height);
            } finally {
                self.getMailboxAsReplicable().unlockRemainingProcesses(self);
            }
        }
    }

    public static void save(Object obj, String name) {
        File file = new File("target/debug-split", String.format(name, saveCount.incrementAndGet()));
        file.getParentFile().mkdirs();
        try (Output out = new Output(new FileOutputStream(file))) {
            pool.write(out, obj);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
