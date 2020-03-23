package csl.actor.example.exp.wordcount;

import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.cluster.FileSplitter;
import csl.actor.cluster.PhaseShift;
import csl.actor.keyaggregate.ActorKeyAggregation;
import csl.actor.keyaggregate.Config;
import csl.actor.keyaggregate.FileMapper;
import csl.actor.keyaggregate.KeyAggregationRoutingSplit;
import csl.actor.remote.KryoBuilder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
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

        Config conf = Config.readConfig(Config.class, System.getProperties());
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

    public static class WordCountMapper extends ActorKeyAggregation {
        public WordCountMapper(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        public WordCountMapper(ActorSystem system, String name, Config config, State state) {
            super(system, name, config, state);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(String.class, line -> Arrays.stream(line.split("\\W+"))
                            .forEach(w -> nextStage().tell(new Count(w, 1), this)))
                    .build();
        }
    }

    public static class DebugSplitLeaf extends KeyAggregationRoutingSplit.RoutingSplitLeaf {
        public DebugSplitLeaf(ActorRef actor, SplitPath path) {
            super(actor, path);
            if (actor == null) {
                throw new RuntimeException("null actor: " + path);
            }
        }

        @Override
        public RoutingSplitLeaf newLeaf(ActorRef actor, SplitPath path) {
            return new DebugSplitLeaf(actor, path);
        }
    }

    public static class WordCountReducer extends ActorKeyAggregation {
        PrintWriter writer;
        String dst;
        ScheduledFuture<?> flushTask;

        public WordCountReducer(ActorSystem system, String name, Config config, State state) {
            super(system, name, config, state);
        }

        public WordCountReducer(ActorSystem system, String name, Config config, String dst) {
            super(system, name, config);
            this.dst = dst;
        }
        /*
        @Override
        public KeyAggregationRoutingSplit.RoutingSplitLeaf newSplitLeaf(ActorRef actor, KeyAggregationRoutingSplit.SplitPath path) {
            return new DebugSplitLeaf(actor, path);
        }*/

        //        @Override
//        protected StateSplitRouter initStateRouter() {
//            return new KeyHistogramSizeChecker.StateSplitRouterSizeDebug();
//        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    //.matchKeyFactory(new DebugBehavior.DebugFactory())
                    .matchKey(Count.class, Count::getWord)
                        .fold((k,vs) -> vs.stream().reduce(new Count(k, 0), Count::add))
                        .eventually()
                    .forEach(this::write)
                    .build();
        }

        @Override
        protected void initClone(ActorKeyAggregation original) {
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
        /*
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
                if (n % 1000_000L == 0 && state instanceof StateUnit) {
                    save(getMailboxAsKeyAggregation().getHistogram(0), String.format("%%05d-proc-%d.obj", n));
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
        }*/
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
        protected void initMerged(ActorKeyAggregation m) {
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
                    String p = Paths.get(dst, "%a/wcout-" + getOutputFileHeader() + ".txt")
                            .toString();
                    Path outFile = ConfigDeployment.getPathModifier(getSystem()).getExpanded(p);
                    log("path: " + ConfigDeployment.getPathModifier(getSystem()) + ".get(" + p  +")" + "\n -> " + outFile);
                    Files.createDirectories(outFile.getParent());
                    writer = new PrintWriter(new FileWriter(outFile.toFile()));
                    flushTask = getSystem().getScheduledExecutor().scheduleAtFixedRate(() ->
                            this.tell(CallableMessage.callableMessageConsumer((a) -> writer.flush())), 3, 3, TimeUnit.SECONDS);
                    log("start writing: " + outFile);
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
            close();
        }
        /*
        @Override
        public KeyAggregationRoutingSplit internalCreateSplitNode(KeyAggregationRoutingSplit.SplitOrMergeContext context,
                                                                  KeyAggregationRoutingSplit old,
                                                                  ActorKeyAggregation target, KeyAggregationRoutingSplit.SplitPath path, int height) {
            try {
                target.getMailboxAsKeyAggregation().lockRemainingProcesses();
                save(target.getMailboxAsKeyAggregation().getHistogram(0), "%05d-split-A.obj");

                ActorRef routerRef = router();
                ActorKeyAggregation a1 = target.internalCreateClone(routerRef);
                ActorKeyAggregation a2 = target.internalCreateClone(routerRef);
                List<Object> splitPoints = target.getMailboxAsKeyAggregation()
                        .splitMessageHistogramIntoReplicas(getSystem(), a1.getMailboxAsKeyAggregation(), a2.getMailboxAsKeyAggregation());

                save(new Object[]{a1.getMailboxAsKeyAggregation().getHistogram(0),
                                a2.getMailboxAsKeyAggregation().getHistogram(0)},
                        "%05d-split-B.obj");


                if (routerRef != target) {
                    target.internalCancel();
                }
                return internalCreateSplitNode(context, old, splitPoints, a1, a2, path, height);
            } finally {
                target.getMailboxAsKeyAggregation().unlockRemainingProcesses(target);
            }
        }

        @Override
        public void internalMerge(ActorKeyAggregation merged) {
            getMailboxAsKeyAggregation().lockRemainingProcesses();
            merged.getMailboxAsKeyAggregation().lockRemainingProcesses();

            save(new Object[] {
                    getMailboxAsKeyAggregation().getHistogram(0),
                    merged.getMailboxAsKeyAggregation().getHistogram(0)}, "%05d-merge-A.obj");

            getMailboxAsKeyAggregation()
                    .merge(merged.getMailboxAsKeyAggregation());

            save(getMailboxAsKeyAggregation().getHistogram(0), "%05d-merge-B.obj");

            merged.internalCancel();
            try {
                initMerged(merged);
            } finally {
                merged.getMailboxAsKeyAggregation().unlockRemainingProcesses(merged);
                getMailboxAsKeyAggregation().unlockRemainingProcesses(this);
            }
        }*/

        @Override
        protected Serializable toSerializableInternalState() {
            return this.dst;
        }

        @Override
        protected void initSerializedInternalState(Serializable s) {
            this.dst = (String) s;
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


    public static void save(Object obj, String name) {
        if (pool != null) {
            File file = new File("target/debug-split", String.format(name, saveCount.incrementAndGet()));
            file.getParentFile().mkdirs();
            try (Output out = new Output(new FileOutputStream(file))) {
                pool.write(out, obj);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
