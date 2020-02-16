package csl.actor.example.wordcount;

import csl.actor.*;
import csl.actor.msgassoc.*;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class WordCount {
    public static void main(String[] args) throws Exception {
        String src = args[0];
        String dst = args[1];
        ActorSystem system = new ActorSystemDefault();

        Config conf = Config.readConfig(System.getProperties());
        conf.log(conf.toString());

        PhaseShift.PhaseFinishActor finisher = new PhaseShift.PhaseFinishActor(system, false);

        FileMapper fileReader = new FileMapper(system, "fileReader", conf, FileSplitter.getWithSplitCount(10));
        WordCountMapper mapper = new WordCountMapper(system, "mapper", conf);
        WordCountReducer reducer = new WordCountReducer(system, "reducer", conf, dst);

        ResponsiveCalls.sendTaskConsumer(fileReader, (fr,s) -> fr.mapper = mapper).get();
        ResponsiveCalls.sendTaskConsumer(mapper, (m,s) -> m.reducer = reducer).get();

        fileReader.routerSplit(3);

        fileReader.tell(new FileSplitter.FileSplit(src), finisher);

        system.getScheduledExecutor().scheduleAtFixedRate(() -> {
            ResponsiveCalls.sendTaskConsumer(mapper, (m,s) -> m.printStatus());
            ResponsiveCalls.sendTaskConsumer(reducer, (m,s) -> m.printStatus());
        }, 10, 10, TimeUnit.SECONDS);
    }

    public static class FileMapper extends ActorAggregationReplicable {
        ActorRef mapper;
        FileSplitter splitter;
        long splitCount;

        public FileMapper(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        public FileMapper(ActorSystem system, String name, Config config, FileSplitter splitter) {
            super(system, name);
            this.splitter = splitter;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchWithSender(FileSplitter.FileSplit.class, this::read)
                    .match(PhaseShift.PhaseShiftCompleted.class, shift -> shift.redirectTo(mapper))
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
                            .forEachRemaining(mapper::tell);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static class WordCountMapper extends ActorAggregationReplicable {
        ActorRef reducer;
        public WordCountMapper(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(PhaseShift.PhaseShiftCompleted.class, s -> s.redirectTo(reducer))
                    .match(String.class, line -> Arrays.stream(line.split("\\W+"))
                            .forEach(w -> reducer.tell(new Count(w, 1), this)))
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
                    .match(PhaseShift.PhaseShiftCompleted.class, PhaseShift.PhaseShiftCompleted::sendToTarget)
                    .matchKey(Count.class, Count::getWord)
                    .fold((k,vs) -> vs.stream().reduce(new Count(k, 0), Count::add))
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
        protected void processMessageDelayWhileParallelRouting(Message<?> message) {
            super.processMessageDelayWhileParallelRouting(message);
            log("WordCount: parallel routing delay: " + message);
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
}
