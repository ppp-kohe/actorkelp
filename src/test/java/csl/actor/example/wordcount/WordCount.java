package csl.actor.example.wordcount;

import csl.actor.*;
import csl.actor.msgassoc.ActorAggregationReplicable;
import csl.actor.msgassoc.Config;
import csl.actor.msgassoc.ResponsiveCalls;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class WordCount {
    public static void main(String[] args) throws Exception {
        String src = args[0];
        String dst = args[1];
        ActorSystem system = new ActorSystemDefault();

        FileMapper fileReader = new FileMapper(system, "fileReader", FileSplitter.getWithSplitCount(10_000_000));
        WordCountMapper mapper = new WordCountMapper(system, "mapper", Config.CONFIG_DEFAULT);
        WordCountReducer reducer = new WordCountReducer(system, "reducer", Config.CONFIG_DEFAULT, dst);

        ResponsiveCalls.sendTaskConsumer(fileReader, (fr,s) -> fr.mapper = mapper).get();
        ResponsiveCalls.sendTaskConsumer(mapper, (m,s) -> m.reducer = reducer).get();

        fileReader.routerSplit(3);

        fileReader.tell(new FileSplitter.FileSplit(src));
    }

    public static class FileMapper extends ActorAggregationReplicable {
        ActorRef mapper;
        FileSplitter splitter;
        long splitCount;

        public FileMapper(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        public FileMapper(ActorSystem system, String name, FileSplitter splitter) {
            super(system, name);
            this.splitter = splitter;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(FileSplitter.FileSplit.class, this::read)
                    .build();
        }

        @Override
        protected void initMerged(ActorAggregationReplicable m) {
            FileMapper fm = (FileMapper) m;
            splitCount = Math.max(splitCount, fm.splitCount);
        }

        void read(FileSplitter.FileSplit s) {
            try {
                if (s.fileLength == 0) {
                    splitter.splitIterator(s.path)
                            .forEachRemaining(this::tell);
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
                    .match(String.class, line -> Arrays.stream(line.split("\\W+"))
                            .forEach(w -> reducer.tell(new Count(w, 1), this)))
                    .build();
        }

        ConcurrentLinkedQueue<Thread> runningThreads = new ConcurrentLinkedQueue<Thread>();
        @Override
        protected void processMessage(Message<?> message) {
            Thread t = Thread.currentThread();
            runningThreads.add(t);
            try {
                super.processMessage(message);
            } catch (Exception ex) {
                List<Thread> ts = new ArrayList<>(runningThreads);
                System.err.println("error " + t + " : " + ex);
                for (Thread pt : ts) {
                    StackTraceElement[] traces = pt.getStackTrace();
                    System.err.println(" thread: " + pt);
                    for (StackTraceElement e: traces) {
                        System.err.println("   " + e);
                    }
                }
                throw ex;
            }
            runningThreads.removeIf(et -> et==t);
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

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(Count.class, Count::getWord)
                    .fold((k,vs) -> vs.stream().reduce(new Count(k, 0), Count::add))
                    .forEach(this::write)
                    .build();
        }

        ConcurrentLinkedQueue<Thread> runningThreads = new ConcurrentLinkedQueue<Thread>();
        @Override
        protected void processMessage(Message<?> message) {
            Thread t = Thread.currentThread();
            runningThreads.add(t);
            try {
                super.processMessage(message);
            } catch (Exception ex) {
                List<Thread> ts = new ArrayList<>(runningThreads);
                System.err.println("error " + t + " : " + ex);
                for (Thread pt : ts) {
                    StackTraceElement[] traces = pt.getStackTrace();
                    System.err.println(" thread: " + pt);
                    for (StackTraceElement e: traces) {
                        System.err.println("   " + e);
                    }
                }
                throw ex;
            }
            runningThreads.removeIf(et -> et==t);
        }

        @Override
        protected void initMerged(ActorAggregationReplicable m) {
            ((WordCountReducer) m).close();
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
