package csl.actor.example.wordcount;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.cluster.FileSplitter;
import csl.actor.example.LockExample;
import csl.actor.keyaggregate.ActorKeyAggregation;
import csl.actor.keyaggregate.Config;
import csl.actor.cluster.PhaseShift;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileSplitterExample2 {
    final static AtomicBoolean error = new AtomicBoolean();
    public static void main(String[] args) throws Exception {
        String src = args[0];
        for (int n = 0; n < 20; ++n) {
            if (error.get()) {
                System.err.println("error");
                break;
            }
            System.err.println("loop: "+ n);
            ActorSystemDefault system = new ActorSystemDefault();

            long max = Long.MAX_VALUE;

            FileMapper mapper = new FileMapper(system, "mapper", Config.CONFIG_DEFAULT, FileSplitter.getWithSplitCount(10));

            LockExample.TreeActor t = new LockExample.TreeActor(system, "root1", LockExample.MapActor::new);
            LockExample.TreeActor t2 = new LockExample.TreeActor(system, "root2", LockExample.LeafActor::new);
            LockExample.EndActor e = new LockExample.EndActor(system, "end", max);

            mapper.setNextStage(t).get();
            t.set(new LockExample.Setting(10, t2));
            t2.set(new LockExample.Setting(10, e));
            Thread.sleep(100);

            mapper.tell(new FileSplitter.FileSplit(src));
            Thread.sleep(2000);
            mapper.tell(new PhaseShift(src, e));

            system.getExecutorService().awaitTermination(1, TimeUnit.HOURS);
        }
    }

    public static class FileMapper extends ActorKeyAggregation {
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
        protected void initMerged(ActorKeyAggregation m) {
            csl.actor.keyaggregate.FileMapper fm = (csl.actor.keyaggregate.FileMapper) m;
            splitCount = Math.max(splitCount, fm.splitCount);
        }

        void read(FileSplitter.FileSplit s, ActorRef sender) {
            try {
                if (s.getFileLength() == 0) {
                    splitter.splitIterator(s.getPath())
                            .forEachRemaining(this::tell);
                    if (sender != null) {
                        router().tell(new PhaseShift(s.getPath(), sender));
                    }
                } else {
                    splitCount = Math.max(splitCount, s.getSplitIndex());
                    splitter.openLineIterator(s)
                            .forEachRemaining(nextStage()::tell);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
