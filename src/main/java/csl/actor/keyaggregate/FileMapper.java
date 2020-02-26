package csl.actor.keyaggregate;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.cluster.FileSplitter;
import csl.actor.cluster.PhaseShift;

import java.util.concurrent.CompletableFuture;

public class FileMapper extends ActorKeyAggregation {
    protected FileSplitter splitter;
    protected long splitCount;

    public FileMapper(ActorSystem system, String name, Config config, State state) {
        super(system, name, config, state);
    }

    public FileMapper(ActorSystem system, String name, Config config, FileSplitter splitter) {
        super(system, name, config);
        this.splitter = splitter;
    }

    public FileMapper(ActorSystem system, String name, Config config) {
        this(system, name, config,
                config.fileMapperSplitByCount ?
                        FileSplitter.getWithSplitCount(config.fileMapperSplitCount) :
                        FileSplitter.getWithSplitLength(config.fileMapperSplitLength));
    }

    @Override
    protected ActorBehavior initBehavior() {
        return behaviorBuilder()
                .matchWithSender(FileSplitter.FileSplit.class, this::read)
                .build();
    }

    @Override
    protected void initMerged(ActorKeyAggregation m) {
        FileMapper fm = (FileMapper) m;
        splitCount = Math.max(splitCount, fm.splitCount);
    }

    public CompletableFuture<PhaseShift.PhaseCompleted> startReadFile(String path) {
        tell(new FileSplitter.FileSplit(path));
        return PhaseShift.start(path, getSystem(), this);
    }

    protected void read(FileSplitter.FileSplit s, ActorRef sender) {
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

    public long getSplitCount() {
        return splitCount;
    }
}
