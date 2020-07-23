package csl.actor.kelp;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.util.FileSplitter;
import csl.actor.util.PathModifier;

import java.util.concurrent.CompletableFuture;

public class FileMapper extends ActorKelp<FileMapper> {
    protected FileSplitter splitter;
    protected long splitCount;

    public FileMapper(ActorSystem system, String name, Config config, State state) {
        super(system, name, config, state);
        this.splitter = splitter(system, config);

    }

    public FileMapper(ActorSystem system, String name, Config config, FileSplitter splitter) {
        super(system, name, config);
        this.splitter = splitter;
    }

    public FileMapper(ActorSystem system, String name, Config config) {
        this(system, name, config, splitter(system, config));
    }

    public static FileSplitter splitter(ActorSystem system, Config config) {
        return config.fileMapperSplitByCount ?
                FileSplitter.getWithSplitCount(config.fileMapperSplitCount, PathModifier.getPathModifier(system)) :
                FileSplitter.getWithSplitLength(config.fileMapperSplitLength, PathModifier.getPathModifier(system));
    }

    @Override
    protected ActorBehavior initBehavior() {
        return behaviorBuilder()
                .matchWithSender(FileSplitter.FileSplit.class, this::read)
                .build();
    }

    @Override
    protected void initMerged(FileMapper m) {
        splitCount = Math.max(splitCount, m.splitCount);
    }

    /**
     * shorthand of <code>tell(new FileSplit(path)); PhaseShift.start(path, getSystem(), this)</code>
     * @param path the reading file
     * @return a future returned by {@link PhaseShift#start(Object, ActorSystem, ActorRef)}
     */
    public CompletableFuture<PhaseShift.PhaseCompleted> startReadFile(String path) {
        tell(new FileSplitter.FileSplit(path));
        return PhaseShift.start(path, getSystem(), this);
    }

    protected void read(FileSplitter.FileSplit s, ActorRef sender) {
        try {
            if (s.getFileLength() == 0) { //the top FileSplit, (created by new FileSplit(path))
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
            throw new RuntimeException("splitter=" + splitter + " split=" + s + " nextStage=" + nextStage, ex);
        }
    }

    public long getSplitCount() {
        return splitCount;
    }
}
