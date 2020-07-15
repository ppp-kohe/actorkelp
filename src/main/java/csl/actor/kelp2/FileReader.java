package csl.actor.kelp2;

import csl.actor.ActorBehavior;
import csl.actor.ActorSystem;
import csl.actor.util.FileSplitter;
import csl.actor.util.StagingActor;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public class FileReader extends ActorKelp<FileReader> {
    public FileReader(ActorSystem system, String name) {
        super(system, name);
    }

    public FileReader(ActorSystem system) {
        super(system);
    }

    @Override
    protected ActorBehavior initBehavior() {
        return behaviorBuilder()
                .build();
    }

    public CompletableFuture<StagingActor.StagingCompleted> start(String path) {
        return start(path, Instant.now());
    }

    public CompletableFuture<StagingActor.StagingCompleted> start(String path, Instant startTime) {
        tell(new FileSplitter.FileSplit(path));
        return StagingActor.staging(system)
                .withStartTime(startTime)
                .withWatcherSleepTimeMs(3)
                .start(this);
    }
}
