package csl.actor.kelp2;

import csl.actor.ActorRef;
import csl.actor.util.StagingActor;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public interface ActorKelpFileReader extends ActorRef  {
    default CompletableFuture<StagingActor.StagingCompleted> startReading(String path) {
        return startReading(path, Instant.now());
    }

    CompletableFuture<StagingActor.StagingCompleted> startReading(String path, Instant startTime);
}
