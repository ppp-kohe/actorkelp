package csl.actor.kelp;

import csl.actor.ActorRef;
import csl.actor.util.StagingActor;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface ActorKelpFileReader<SelfType extends ActorKelp<SelfType>> extends ActorRef, KelpStage<SelfType>  {
    default CompletableFuture<StagingActor.StagingCompleted> startReading(String path) {
        return startReading(path, Instant.now());
    }

    default CompletableFuture<StagingActor.StagingCompleted> startReading(String path, Instant startTime) {
        return startReading(path, startTime, (s) -> {});
    }


    default CompletableFuture<StagingActor.StagingCompleted> startReading(String path, Consumer<StagingActor> setup) {
        return startReading(path, Instant.now(), setup);
    }

    CompletableFuture<StagingActor.StagingCompleted> startReading(String path, Instant startTime, Consumer<StagingActor> setup);
}
