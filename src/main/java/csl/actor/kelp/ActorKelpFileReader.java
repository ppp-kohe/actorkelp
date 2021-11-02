package csl.actor.kelp;

import csl.actor.ActorRef;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface ActorKelpFileReader<SelfType extends ActorKelp<SelfType>> extends ActorRef, KelpStage<SelfType>  {
    default CompletableFuture<KelpStageGraphActor> startReading(String path) {
        return startReading(path, Instant.now());
    }

    default CompletableFuture<KelpStageGraphActor> startReading(String path, Instant startTime) {
        return startReading(path, startTime, (s) -> {});
    }


    default CompletableFuture<KelpStageGraphActor> startReading(String path, Consumer<KelpStageGraphActor> setup) {
        return startReading(path, Instant.now(), setup);
    }

    CompletableFuture<KelpStageGraphActor> startReading(String path, Instant startTime, Consumer<KelpStageGraphActor> setup);
}
