package csl.actor.kelp;

import csl.actor.ActorSystem;
import csl.actor.kelp.actors.ActorKelpFileReader;
import csl.actor.kelp.actors.ActorKelpFileWriter;
import csl.actor.kelp.actors.ActorKelpLambda;
import csl.actor.kelp.actors.ActorKelpSubProcess;

import java.util.function.Function;

public interface ActorKelpBuilder {
    ActorSystem system();
    ConfigKelp config();

    default ActorKelpInternalFactory internalFactory() {
        return ActorKelpInternalFactory.DEFAULT_FACTORY;
    }

    default ActorKelpLambda actor(ActorKelpLambda.ActorBuilder builderFunction) {
        return internalFactory().actor(system(), config(), builderFunction);
    }

    default ActorKelpLambda actor(String name, ActorKelpLambda.ActorBuilder builderFunction) {
        return internalFactory().actor(system(), config(), name, builderFunction);
    }

    default ActorKelpFileReader actorReader() {
        return internalFactory().actorReader(system(), config());
    }

    default ActorKelpFileReader actorReader(String name) {
        return internalFactory().actorReader(system(), config(), name);
    }

    default ActorKelpFileWriter actorWriter() {
        return internalFactory().actorWriter(system(), config());
    }

    default ActorKelpFileWriter actorWriter(String name) {
        return internalFactory().actorWriter(system(), config(), name);
    }

    default ActorKelpSubProcess actorSubProcess(Function<ActorKelpSubProcess.ProcessSource, ActorKelpSubProcess.ProcessSource> init) {
        return internalFactory().actorSubProcess(system(), config(), init);
    }

    default ActorKelpSubProcess actorSubProcess(String name, Function<ActorKelpSubProcess.ProcessSource, ActorKelpSubProcess.ProcessSource> init) {
        return internalFactory().actorSubProcess(system(), config(), name, init);
    }
}
