package csl.actor;

import java.util.concurrent.Executor;

public interface ActorSystem extends Executor {
    void register(Actor actor);
    void unregister(String actorName);
    void send(Message<?> message);

    Actor resolveActorLocalNamed(ActorRefLocalNamed ref);
}
