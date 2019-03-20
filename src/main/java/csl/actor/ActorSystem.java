package csl.actor;

import java.util.concurrent.Executor;

public interface ActorSystem extends Executor {
    void register(Actor actor);
    void send(Message<?> message);
}
