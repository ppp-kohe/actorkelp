package csl.actor;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public interface ActorSystem extends Executor, AutoCloseable {
    void register(Actor actor);
    void unregister(String actorName);
    void send(Message<?> message);

    Actor resolveActorLocalNamed(ActorRefLocalNamed ref);
    void close();

    int getThreads();
    ScheduledExecutorService getScheduledExecutor();

    void awaitClose(long time, TimeUnit unit) throws InterruptedException;
}
