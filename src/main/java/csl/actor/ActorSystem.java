package csl.actor;

public interface ActorSystem {
    void send(Message<?> message);
}
