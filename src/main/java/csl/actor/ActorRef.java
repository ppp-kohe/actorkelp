package csl.actor;

public interface ActorRef {
    void tell(Object data, ActorRef sender);
}
