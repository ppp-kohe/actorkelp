package csl.actor.kelp.shuffle;

import csl.actor.ActorRef;
import csl.actor.Message;

public class MessageAccepted<T> extends Message<T> {
    public static final long serialVersionUID = -1;

    public MessageAccepted(ActorRef target, ActorRef sender, T data) {
        super(target, sender, data);
    }
}
