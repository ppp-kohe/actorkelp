package csl.actor;

import java.io.Serializable;

public class Message<DataType> implements Serializable {

    protected ActorRef target;
    protected ActorRef sender;
    protected DataType data;

    public Message(ActorRef target, ActorRef sender, DataType data) {
        this.target = target;
        this.sender = sender;
        this.data = data;
    }

    public ActorRef getTarget() {
        return target;
    }

    public ActorRef getSender() {
        return sender;
    }

    public DataType getData() {
        return data;
    }

    @Override
    public String toString() {
        return "Message(" + target + " ! " + data + " <- " + sender + ")";
    }

    /**
     * the special message intended to start processing of the target
     */
    public static class MessageNone extends Message<Void> {
        public MessageNone(ActorRef target) {
            super(target, null, null);
        }
    }
}
