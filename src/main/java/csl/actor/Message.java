package csl.actor;

import java.io.Serializable;
import java.util.function.Function;

public class Message<DataType> implements Serializable {
    public static final long serialVersionUID = 1L;

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
        return getClass().getSimpleName() + "(" + data + " : " + target + " <- " + sender + ")";
    }

    public String toString(Function<Object,Object> dataToStr) {
        return getClass().getSimpleName() + "(" + dataToStr.apply(data) + " : " + target + " <- " + sender + ")";
    }

    /**
     * the special message intended to start processing of the target
     */
    public static class MessageNone extends Message<Void> {
        public static final long serialVersionUID = 1L;
        public MessageNone(ActorRef target) {
            super(target, null, null);
        }
    }
}
