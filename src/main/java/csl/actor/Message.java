package csl.actor;

import java.io.Serializable;
import java.util.function.Function;

public class Message<DataType> implements Serializable {
    public static final long serialVersionUID = 1L;

    public ActorRef target;
    public DataType data;

    public Message() {}

    public Message(ActorRef target, DataType data) {
        this.target = target;
        this.data = data;
    }

    public ActorRef getTarget() {
        return target;
    }

    public DataType getData() {
        return data;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + data + " : " + target + ")";
    }

    public String toString(Function<Object,Object> dataToStr) {
        return getClass().getSimpleName() + "(" + dataToStr.apply(data) + " : " + target + ")";
    }

    public Message<DataType> renewTarget(ActorRef target) {
        return new Message<>(target, data);
    }

    public int dataSize() {
        return 1;
    }

    /**
     * the special message intended to start processing of the target
     */
    public static class MessageNone extends Message<Void> {
        public static final long serialVersionUID = 1L;

        public MessageNone() {}

        public MessageNone(ActorRef target) {
            super(target, null);
        }

        @Override
        public Message<Void> renewTarget(ActorRef target) {
            return new MessageNone(target);
        }
    }

    /**
     * indicates the type can be sent as a message. (actors also can handle a regular object as an application message)
     */
    public interface MessageData {}
    public interface MessageDataDelayed extends MessageData { }
    public interface MessageDataSpecial extends MessageData { }
    public interface MessageDataResponse extends MessageDataSpecial { }

    public static Object unwrapHolder(Object data) {
        if (data instanceof MessageDataHolder<?>) {
            return unwrapHolder(((MessageDataHolder<?>) data).getData());
        } else {
            return data;
        }
    }

    public interface MessageDataHolder<T> extends MessageData {
        T getData();
    }

    public static class MessageDataClock<T> implements Serializable, MessageDataHolder<T> {
        public static final long serialVersionUID = 1L;
        public int clock;
        public T body;

        public MessageDataClock() {}

        public MessageDataClock(int clock, T body) {
            this.clock = clock;
            this.body = body;
        }

        @Override
        public T getData() {
            return null;
        }

        @Override
        public String toString() {
            return "clock(" +
                    clock +
                    ", body=" + body +
                    ')';
        }
    }

    public static <T> MessageDataPacket<T> dataWithSender(T data, ActorRef sender) {
        return new MessageDataPacket<>(data, sender);
    }

    public static class MessageDataPacket<T> implements Serializable, MessageDataHolder<T> {
        public static final long serialVersionUID = 1;
        public T data;
        public ActorRef sender;
        public MessageDataPacket() {}

        public MessageDataPacket(T data, ActorRef sender) {
            this.data = data;
            this.sender = sender;
        }

        @Override
        public T getData() {
            return data;
        }

        public ActorRef getSender() {
            return sender;
        }

        @Override
        public String toString() {
            return "pack(" +
                    data +
                    " <- " + sender +
                    ')';
        }
    }
}
