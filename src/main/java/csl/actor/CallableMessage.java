package csl.actor;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Objects;

/**
 * the task interface.
 *  To apply the task to an actor, send {@link CallablePacket} to the actor by {@link #withSender(ActorRef)}
 * @param <ActorType> the target actor type executing the message
 * @param <RetType> the value type returned to the sender
 */
@FunctionalInterface
public interface CallableMessage<ActorType extends Actor,RetType> extends Serializable, Message.MessageData {

    RetType call(ActorType self);

    default CallablePacket<ActorType, RetType> withSender(ActorRef sender) {
        return new CallablePacket<>(this, sender);
    }


    class CallableFailure implements Serializable, Message.MessageDataResponse {
        public static final long serialVersionUID = 1L;
        public Throwable error;
        public String trace;

        public CallableFailure() {}

        public CallableFailure(Throwable error) {
            this.error = error;
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            error.printStackTrace(pw);
            pw.close();
            trace = sw.getBuffer().toString();
        }

        public Throwable getError() {
            return error;
        }

        public String getTrace() {
            return trace;
        }

        @Override
        public String toString() {
            return "failure(" + trace + ")";
        }
    }

    static <ActorType extends Actor,RetType> CallableMessage<ActorType,RetType> callableMessage(CallableMessage<ActorType,RetType> m) {
        return m;
    }

    static <ActorType extends Actor> CallableMessageConsumer<ActorType> callableMessageConsumer(CallableMessageConsumer<ActorType> m) {
        return m;
    }

    interface CallableMessageConsumer<ActorType extends Actor> extends CallableMessage<ActorType,CallableResponseVoid> {
        void accept(ActorType self);

        default CallableResponseVoid call(ActorType self) {
            accept(self);
            return Void;
        }
    }

    class CallableResponseVoid implements Serializable, Message.MessageDataResponse {
        public static final long serialVersionUID = 1L;
        @Override
        public String toString() {
            return "void";
        }
    }

    CallableResponseVoid Void = new CallableResponseVoid();

    class CallableResponse<V> implements Serializable, Message.MessageDataResponse {
        public static final long serialVersionUID = 1L;
        public V value;

        public CallableResponse() {}

        public CallableResponse(V value) {
            this.value = value;
        }

        public V getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "res(" + value + ")";
        }
    }

    class CallablePacket<ActorType extends Actor, RetType> extends Message.MessageDataPacket<CallableMessage<ActorType, RetType>> {
        public static final long serialVersionUID = 1;

        public CallablePacket() {}

        public CallablePacket(CallableMessage<ActorType, RetType> task, ActorRef sender) {
            super(task, sender);
        }
    }
}
