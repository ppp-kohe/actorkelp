package csl.actor;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Objects;

@FunctionalInterface
public interface CallableMessage<ActorType extends Actor,RetType> extends Serializable {
    default RetType call(ActorType self, ActorRef sender) {
        return call(self);
    }

    RetType call(ActorType self);


    class CallableFailure implements Serializable {
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
        default void accept(ActorType self, ActorRef sender) {
            accept(self);
        }

        void accept(ActorType self);

        @Override
        default CallableResponseVoid call(ActorType self, ActorRef sender) {
            accept(self, sender);
            return Void;
        }

        default CallableResponseVoid call(ActorType self) {
            accept(self);
            return Void;
        }
    }

    class CallableResponseVoid implements Serializable {
        public static final long serialVersionUID = 1L;
        @Override
        public String toString() {
            return "void";
        }
    }

    CallableResponseVoid Void = new CallableResponseVoid();

    class CallableResponse<V> implements Serializable {
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
}
