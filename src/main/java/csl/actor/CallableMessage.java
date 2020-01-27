package csl.actor;

import java.io.Serializable;

@FunctionalInterface
public interface CallableMessage<T> extends Serializable {
    T call(Actor self, ActorRef sender);


    class CallableFailure implements Serializable {
        protected Throwable error;

        public CallableFailure(Throwable error) {
            this.error = error;
        }

        public Throwable getError() {
            return error;
        }

        @Override
        public String toString() {
            return "failure(" + error + ")";
        }
    }

    static <T> CallableMessage<T> callableMessage(CallableMessage<T> m) {
        return m;
    }

    static CallableMessageConsumer callableMessageConsumer(CallableMessageConsumer m) {
        return m;
    }

    interface CallableMessageConsumer extends CallableMessage<Void> {
        void accept(Actor self, ActorRef sender);

        @Override
        default Void call(Actor self, ActorRef sender) {
            accept(self, sender);
            return null;
        }
    }
}
