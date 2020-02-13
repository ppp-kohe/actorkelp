package csl.actor;

import java.io.Serializable;

@FunctionalInterface
public interface CallableMessage<ActorType extends Actor,RetType> extends Serializable {
    RetType call(ActorType self, ActorRef sender);


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

    static <ActorType extends Actor,RetType> CallableMessage<ActorType,RetType> callableMessage(CallableMessage<ActorType,RetType> m) {
        return m;
    }

    static <ActorType extends Actor> CallableMessageConsumer<ActorType> callableMessageConsumer(CallableMessageConsumer<ActorType> m) {
        return m;
    }

    interface CallableMessageConsumer<ActorType extends Actor> extends CallableMessage<ActorType,Void> {
        void accept(ActorType self, ActorRef sender);

        @Override
        default Void call(ActorType self, ActorRef sender) {
            accept(self, sender);
            return null;
        }
    }
}
