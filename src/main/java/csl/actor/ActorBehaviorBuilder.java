package csl.actor;

import java.io.Serializable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ActorBehaviorBuilder {
    protected ActorBehavior behavior = BEHAVIOR_NOTHING;

    public <DataType> ActorBehaviorBuilder match(Class<DataType> dataType, Consumer<DataType> handler) {
        return with(new ActorBehaviorMatch<>(dataType, (data, sender) -> handler.accept(data)));
    }

    public <DataType> ActorBehaviorBuilder matchWithSender(Class<DataType> dataType, BiConsumer<DataType, ActorRef> handler) {
        return with(new ActorBehaviorMatch<>(dataType, handler));
    }

    public ActorBehaviorBuilder matchAny(BiConsumer<Object,ActorRef> handler) {
        return with(new ActorBehaviorAny(handler));
    }

    public ActorBehaviorBuilder with(ActorBehavior behavior) {
        if (this.behavior == null || this.behavior.equals(BEHAVIOR_NOTHING)) {
            this.behavior = behavior;
        } else {
            this.behavior = new ActorBehaviorOr(this.behavior, behavior);
        }
        return this;
    }

    public ActorBehavior build() {
        return with(new ActorBehaviorCallable<>(CallableMessage.class))
                .buildWithoutDefault();
    }

    public ActorBehavior buildWithoutDefault() {
        return behavior;
    }

    public static ActorBehaviorNothing BEHAVIOR_NOTHING = new ActorBehaviorNothing();

    public static class ActorBehaviorNothing implements ActorBehavior {
        @Override
        public boolean process(Actor self, Message<?> message) {
            return false;
        }
    }

    public static class ActorBehaviorOr implements ActorBehavior {
        protected ActorBehavior left;
        protected ActorBehavior right;

        public ActorBehaviorOr(ActorBehavior left, ActorBehavior right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            if (left.process(self, message)) {
                return true;
            } else {
                return right.process(self, message);
            }
        }

        /** @return implementation field getter */
        public ActorBehavior getLeft() {
            return left;
        }

        /** @return implementation field getter */
        public ActorBehavior getRight() {
            return right;
        }
    }

    public static class ActorBehaviorMatch<DataType> implements ActorBehavior {
        protected Class<DataType> dataType;
        protected BiConsumer<DataType,ActorRef> handler;

        public ActorBehaviorMatch(Class<DataType> dataType, BiConsumer<DataType,ActorRef> handler) {
            this.dataType = dataType;
            this.handler = handler;
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            Object d = message.getData();
            if (dataType.isInstance(d)) {
                handler.accept(dataType.cast(d), message.getSender());
                return true;
            } else {
                return false;
            }
        }

        /** @return implementation field getter */
        public Class<DataType> getDataType() {
            return dataType;
        }

        /** @return implementation field getter */
        public BiConsumer<DataType, ActorRef> getHandler() {
            return handler;
        }
    }

    public static class ActorBehaviorAny implements ActorBehavior {
        protected BiConsumer<Object,ActorRef> handler;

        public ActorBehaviorAny(BiConsumer<Object, ActorRef> handler) {
            this.handler = handler;
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            handler.accept(message.getData(), message.getSender());
            return true;
        }

        /** @return implementation field getter */
        public BiConsumer<Object, ActorRef> getHandler() {
            return handler;
        }
    }

    //////

    public static class ActorBehaviorCallable<DataType extends CallableMessage<?>> implements ActorBehavior {
        protected Class<DataType> dataType;

        public ActorBehaviorCallable(Class<DataType> dataType) {
            this.dataType = dataType;
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            Object d = message.getData();
            if (dataType.isInstance(d)) {
                Object res;
                ActorRef sender = message.getSender();
                try {
                    res = dataType.cast(d).call(self, sender);
                } catch (Throwable ex) {
                    res = new CallableFailure(ex);
                }
                if (sender != null && res != null) {
                    sender.tell(res, self);
                }
                return true;
            }
            return false;
        }
    }

    public interface CallableMessage<T> extends Serializable {
        T call(Actor self, ActorRef sender);
    }

    public static <T> CallableMessage<T> callableMessage(CallableMessage<T> m) {
        return m;
    }

    public static class CallableFailure implements Serializable {
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
}
