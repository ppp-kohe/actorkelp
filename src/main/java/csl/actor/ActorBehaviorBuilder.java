package csl.actor;

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
        return behavior;
    }

    public static ActorBehaviorNothing BEHAVIOR_NOTHING = new ActorBehaviorNothing();

    public static class ActorBehaviorNothing implements ActorBehavior {
        @Override
        public boolean process(Message<?> message) {
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
        public boolean process(Message<?> message) {
            if (left.process(message)) {
                return true;
            } else {
                return right.process(message);
            }
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
        public boolean process(Message<?> message) {
            Object d = message.getData();
            if (dataType.isInstance(d)) {
                handler.accept(dataType.cast(d), message.getSender());
                return true;
            } else {
                return false;
            }
        }
    }

    public static class ActorBehaviorAny implements ActorBehavior {
        protected BiConsumer<Object,ActorRef> handler;

        public ActorBehaviorAny(BiConsumer<Object, ActorRef> handler) {
            this.handler = handler;
        }

        @Override
        public boolean process(Message<?> message) {
            handler.accept(message.getData(), message.getSender());
            return true;
        }
    }
}
