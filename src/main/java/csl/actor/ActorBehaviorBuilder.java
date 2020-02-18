package csl.actor;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ActorBehaviorBuilder {
    protected ActorBehavior behavior = BEHAVIOR_NOTHING;

    protected MatchFactory matchFactory = new MatchFactory();

    public ActorBehaviorBuilder matchFactory(MatchFactory matchFactory) {
        this.matchFactory = matchFactory;
        return this;
    }

    public static class MatchFactory {
        public <DataType> ActorBehavior get(Class<DataType> dataType, BiConsumer<DataType, ActorRef> handler) {
            return new ActorBehaviorMatch<>(dataType, handler);
        }
    }

    public <DataType> ActorBehaviorBuilder match(Class<DataType> dataType, Consumer<DataType> handler) {
        return matchWithSender(dataType, (data, sender) -> handler.accept(data));
    }

    public <DataType> ActorBehaviorBuilder matchWithSender(Class<DataType> dataType, BiConsumer<DataType, ActorRef> handler) {
        return with(matchFactory.get(dataType, handler));
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

    public static class ActorBehaviorCallable<ActorType extends Actor,DataType extends CallableMessage<ActorType,?>> implements ActorBehavior {
        protected Class<DataType> dataType;

        public ActorBehaviorCallable(Class<DataType> dataType) {
            this.dataType = dataType;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object d = message.getData();
            if (dataType.isInstance(d)) {
                Object res;
                ActorRef sender = message.getSender();
                try {
                    res = dataType.cast(d).call((ActorType) self, sender);
                } catch (Throwable ex) {
                    res = new CallableMessage.CallableFailure(ex);
                }
                if (sender != null) { //send res even if null
                    sender.tell(res, self);
                }
                return true;
            } else if (d instanceof CallableMessage.CallableResponseVoid) { //the guard for Void response
                //nothing
                return true;
            }
            return false;
        }
    }


}
