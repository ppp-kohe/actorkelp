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

        public ActorBehavior getAny(BiConsumer<Object,ActorRef> handler) {
            return new ActorBehaviorAny(handler);
        }

        public ActorBehavior getOr(ActorBehavior l, ActorBehavior r) {
            return new ActorBehaviorOr(l, r);
        }

        public <ActorType extends Actor,DataType extends CallableMessage<ActorType,?>> ActorBehavior getCallable(Class<DataType> type) {
            return new ActorBehaviorCallable<>(type);
        }
    }

    public <DataType> ActorBehaviorBuilder match(Class<DataType> dataType, Consumer<DataType> handler) {
        return matchWithSender(dataType, (data, sender) -> handler.accept(data));
    }

    public <DataType> ActorBehaviorBuilder matchWithSender(Class<DataType> dataType, BiConsumer<DataType, ActorRef> handler) {
        return with(matchFactory.get(dataType, handler));
    }

    public ActorBehaviorBuilder matchAny(BiConsumer<Object,ActorRef> handler) {
        return with(matchFactory.getAny(handler));
    }

    public ActorBehaviorBuilder with(ActorBehavior behavior) {
        if (this.behavior == null || this.behavior.equals(BEHAVIOR_NOTHING)) {
            this.behavior = behavior;
        } else {
            this.behavior = matchFactory.getOr(this.behavior, behavior);
        }
        return this;
    }

    /**
     * @return the behavior with appending {@link ActorBehaviorCallable} for {@link CallableMessage}
     */
    @SuppressWarnings("unchecked")
    public ActorBehavior build() {
        return with(matchFactory.getCallable(CallableMessage.class))
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

    /**
     * checks the message type and calls the {@link CallableMessage#call(Actor, ActorRef)}
     * <ul>
     *     <li>regular returning of the call becomes a {@link csl.actor.CallableMessage.CallableResponse}</li>
     *     <li>if the call caused an exception,  it becomes {@link CallableMessage.CallableFailure} </li>
     *     <li>the response will be sent back to the sender (if non-null)</li>
     * </ul>
     * Also the handler processes responses of {@link csl.actor.CallableMessage.CallableResponseVoid} and
     *    {@link csl.actor.CallableMessage.CallableResponse}({@link csl.actor.CallableMessage.CallableResponseVoid}):
     *     for ignoring void responses
     * @param <ActorType> the target actor type
     * @param <DataType> the {@link CallableMessage}
     */
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
                    res = new CallableMessage.CallableResponse<>(dataType.cast(d).call((ActorType) self, sender));
                } catch (Throwable ex) {
                    res = new CallableMessage.CallableFailure(ex);
                }
                if (sender != null) { //send res even if null
                    sender.tell(res, self);
                }
                return true;
            } else if (d instanceof CallableMessage.CallableResponseVoid ||
                    (d instanceof CallableMessage.CallableResponse &&
                    ((CallableMessage.CallableResponse<?>) d).getValue() instanceof CallableMessage.CallableResponseVoid)) { //the guard for Void response
                //nothing
                return true;
            }
            return false;
        }
    }


}
