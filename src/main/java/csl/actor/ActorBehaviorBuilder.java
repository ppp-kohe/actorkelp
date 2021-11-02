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
        public <DataType> ActorBehavior get(Class<DataType> dataType, Consumer<DataType> handler) {
            return new ActorBehaviorMatch<>(dataType, handler);
        }
        public <DataType> ActorBehavior getWithSender(Class<DataType> dataType, BiConsumer<DataType, ActorRef> handler) {
            return new ActorBehaviorMatchWithSender<>(dataType, handler);
        }

        public ActorBehavior getAny(Consumer<Object> handler) {
            return new ActorBehaviorAny(handler);
        }

        public ActorBehavior getOr(ActorBehavior l, ActorBehavior r) {
            return new ActorBehaviorOr(l, r);
        }

        public <ActorType extends Actor,DataType extends CallableMessage.CallablePacket<ActorType,?>> ActorBehavior getCallable(Class<DataType> type) {
            return new ActorBehaviorCallable<>(type);
        }

        public ActorBehavior getBundle() {
            return new ActorBehaviorBundle();
        }
    }

    public <DataType> ActorBehaviorBuilder match(Class<DataType> dataType, Consumer<DataType> handler) {
        return with(matchFactory.get(dataType, handler));
    }

    public <DataType> ActorBehaviorBuilder matchWithSender(Class<DataType> dataType, BiConsumer<DataType, ActorRef> handler) {
        return with(matchFactory.getWithSender(dataType, handler));
    }

    public ActorBehaviorBuilder matchAny(Consumer<Object> handler) {
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
     * @return the behavior with appending {@link ActorBehaviorCallable}for {@link CallableMessage}
     */
    @SuppressWarnings("unchecked")
    public ActorBehavior build() {
        return with(matchFactory.getCallable(CallableMessage.CallablePacket.class))
                .with(matchFactory.getBundle())
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
        protected Consumer<DataType> handler;

        public ActorBehaviorMatch(Class<DataType> dataType, Consumer<DataType> handler) {
            this.dataType = dataType;
            this.handler = handler;
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            Object d = message.getData();
            if (dataType.isInstance(d)) {
                handler.accept(dataType.cast(d));
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
        public Consumer<DataType> getHandler() {
            return handler;
        }
    }

    public static class ActorBehaviorMatchWithSender<DataType> implements ActorBehavior {
        protected Class<DataType> dataType;
        protected BiConsumer<DataType, ActorRef> handler;

        public ActorBehaviorMatchWithSender(Class<DataType> dataType, BiConsumer<DataType, ActorRef> handler) {
            this.dataType = dataType;
            this.handler = handler;
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            Object d = message.getData();
            Object data;
            if (d instanceof Message.MessageDataPacket &&
                    dataType.isInstance(data = ((Message.MessageDataPacket<?>) d).getData())) {
                handler.accept(dataType.cast(data), ((Message.MessageDataPacket<?>) d).getSender());
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
        protected Consumer<Object> handler;

        public ActorBehaviorAny(Consumer<Object> handler) {
            this.handler = handler;
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            handler.accept(message.getData());
            return true;
        }

        /** @return implementation field getter */
        public Consumer<Object> getHandler() {
            return handler;
        }
    }

    //////

    /**
     * checks the message type of {@link CallableMessage.CallablePacket} and calls the {@link CallableMessage#call(Actor)}
     * <ul>
     *     <li>regular returning of the call becomes a {@link csl.actor.CallableMessage.CallableResponse}</li>
     *     <li>if the call caused an exception,  it becomes {@link CallableMessage.CallableFailure} </li>
     *     <li>the response will be sent back to the sender of {@link CallableMessage.CallablePacket} (if non-null)</li>
     * </ul>
     * Also the handler processes responses of {@link CallableMessage.CallableResponseVoid} and
     *    {@link CallableMessage.CallableResponse}({@link CallableMessage.CallableResponseVoid}):
     *     for ignoring void responses
     * @param <ActorType> the target actor type
     * @param <DataType> the {@link CallableMessage}
     */
    public static class ActorBehaviorCallable<ActorType extends Actor,DataType extends CallableMessage.CallablePacket<ActorType,?>> implements ActorBehavior {
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
                ActorRef sender = null;
                try {
                    CallableMessage.CallablePacket<ActorType,?> pack = dataType.cast(d);
                    sender = pack.sender;
                    res = new CallableMessage.CallableResponse<>(pack.getData().call((ActorType) self));
                } catch (Throwable ex) {
                    res = new CallableMessage.CallableFailure(ex);
                }
                if (sender != null) { //send res even if null
                    sender.tell(res);
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
    public static class ActorBehaviorBundle implements ActorBehavior {
        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            if (message instanceof MessageBundle<?>) {
                MessageBundle.processMessageBundle(self, (MessageBundle<Object>) message);
                return true;
            } else {
                return false;
            }
        }
    }
}
