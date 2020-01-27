package csl.actor.msgassoc;

import csl.actor.*;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public abstract class ActorAggregationReplicable extends ActorAggregation implements Cloneable {
    protected State state;

    public ActorAggregationReplicable(ActorSystem system, String name, ActorBehavior behavior) {
        super(system, name, behavior);
        state = new StateDefault();
    }

    public ActorAggregationReplicable(ActorSystem system, ActorBehavior behavior) {
        super(system, behavior);
        state = new StateDefault();
    }

    public ActorAggregationReplicable(ActorSystem system, String name) {
        super(system, name);
        state = new StateDefault();
    }

    public ActorAggregationReplicable(ActorSystem system) {
        super(system);
        state = new StateDefault();
    }

    @Override
    protected void initMailbox() {
        mailbox = new MailboxAggregationReplicable();
    }

    protected void initMailboxForClone() {
        mailbox = getMailboxAsReplicable().create();
    }

    public MailboxAggregationReplicable getMailboxAsReplicable() {
        return (MailboxAggregationReplicable) mailbox;
    }

    public interface State {
        void processMessage(ActorAggregationReplicable self, Message<?> message);

        default ActorRef place(ActorPlacement placement, ActorAggregationReplicable a) {
            if (placement != null) {
                return placement.place(a);
            } else {
                a.getSystem().send(new Message.MessageNone(a));
                return a;
            }
        }
    }

    public ActorPlacement getPlacement() {
        Actor placement = getSystem().resolveActorLocalNamed(
                ActorRefLocalNamed.get(getSystem(), ActorPlacement.PLACEMENT_NAME));
        if (placement instanceof ActorPlacement) {
            return (ActorPlacement) placement;
        } else {
            return null;
        }
    }

    public static class StateDefault implements State {
        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            if (self.getMailboxAsReplicable().isOverThreshold()) {
                becomeRouter(self, message);
            } else {
                self.behavior.process(self, message);
            }
        }

        public void becomeRouter(ActorAggregationReplicable self, Message<?> message) {
            ActorAggregationReplicable a1 = self.createClone();
            ActorAggregationReplicable a2 = self.createClone();
            a1.state = new StateReplica(self);
            a2.state = new StateReplica(self);
            List<Object> splitPoints = self.getMailboxAsReplicable().splitMessageTableIntoReplicas(a1, a2);

            ActorPlacement placement = self.getPlacement();

            StateRouter r = new StateRouter(self,
                    place(placement, a1),
                    place(placement, a2), splitPoints);
            self.state = r;
            r.route(self, message);
        }
    }

    public static class StateRouter implements State {
        protected List<MailboxAggregationReplicable.SplitTreeRoot> splits;
        protected Random random = new Random();

        public StateRouter(ActorAggregationReplicable self, ActorRef a1, ActorRef a2, List<Object> splitPoints) {
            MailboxAggregationReplicable mailbox = self.getMailboxAsReplicable();
            splits = mailbox.createSplits(a1, a2, random, splitPoints);
        }

        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            if (message.getData() instanceof RequestUpdateSplits) {
                updatePoints(self, (RequestUpdateSplits) message.getData());
            } else {
                route(self, message);
            }
        }

        public void updatePoints(ActorAggregationReplicable self, RequestUpdateSplits request) {
            List<Object> splitPoints = request.getNewSplitPoints();
            ActorRef left = request.getLeft();
            ActorRef right = request.getRight();
            for (int i = 0, size = splits.size(); i < size; ++i) {
                splits.get(i).updatePoint(splitPoints.get(i), left, right);
            }
        }

        public void route(ActorAggregationReplicable self, Message<?> message) {
            MailboxAggregation.HistogramSelection target = self.getMailboxAsReplicable().selectTable(message.getData());
            if (target == null) {
                splits.get(random.nextInt(splits.size())).sendNonKey(message);
            } else {
                splits.get(target.entryId).send(message, target.position);
            }
        }

        /** @return implementation field getter */
        public Random getRandom() {
            return random;
        }

        /** @return implementation field getter */
        public List<MailboxAggregationReplicable.SplitTreeRoot> getSplits() {
            return splits;
        }
    }

    public static class StateReplica implements State {
        protected ActorRef router;

        public StateReplica(ActorRef router) {
            this.router = router;
        }

        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            self.behavior.process(self, message);

            if (self.getMailboxAsReplicable().isOverThreshold()) {
                createReplica(self);
            }
        }

        public ActorRef getRouter() {
            return router;
        }

        public void createReplica(ActorAggregationReplicable self) {
            ActorAggregationReplicable r1 = self.createClone();
            ActorAggregationReplicable r2 = self.createClone();
            r1.state = new StateReplica(router);
            r2.state = new StateReplica(router);
            List<Object> splitPoints = self.getMailboxAsReplicable().splitMessageTableIntoReplicas(r1, r2);

            ActorPlacement placement = self.getPlacement();

            self.state = new StateRouterTemporary(self,
                    place(placement, r1),
                    place(placement, r2), router, splitPoints);
        }
    }

    public static class StateRouterTemporary extends StateRouter {
        protected ActorRef router;
        protected ActorRef left;
        protected ActorRef right;
        protected boolean requested;
        public StateRouterTemporary(ActorAggregationReplicable self, ActorRef a1, ActorRef a2, ActorRef router, List<Object> splitPoints) {
            super(self, a1, a2, splitPoints);
            this.router = router;
            this.left = a1;
            this.right = a2;
            requested = false;
        }

        @Override
        public void processMessage(ActorAggregationReplicable self, Message<?> message) {
            super.processMessage(self, message);
            processFurtherMessages(self);

            if (!requested && self.getMailboxAsReplicable().isEmpty()) {
                router.tell(new RequestUpdateSplits(toSplitPoints(), left, right), self);
                requested = true;
            }
        }

        protected void processFurtherMessages(ActorAggregationReplicable self) {
            //here we can manually process further messages on self for quickly delivering messages
            MailboxAggregationReplicable mailbox = self.getMailboxAsReplicable();
            int processLimit = Math.max(10, mailbox.getThreshold() / 10);
            for (int i = 0; !mailbox.isEmpty() && i < processLimit; ++i) {
                super.processMessage(self, mailbox.poll());
            }
        }

        protected List<Object> toSplitPoints() {
            return splits.stream()
                    .map(MailboxAggregationReplicable.SplitTreeRoot::getSplitPoint)
                    .collect(Collectors.toList());
        }

        /** @return implementation field getter */
        public boolean isRequested() {
            return requested;
        }

        /** @return implementation field getter */
        public ActorRef getRouter() {
            return router;
        }

        /** @return implementation field getter */
        public ActorRef getLeft() {
            return left;
        }

        /** @return implementation field getter */
        public ActorRef getRight() {
            return right;
        }
    }

    @Override
    protected void processMessage(Message<?> message) {
        if (isNoRoutingMessage(message)) {
            super.processMessage(message);
        } else {
            state.processMessage(this, message);
        }
    }

    protected boolean isNoRoutingMessage(Message<?> message) {
        return message.getData() instanceof NoRouting ||
                (message.getData() instanceof CallableMessage<?> &&
                        !(message.getData() instanceof Routing));
    }

    public interface NoRouting { }
    public interface Routing { }

    public interface CallableMessageRouting<T> extends CallableMessage<T>, Routing { }

    public static <T> CallableMessageRouting<T> callableRouting(CallableMessageRouting<T> t) {
        return t;
    }

    public ActorAggregationReplicable createClone() {
        try {
            ActorAggregationReplicable a = (ActorAggregationReplicable) super.clone();
            //if the actor has the name, it copies the reference to the name,
            // but it does not register the actor
            a.processLock = new AtomicBoolean(false);
            a.initMailboxForClone();
            a.behavior = a.initBehavior(); //recreate behavior with initMessageTable by ActorBehaviorBuilderKeyValue
            a.initClone(this);
            return a;
        } catch (CloneNotSupportedException ce) {
            throw new RuntimeException(ce);
        }
    }

    protected void initClone(ActorAggregationReplicable original) { }

    public State getState() {
        return state;
    }

    public static class RequestUpdateSplits implements Serializable {
        protected List<Object> newSplitPoints;
        protected ActorRef left;
        protected ActorRef right;

        public RequestUpdateSplits(List<Object> newSplitPoints, ActorRef left, ActorRef right) {
            this.newSplitPoints = newSplitPoints;
            this.left = left;
            this.right = right;
        }

        public List<Object> getNewSplitPoints() {
            return newSplitPoints;
        }

        public ActorRef getLeft() {
            return left;
        }

        public ActorRef getRight() {
            return right;
        }
    }

    public static class PlacemenActorReplicable extends ActorPlacement.PlacemenActor {
        public PlacemenActorReplicable(ActorSystem system, String name) {
            super(system, name);
        }

        public PlacemenActorReplicable(ActorSystem system) {
            super(system);
        }

        @Override
        protected PlacementStrategy initStrategy() {
            return new PlacementStrategyRoundRobin();
        }

        @Override
        public Serializable toSerializable(Actor a, long num) {
            if (a instanceof ActorAggregationReplicable) {
                return ((ActorAggregationReplicable) a).toSerializable(num);
            } else {
                return null;
            }
        }

        @Override
        public Actor fromSerializable(Serializable s, long num) {
            if (s instanceof ActorReplicableSerializableState) {
                return ActorAggregationReplicable.fromSerializable(getSystem(), (ActorReplicableSerializableState) s, num);
            } else {
                return null;
            }
        }

        @Override
        protected ActorRef placeLocal(Actor a) {
            a.getSystem().send(new Message.MessageNone(a));
            return a;
        }
    }

    public ActorReplicableSerializableState toSerializable(long num) {
        ActorReplicableSerializableState state = newSerializableState();
        state.actorType = getClass();
        state.name = String.format("%s#%d", getName(), num);

        State s = getState();
        if (s instanceof StateReplica) { //a replica always has StateReplica
            state.router = ((StateReplica) s).getRouter();
        }

        MailboxAggregationReplicable r = getMailboxAsReplicable();
        r.serializeTo(state);
        return state;
    }

    protected ActorReplicableSerializableState newSerializableState() {
        return new ActorReplicableSerializableState();
    }

    public static ActorAggregationReplicable fromSerializable(ActorSystem system, ActorReplicableSerializableState state, long num) {
        try {
            ActorAggregationReplicable r = state.actorType.getConstructor(ActorSystem.class, String.class)
                .newInstance(system, String.format("%s_%d", state.name, num));
            r.state = new StateReplica(state.router);
            r.getMailboxAsReplicable().deserializeFrom(state);
            system.send(new Message.MessageNone(r));
            return r;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public static class ActorReplicableSerializableState implements Serializable {
        public Class<? extends ActorAggregationReplicable> actorType;
        public String name;
        public ActorRef router;
        public Message<?>[] messages;
        public List<KeyHistograms.HistogramTree> tables;
        public int threshold;
    }
}
