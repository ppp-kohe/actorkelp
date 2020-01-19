package csl.actor.msgassoc;

import csl.actor.*;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ActorReplicable extends ActorAggregation implements Cloneable {
    protected State state;

    public ActorReplicable(ActorSystem system, String name, ActorBehavior behavior) {
        super(system, name, behavior);
        state = new StateDefault();
    }

    public ActorReplicable(ActorSystem system, ActorBehavior behavior) {
        super(system, behavior);
        state = new StateDefault();
    }

    public ActorReplicable(ActorSystem system, String name) {
        super(system, name);
        state = new StateDefault();
    }

    public ActorReplicable(ActorSystem system) {
        super(system);
        state = new StateDefault();
    }

    @Override
    protected void initMailbox() {
        mailbox = new MailboxReplicable();
    }

    protected void initMailboxForClone() {
        mailbox = getMailboxAsReplicable().create();
    }

    public MailboxReplicable getMailboxAsReplicable() {
        return (MailboxReplicable) mailbox;
    }

    public interface State {
        void processMessage(ActorReplicable self, Message<?> message);

        default ActorRef place(ActorPlacement placement, ActorReplicable a) {
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
        public void processMessage(ActorReplicable self, Message<?> message) {
            if (self.getMailboxAsReplicable().isOverThreshold()) {
                becomeRouter(self, message);
            } else {
                self.behavior.process(self, message);
            }
        }

        public void becomeRouter(ActorReplicable self, Message<?> message) {
            ActorReplicable a1 = self.createClone();
            ActorReplicable a2 = self.createClone();
            a1.state = new StateReplica(self);
            a2.state = new StateReplica(self);
            self.getMailboxAsReplicable().splitMessageTableIntoReplicas(a1, a2);

            ActorPlacement placement = self.getPlacement();

            StateRouter r = new StateRouter(self,
                    place(placement, a1),
                    place(placement, a2));
            self.state = r;
            r.route(self, message);
        }
    }

    public static class StateRouter implements State {
        protected List<MailboxReplicable.SplitTreeRoot> splits;
        protected Random random = new Random();

        public StateRouter(ActorReplicable self, ActorRef a1, ActorRef a2) {
            MailboxReplicable mailbox = self.getMailboxAsReplicable();
            splits = mailbox.createSplits(a1, a2, random);
        }

        @Override
        public void processMessage(ActorReplicable self, Message<?> message) {
            if (message.getData() instanceof RequestNewReplica) {
                updatePoints(self, (RequestNewReplica) message.getData());
            } else {
                route(self, message);
            }
        }

        public void updatePoints(ActorReplicable self, RequestNewReplica request) {
            ActorReplicable replica = self.createClone();
            replica.state = new StateReplica(self);
            List<Comparable<?>> splitPoints = request.getNewSplitPoints();

            ActorRef newActor = place(self.getPlacement(), replica);
            for (int i = 0, size = splits.size(); i < size; ++i) {
                splits.get(i).updatePoint(splitPoints.get(i), newActor);
            }
        }

        public void route(ActorReplicable self, Message<?> message) {
            int target = self.getMailboxAsReplicable().getHistogramSelector().select(message.getData());
            if (target == -1) {
                splits.get(random.nextInt(splits.size())).sendNonKey(message);
            } else {
                splits.get(target).send(message);
            }
        }
    }

    public static class StateReplica implements State {
        protected ActorRef router;
        protected boolean requested;

        public StateReplica(ActorRef router) {
            this.router = router;
        }

        @Override
        public void processMessage(ActorReplicable self, Message<?> message) {
            self.behavior.process(self, message);
            if (!requested && self.getMailboxAsReplicable().isOverThreshold()) {
                requestNewReplica(self);
            }
        }

        public ActorRef getRouter() {
            return router;
        }

        public void requestNewReplica(ActorReplicable self) {
            requested = true;
            router.tell(new RequestNewReplica(self.getMailboxAsReplicable().createSplitPoints()), self);
        }
    }

    @Override
    protected void processMessage(Message<?> message) {
        state.processMessage(this, message);
    }

    public ActorReplicable createClone() {
        try {
            ActorReplicable a = (ActorReplicable) super.clone();
            //if the actor has the name, it copies the reference to the name,
            // but it does not register the actor
            a.processLock = new AtomicBoolean(false);
            //share behavior
            a.initMailboxForClone();
            return a;
        } catch (CloneNotSupportedException ce) {
            throw new RuntimeException(ce);
        }
    }

    public State getState() {
        return state;
    }

    public static class RequestNewReplica implements Serializable {
        protected List<Comparable<?>> newSplitPoints;

        public RequestNewReplica(List<Comparable<?>> newSplitPoints) {
            this.newSplitPoints = newSplitPoints;
        }

        public List<Comparable<?>> getNewSplitPoints() {
            return newSplitPoints;
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
        PlacementStrategy initStrategy() {
            return new PlacementStrategyRoundRobin();
        }

        @Override
        public Serializable toSerializable(Actor a, long num) {
            if (a instanceof ActorReplicable) {
                return ((ActorReplicable) a).toSerializable(num);
            } else {
                return null;
            }
        }

        @Override
        public Actor fromSerializable(Serializable s, long num) {
            if (s instanceof ActorReplicableSerializableState) {
                return ActorReplicable.fromSerializable(getSystem(), (ActorReplicableSerializableState) s, num);
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

        MailboxReplicable r = getMailboxAsReplicable();
        r.serializeTo(state);
        return state;
    }

    protected ActorReplicableSerializableState newSerializableState() {
        return new ActorReplicableSerializableState();
    }

    public static ActorReplicable fromSerializable(ActorSystem system, ActorReplicableSerializableState state, long num) {
        try {
            ActorReplicable r = state.actorType.getConstructor(ActorSystem.class, String.class)
                .newInstance(system, String.format("%s_%d", state.name, num));
            r.state = new StateReplica(state.router);
            r.getMailboxAsReplicable().deserializeFrom(state);
            return r;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public static class ActorReplicableSerializableState implements Serializable {
        public Class<? extends ActorReplicable> actorType;
        public String name;
        public ActorRef router;
        public Message<?>[] messages;
        public MailboxReplicable.EntryTable[] entries;
        public int threshold;
    }
}
