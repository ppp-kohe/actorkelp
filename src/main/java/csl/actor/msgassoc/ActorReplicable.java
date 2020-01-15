package csl.actor.msgassoc;

import csl.actor.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public abstract class ActorReplicable extends ActorDefault implements Cloneable {
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
    protected ActorBehaviorBuilderKeyValue behaviorBuilder() {
        return new ActorBehaviorBuilderKeyValue((ls, sel) -> getMailboxAsReplicable().initMessageTable(ls, sel));
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

    @Override
    public boolean processMessageNext() {
        if (getMailboxAsReplicable().processTable()) {
            return true;
        }
        return super.processMessageNext();
    }

    public interface State {
        void processMessage(ActorReplicable self, Message<?> message);
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
            self.getMailboxAsReplicable().splitMessageTableIntoReplicas(a1, a2);

            self.state = new StateRouter(self, a1, a2);
            a1.tell(message.getData(), self); //TODO pass self as sender. Is this OK?
        }
    }

    public static class StateRouter implements State {
        protected List<MailboxReplicable.Split> splits;
        protected Random random = new Random();

        public StateRouter(ActorReplicable self, ActorReplicable a1, ActorReplicable a2) {
            MailboxReplicable mailbox = self.getMailboxAsReplicable();
            splits = mailbox.createSplits(a1, a2);
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
            for (int i = 0, size = splits.size(); i < size; ++i) {
                splits.set(i, splits.get(i).updatePoint(splitPoints.get(i), replica));
            }
        }

        public void route(ActorReplicable self, Message<?> message) {
            int target = self.getMailboxAsReplicable().getHistogramSelector().select(message.getData());
            MailboxReplicable.Split split;
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

        public void requestNewReplica(ActorReplicable self) {
            requested = true;
            router.tell(new RequestNewReplica(self.getMailboxAsReplicable().createSplitPoints(), self), self);
        }
    }

    @Override
    protected void processMessage(Message<?> message) {
        state.processMessage(this, message);
    }

    public ActorReplicable createClone() {
        try {
            ActorReplicable a = (ActorReplicable) super.clone();
            a.processLock = new AtomicBoolean(false);
            //TODO share behavior
            a.initMailboxForClone();
            return a;
        } catch (CloneNotSupportedException ce) {
            throw new RuntimeException(ce);
        }
    }

    public static class RequestNewReplica implements Serializable {
        protected List<Comparable<?>> newSplitPoints;
        protected ActorRef actorRef;

        public RequestNewReplica(List<Comparable<?>> newSplitPoints, ActorRef actorRef) {
            this.newSplitPoints = newSplitPoints;
            this.actorRef = actorRef;
        }

        public List<Comparable<?>> getNewSplitPoints() {
            return newSplitPoints;
        }

        public ActorRef getActorRef() {
            return actorRef;
        }
    }
}
