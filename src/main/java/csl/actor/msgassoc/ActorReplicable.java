package csl.actor.msgassoc;

import csl.actor.*;

import java.io.Serializable;
import java.util.Map;
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
        return new ActorBehaviorBuilderKeyValue(ls -> getMailboxAsReplicable().initTable(ls));
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
            self.getMailboxAsReplicable().splitMessageTableToReplicas(a1, a2);

            a1.tell(message.getData(), self); //TODO pass self as sender. Is this OK?

            self.state = new StateRouter(self);
        }
    }

    public static class StateRouter implements State {
        protected Object dividingPoints;
        protected Map<Class<?>, Function<?,?>> classToFunction;

        public StateRouter(ActorReplicable self) {
            MailboxReplicable mailbox = self.getMailboxAsReplicable();
            classToFunction = mailbox.createClassFunctionMap();
            dividingPoints = mailbox.createDividingPoints();
        }

        @Override
        public void processMessage(ActorReplicable self, Message<?> message) {
            if (message.getData() instanceof RequestNewReplica) {
                updatePoints(self, (RequestNewReplica) message.getData());
            } else {
                route(message);
            }
        }

        public void updatePoints(ActorReplicable self, RequestNewReplica request) {
            ActorReplicable replica = self.createClone();
            replica.state = new StateReplica(self);
            //TODO dividingPoints

        }

        public void route(Message<?> message) {
            Class<?> messageClass = message.getData().getClass();
            Function<?,?> func = classToFunction.get(messageClass);
            if (func == null) {
                //TODO dividingPoints sendRandom(messageClass)
            } else {
                //TODO dividingPoints.send(message, func)
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
            router.tell(new RequestNewReplica(), self);
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

    }
}
