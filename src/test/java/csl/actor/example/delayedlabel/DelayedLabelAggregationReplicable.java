package csl.actor.example.delayedlabel;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.msgassoc.ActorAggregationReplicable;

import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicInteger;

public class DelayedLabelAggregationReplicable extends DelayedLabelManual {
    public static void main(String[] args) {
        run(new DelayedLabelAggregationReplicable(), args);
    }

    @Override
    public ActorRef learnerActor(ActorSystem system, PrintWriter out, ActorRef resultActor, int numInstances) {
        root = new LernerActorAggregationReplicable(system, out, resultActor, numInstances);
        return root;
    }

    static LernerActorAggregationReplicable root;

    static class LernerActorAggregationReplicable extends ActorAggregationReplicable {
        DelayedLabelAggregation.LearnerAggregationSupport support;

        public LernerActorAggregationReplicable(ActorSystem system, String name, PrintWriter out, ActorRef resultActor, int numInstances) {
            super(system, name);
            support = new DelayedLabelAggregation.LearnerAggregationSupport(this, out, resultActor, numInstances);
        }

        public LernerActorAggregationReplicable(ActorSystem system, PrintWriter out, ActorRef resultActor, int numInstances) {
            super(system);
            support = new DelayedLabelAggregation.LearnerAggregationSupport(this, out, resultActor, numInstances);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(FeatureInstance.class, FeatureInstance::getId)
                    .or(LabelInstance.class, LabelInstance::getId)
                    .collect()
                    .forEachPair(this::train)
                    .match(Finish.class, this::finish)
                    .build();
        }

        public void train(FeatureInstance f, LabelInstance l) {
            support.train(f, l);
        }

        public void finish(Finish f) {
            try {
                Thread.sleep(3000);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            support.finish(f);
        }

        AtomicInteger rec = new AtomicInteger();

        @Override
        protected void initClone(ActorAggregationReplicable original) {
            rec = new AtomicInteger();
            support = support.createClone(this);
        }

        @Override
        public boolean processMessageNext() {
            int n = rec.getAndIncrement();
            if (n > 0) {
                System.err.println(this + " re-entrance process " + n + " " + Thread.currentThread());
            }
            try {
                return super.processMessageNext();
            } catch (Throwable ex) {
                /*
                System.err.println("#error: " + this);
                System.err.println("  completed: " +
                        Integer.toHexString(System.identityHashCode(this.getMailboxAsReplicable().getTable(0).getCompleted())));

                 */
                ex.printStackTrace();
                throw ex;
            } finally {
                rec.decrementAndGet();
            }
        }

        @Override
        protected void processMessage(Message<?> message) {
            if (root == this) {
                support.processMessageBefore(message);
            } else {
                DelayedLabelAggregation.LearnerAggregationSupport.pruneCount.addAndGet(getMailboxAsReplicable().prune(32, 0.5));
            }
            super.processMessage(message);
        }

        @Override
        public ActorAggregationReplicable createClone() {
            ActorAggregationReplicable r = super.createClone();
            /*
            System.err.println("#clone: " + r + " <- " + this);
            System.err.println("   completed  " +
                    Integer.toHexString(System.identityHashCode(r.getMailboxAsReplicable().getTable(0).getCompleted())) + " <- " +
                    Integer.toHexString(System.identityHashCode(this.getMailboxAsReplicable().getTable(0).getCompleted())));

             */
            return r;
        }
    }
}
