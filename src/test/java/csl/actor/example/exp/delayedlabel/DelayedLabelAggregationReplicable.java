package csl.actor.example.exp.delayedlabel;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.cluster.PhaseShift;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.KelpVisitor;
import csl.actor.kelp.Config;
import csl.actor.cluster.ResponsiveCalls;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class DelayedLabelAggregationReplicable extends DelayedLabelManual {
    public static void main(String[] args) {
        new DelayedLabelAggregationReplicable().run(args);
    }

    @Override
    public ActorRef learnerActor(ActorSystem system, ActorSystem.SystemLogger out, ActorRef resultActor) {
        root = new LearnerActorAggregationReplicable(system, out, resultActor, config);
        return root;
    }

    @Override
    public ResultActor resultActor(ActorSystem system, ActorSystem.SystemLogger out, Instant startTime) {
        return new ResultActorAggregationReplicable(system, out, startTime, config.instances);
    }

    static LearnerActorAggregationReplicable root;

    static class ResultActorAggregationReplicable extends ResultActor {
        boolean printed;
        public ResultActorAggregationReplicable(ActorSystem system, ActorSystem.SystemLogger out, Instant startTime, int numInstances) {
            super(system, out, startTime, numInstances);
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public void receive(int next, ActorRef sender) {
            super.receive(next, sender);
//            if (learner != null) {
//                root.support.process();
//            }
            if (!printed && (numInstances * 0.9) < this.finishedInstances) {
                ResponsiveCalls.sendTask(system, root, (a) -> {
                    System.err.println("print router");
                    KelpVisitor.tell((ActorKelp) a, (v, snd) -> v.printStatus(), null);
                    return null;
                });
                printed = true;
            }
        }
    }

    public static class LearnerState extends ActorKelp.ActorKelpSerializable {
        public static final long serialVersionUID = 1L;
        public ActorRef resultActor;

        @Override
        protected ActorKelp<?> create(ActorSystem system, String name, Config config, ActorKelp.State state) throws Exception {
            return new LearnerActorAggregationReplicable(system, name, config, resultActor, state);
        }
    }

    public static class LearnerActorAggregationReplicable extends ActorKelp<LearnerActorAggregationReplicable> {
        DelayedLabelAggregation.LearnerAggregationSupport support;

        public LearnerActorAggregationReplicable(ActorSystem system, String name, Config config, ActorRef result, State state) {
            super(system, name, config, state);
            nextStage = result;
            support = new DelayedLabelAggregation.LearnerAggregationSupport(this, config.getLogger(), result,
                    ((DelayedLabelConfig) config).instances);
        }

        public LearnerActorAggregationReplicable(ActorSystem system, String name, ActorSystem.SystemLogger out, ActorRef resultActor,
                                                 DelayedLabelConfig config, State state) {
            super(system, name, config, state);
            nextStage = resultActor;
            support = new DelayedLabelAggregation.LearnerAggregationSupport(this, out, resultActor, config.instances);
        }

        public LearnerActorAggregationReplicable(ActorSystem system, ActorSystem.SystemLogger out, ActorRef resultActor,
                                                 DelayedLabelConfig config) {
            this(system, "learner", out, resultActor, config, null);
            nextStage = resultActor;
            state = initStateRouter();
        }

        @Override
        protected ActorKelpSerializable newSerializableState() {
            LearnerState ls = new LearnerState();
            ls.resultActor = support.model.resultActor;
            return ls;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(FeatureInstance.class, FeatureInstance::getId)
                          .or(LabelInstance.class, LabelInstance::getId)
                    .forEachPair(this::train)
                    .match(PhaseShift.PhaseCompleted.class, this::finish)
                    .build();
        }

        public void train(FeatureInstance f, LabelInstance l) {
            support.train(f, l);
        }

        public void finish(PhaseShift.PhaseCompleted c) {
            support.finish();
            c.accept(this);
        }

        AtomicInteger rec = new AtomicInteger();

        @Override
        protected void initClone(LearnerActorAggregationReplicable original) {
            log("clone");
            rec = new AtomicInteger();
            support = support.createClone(this);
        }

        @Override
        protected void initMerged(LearnerActorAggregationReplicable m) {
            log("merge");
        }

        @Override
        protected void processMessage(Message<?> message) {
            if (root == this) {
                support.processMessageBefore(message);
            } else {
                DelayedLabelAggregation.LearnerAggregationSupport.pruneCount.addAndGet(getMailboxAsKelp().prune(32, 0.5));
            }
            super.processMessage(message);
        }

        @Override
        public String toStringContents() {
            return super.toStringContents() + String.format(", trained=%,d", support.model.numSamples);
        }
    }


}
