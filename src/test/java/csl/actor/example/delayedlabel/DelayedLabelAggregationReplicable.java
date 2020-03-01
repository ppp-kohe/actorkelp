package csl.actor.example.delayedlabel;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.keyaggregate.ActorKeyAggregation;
import csl.actor.keyaggregate.KeyAggregationVisitor;
import csl.actor.keyaggregate.Config;
import csl.actor.cluster.ResponsiveCalls;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class DelayedLabelAggregationReplicable extends DelayedLabelManual {
    public static void main(String[] args) {
        new DelayedLabelAggregationReplicable().run(args);
    }

    @Override
    public ActorRef learnerActor(ActorSystem system, Consumer<String> out, ActorRef resultActor) {
        root = new LearnerActorAggregationReplicable(system, out, resultActor, config);
        return root;
    }

    @Override
    public ResultActor resultActor(ActorSystem system, Consumer<String> out, Instant startTime) {
        return new ResultActorAggregationReplicable(system, out, startTime, config.instances);
    }

    static LearnerActorAggregationReplicable root;

    static class ResultActorAggregationReplicable extends ResultActor {
        boolean printed;
        public ResultActorAggregationReplicable(ActorSystem system, Consumer<String> out, Instant startTime, int numInstances) {
            super(system, out, startTime, numInstances);
        }

        @Override
        public void receive(int next, ActorRef sender) {
            super.receive(next, sender);
            if (learner != null) {
                root.support.process();
            }
            if (!printed && (numInstances * 0.9) < this.finishedInstances) {
                ResponsiveCalls.sendTask(system, root, (a, s) -> {
                    System.err.println("print router");
                    KeyAggregationVisitor.tell((ActorKeyAggregation) a, (v, snd) -> v.printStatus(), null);
                    return null;
                });
                printed = true;
            }
        }
    }

    public static class LearnerState extends ActorKeyAggregation.ActorKeyAggregationSerializable {
        public ActorRef resultActor;

        @Override
        protected ActorKeyAggregation create(ActorSystem system, String name, Config config, ActorKeyAggregation.State state) throws Exception {
            return new LearnerActorAggregationReplicable(system, name, config, resultActor, state);
        }
    }

    public static class LearnerActorAggregationReplicable extends ActorKeyAggregation {
        DelayedLabelAggregation.LearnerAggregationSupport support;

        public LearnerActorAggregationReplicable(ActorSystem system, String name, Config config, ActorRef result, State state) {
            super(system, name, config, state);
            support = new DelayedLabelAggregation.LearnerAggregationSupport(this, config.getLogOut(), result,
                    ((DelayedLabelConfig) config).instances);
        }

        public LearnerActorAggregationReplicable(ActorSystem system, String name, Consumer<String> out, ActorRef resultActor,
                                                 DelayedLabelConfig config, State state) {
            super(system, name, config, state);
            support = new DelayedLabelAggregation.LearnerAggregationSupport(this, out, resultActor, config.instances);
        }

        public LearnerActorAggregationReplicable(ActorSystem system, Consumer<String> out, ActorRef resultActor,
                                                 DelayedLabelConfig config) {
            this(system, "learner", out, resultActor, config, null);
            state = initStateRouter();
        }

        @Override
        protected ActorKeyAggregationSerializable newSerializableState() {
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
                    .match(Finish.class, this::finish)
                    .build();
        }

        public void train(FeatureInstance f, LabelInstance l) {
            support.train(f, l);
        }

        public void finish(Finish f) {
            try {
                Thread.sleep(1000);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            support.finish(f);
        }

        AtomicInteger rec = new AtomicInteger();

        @Override
        protected void initClone(ActorKeyAggregation original) {
            log("clone");
            rec = new AtomicInteger();
            support = support.createClone(this);
        }

        @Override
        protected void initMerged(ActorKeyAggregation m) {
            log("merge");
        }

        @Override
        protected void processMessage(Message<?> message) {
            if (root == this) {
                support.processMessageBefore(message);
            } else {
                DelayedLabelAggregation.LearnerAggregationSupport.pruneCount.addAndGet(getMailboxAsKeyAggregation().prune(32, 0.5));
            }
            super.processMessage(message);
        }

        @Override
        public String toStringContents() {
            return super.toStringContents() + String.format(", trained=%,d", support.model.numSamples);
        }
    }


}
