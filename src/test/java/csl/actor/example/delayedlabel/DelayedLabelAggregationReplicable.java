package csl.actor.example.delayedlabel;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.msgassoc.ActorAggregationReplicable;
import csl.actor.msgassoc.ActorVisitor;
import csl.actor.msgassoc.Config;
import csl.actor.msgassoc.ResponsiveCalls;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DelayedLabelAggregationReplicable extends DelayedLabelManual {
    public static void main(String[] args) {
        new DelayedLabelAggregationReplicable().run(args);
    }

    @Override
    public ActorRef learnerActor(ActorSystem system, PrintWriter out, ActorRef resultActor) {
        root = new LearnerActorAggregationReplicable(system, out, resultActor, config);
        return root;
    }

    @Override
    public ResultActor resultActor(ActorSystem system, PrintWriter out, Instant startTime) {
        return new ResultActorAggregationReplicable(system, out, startTime, config.instances);
    }

    static LearnerActorAggregationReplicable root;

    static List<LearnerActorAggregationReplicable> processing = new ArrayList<>();

    static class ResultActorAggregationReplicable extends ResultActor {
        boolean printed;
        public ResultActorAggregationReplicable(ActorSystem system, PrintWriter out, Instant startTime, int numInstances) {
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
                    ActorVisitor.tell((ActorAggregationReplicable) a, (v, snd) -> v.printStatus(), null);
                    return null;
                });
                printed = true;
            }
        }
    }

    public static class LearnerState extends ActorAggregationReplicable.ActorReplicableSerializableState {
        public ActorRef resultActor;

        @Override
        protected ActorAggregationReplicable create(ActorSystem system, String name, Config config) throws Exception {
            return new LearnerActorAggregationReplicable(system, name, config, resultActor);
        }
    }

    public static class LearnerActorAggregationReplicable extends ActorAggregationReplicable {
        DelayedLabelAggregation.LearnerAggregationSupport support;

        public LearnerActorAggregationReplicable(ActorSystem system, String name, Config config, ActorRef result) {
            super(system, name, config);
            support = new DelayedLabelAggregation.LearnerAggregationSupport(this, config.getLogOut(), result,
                    ((DelayedLabelConfig) config).instances);
        }

        public LearnerActorAggregationReplicable(ActorSystem system, String name, PrintWriter out, ActorRef resultActor,
                                                 DelayedLabelConfig config) {
            super(system, name, config);
            support = new DelayedLabelAggregation.LearnerAggregationSupport(this, out, resultActor, config.instances);
        }

        public LearnerActorAggregationReplicable(ActorSystem system, PrintWriter out, ActorRef resultActor,
                                                 DelayedLabelConfig config) {
            this(system, "learner", out, resultActor, config);
        }

        @Override
        protected ActorReplicableSerializableState newSerializableState() {
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
        protected void initClone(ActorAggregationReplicable original) {
            log("clone");
            rec = new AtomicInteger();
            support = support.createClone(this);
        }

        @Override
        protected void initMerged(ActorAggregationReplicable m) {
            log("merge");
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
        public String toStringContents() {
            return super.toStringContents() + String.format(", trained=%,d", support.model.numSamples);
        }
    }


}
