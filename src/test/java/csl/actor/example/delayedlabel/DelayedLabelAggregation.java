package csl.actor.example.delayedlabel;

import csl.actor.*;
import csl.actor.cluster.ResponsiveCalls;
import csl.actor.keyaggregate.*;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class DelayedLabelAggregation extends DelayedLabelManual {
    public static void main(String[] args) {
        new DelayedLabelAggregation().run(args);
    }

    @Override
    public ActorRef learnerActor(ActorSystem system, ActorSystem.SystemLogger out, ActorRef resultActor) {
        return new LernerActorAggregation(system, out, resultActor, config.instances);
    }

    public static class LernerActorAggregation extends ActorKeyAggregation {
        LearnerAggregationSupport support;

        public LernerActorAggregation(ActorSystem system, String name, ActorSystem.SystemLogger out, ActorRef resultActor, int numInstances) {
            super(system, name);
            support = new LearnerAggregationSupport(this, out, resultActor, numInstances);
            setAsUnit();
        }

        public LernerActorAggregation(ActorSystem system, ActorSystem.SystemLogger out, ActorRef resultActor, int numInstances) {
            super(system);
            support = new LearnerAggregationSupport(this, out, resultActor, numInstances);
            setAsUnit();
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
            support.finish(f);
        }

        @Override
        protected void processMessage(Message<?> message) {
            support.processMessageBefore(message);
            super.processMessage(message);
        }
    }

    public static class LearnerAggregationSupport {
        public LearnerModel model;
        public int numInstances;
        public boolean debug = System.getProperty("debug", "").equals("true");
        public ActorSystem.SystemLogger out;

        public long count = 0;
        public static AtomicLong pruneCount = new AtomicLong();

        public ActorKeyAggregation self;
        public ActorKeyAggregation root;

        public LearnerAggregationSupport(ActorKeyAggregation self, ActorSystem.SystemLogger out, ActorRef resultActor, int numInstances) {
            this.self = self;
            this.root = self;
            model = new LearnerModel(resultActor);
            this.numInstances = numInstances;
            this.out = out;
        }

        public LearnerAggregationSupport createClone(ActorKeyAggregation self) {
            LearnerAggregationSupport l = new LearnerAggregationSupport(self, out, model.resultActor, numInstances);
            l.model.model = new HashMap<>(model.model);
            l.model.numSamples = model.numSamples;
            l.root = root;
            return l;
        }

        public void train(FeatureInstance f, LabelInstance l) {
            model.train(new DataInstance(f.getId(), f.getVector(), l.getLabel()), self);
        }

        public void processMessageBefore(Message<?> message) {
            pruneCount.addAndGet(self.getMailboxAsKeyAggregation().prune(32, 0.5));
            process();
        }

        public void process() {
            ++count;
            if (debug && ((count % (numInstances / 10)) == 0 || count == numInstances)) {
                save(Long.toString(count), false);
            }
        }

        public void save(String count, boolean finish) {
            File dir = new File("target/delayed-debug");
            if (!dir.exists()) {
                dir.mkdirs();
            }
            String sn = self.getClass().getSimpleName();
            File file = new File(dir, String.format("delayed-%s-%s.dot", sn, count));
            ResponsiveCalls.sendTaskConsumer(self.getSystem(), root, (self, ref) -> {
                if (new ActorToGraph(self.getSystem(), file, self).save(self).finish()) {
                    if (finish) {
                        System.exit(0);
                    }
                } else {
                    if (finish) {
                        ActorToGraph.schedule(50000, () -> {
                            System.exit(0);
                        });
                    }
                }
            });
        }

        public void finish(Finish f) {
            KeyHistograms.HistogramTree tree = self.getMailboxAsKeyAggregation().getHistogram(0);
            out.log(String.format("#prune-count: %,d : leaf=%,d, non-zero-leaf=%,d : %04f",
                    pruneCount.get(), tree.getLeafSize(), tree.getLeafSizeNonZero(), tree.getLeafSizeNonZeroRate()));
            out.log(String.format("#debug-free-memory: %,d bytes",
                    Runtime.getRuntime().freeMemory()));
            save("finish-" + f.numInstances, true);
        }
    }
}
