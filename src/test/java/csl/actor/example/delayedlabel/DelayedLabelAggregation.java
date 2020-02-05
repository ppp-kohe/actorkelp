package csl.actor.example.delayedlabel;

import csl.actor.*;
import csl.actor.msgassoc.*;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class DelayedLabelAggregation extends DelayedLabelManual {
    public static void main(String[] args) {
        new DelayedLabelAggregation().run(args);
    }

    @Override
    public ActorRef learnerActor(ActorSystem system, PrintWriter out, ActorRef resultActor) {
        return new LernerActorAggregation(system, out, resultActor, config.instances);
    }

    public static class LernerActorAggregation extends ActorAggregation {
        LearnerAggregationSupport support;

        public LernerActorAggregation(ActorSystem system, String name, PrintWriter out, ActorRef resultActor, int numInstances) {
            super(system, name);
            support = new LearnerAggregationSupport(this, out, resultActor, numInstances);
        }

        public LernerActorAggregation(ActorSystem system, PrintWriter out, ActorRef resultActor, int numInstances) {
            super(system);
            support = new LearnerAggregationSupport(this, out, resultActor, numInstances);
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
        public PrintWriter out;

        public long count = 0;
        public static AtomicLong pruneCount = new AtomicLong();

        public ActorAggregation self;
        public ActorAggregation root;

        public LearnerAggregationSupport(ActorAggregation self, PrintWriter out, ActorRef resultActor, int numInstances) {
            this.self = self;
            this.root = self;
            model = new LearnerModel(resultActor);
            this.numInstances = numInstances;
            this.out = out;
        }

        public LearnerAggregationSupport createClone(ActorAggregation self) {
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
            pruneCount.addAndGet(self.getMailboxAsAggregation().prune(32, 0.5));
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
            ResponsiveCalls.send(self.getSystem(), root, (self, ref) -> {
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
                return null;
            }, (t) -> {});
        }

        public void finish(Finish f) {
            KeyHistograms.HistogramTree tree = self.getMailboxAsAggregation().getTable(0);
            out.println(String.format("#prune-count: %,d : leaf=%,d, non-zero-leaf=%,d : %04f",
                    pruneCount.get(), tree.getLeafSize(), tree.getLeafSizeNonZero(), tree.getLeafSizeNonZeroRate()));
            out.println(String.format("#debug-free-memory: %,d bytes",
                    Runtime.getRuntime().freeMemory()));
            save("finish-" + f.numInstances, true);
        }
    }
}
