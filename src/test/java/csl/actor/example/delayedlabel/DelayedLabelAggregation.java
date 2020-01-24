package csl.actor.example.delayedlabel;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.msgassoc.ActorAggregation;
import csl.actor.msgassoc.KeyHistograms;

import java.io.File;
import java.io.PrintWriter;

public class DelayedLabelAggregation extends DelayedLabelManual {
    public static void main(String[] args) {
        run(new DelayedLabelAggregation(), args);
    }

    @Override
    public ActorRef learnerActor(ActorSystem system, PrintWriter out, ActorRef resultActor, int numInstances) {
        return new LernerActorAggregation(system, out, resultActor, numInstances);
    }

    public static class LernerActorAggregation extends ActorAggregation {
        LearnerModel model;
        int numInstances;
        boolean debug = System.getProperty("debug", "").equals("true");
        PrintWriter out;

        public LernerActorAggregation(ActorSystem system, String name, PrintWriter out, ActorRef resultActor, int numInstances) {
            super(system, name);
            model = new LearnerModel(resultActor);
            this.numInstances = numInstances;
            this.out = out;
        }

        public LernerActorAggregation(ActorSystem system, PrintWriter out, ActorRef resultActor, int numInstances) {
            super(system);
            model = new LearnerModel(resultActor);
            this.numInstances = numInstances;
            this.out = out;
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
            model.train(new DataInstance(f.getId(), f.getVector(), l.getLabel()), this);
        }

        long count = 0;
        long pruneCount;

        @Override
        protected void processMessage(Message<?> message) {
            ++count;
            pruneCount += getMailboxAsAggregation().prune(32, 0.5);
            if (debug && ((count % (numInstances / 10)) == 0 || count == numInstances)) {
                ActorToGraph g = new ActorToGraph();
                g.save(this);

                File dir = new File("target/delayed-debug");
                if (!dir.exists()) {
                    dir.mkdirs();
                }

                try (PrintWriter out = new PrintWriter(new File(dir, String.format("delayed-agg-%d.dot", count)))) {
                    g.write(out);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            super.processMessage(message);
        }

        public void finish(Finish f) {
            KeyHistograms.HistogramTree tree = getMailboxAsAggregation().getTable(0);
            out.println(String.format("#prune-count: %,d : leaf=%,d, non-zero-leaf=%,d : %04f",
                    pruneCount, tree.getLeafSize(), tree.getLeafSizeNonZero(), tree.getLeafSizeNonZeroRate()));
            System.exit(0);
        }
    }
}
