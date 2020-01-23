package csl.actor.example.delayedlabel;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.msgassoc.ActorAggregation;

import java.io.File;
import java.io.PrintWriter;

public class DelayedLabelAggregation extends DelayedLabelManual {
    public static void main(String[] args) {
        run(new DelayedLabelAggregation(), args);
    }

    @Override
    public ActorRef learnerActor(ActorSystem system, ActorRef resultActor, int numInstances) {
        return new LernerActorAggregation(system, resultActor, numInstances);
    }

    public static class LernerActorAggregation extends ActorAggregation {
        LearnerModel model;
        int numInstances;
        boolean debug = System.getProperty("debug", "").equals("true");

        public LernerActorAggregation(ActorSystem system, String name, ActorRef resultActor, int numInstances) {
            super(system, name);
            model = new LearnerModel(resultActor);
            this.numInstances = numInstances;
        }

        public LernerActorAggregation(ActorSystem system, ActorRef resultActor, int numInstances) {
            super(system);
            model = new LearnerModel(resultActor);
            this.numInstances = numInstances;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(FeatureInstance.class, FeatureInstance::getId)
                          .or(LabelInstance.class, LabelInstance::getId)
                    .collect()
                    .forEachPair(this::train)
                    .build();
        }

        public void train(FeatureInstance f, LabelInstance l) {
            model.train(new DataInstance(f.getId(), f.getVector(), l.getLabel()));
        }

        long count = 0;

        @Override
        protected void processMessage(Message<?> message) {
            ++count;
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
    }
}
