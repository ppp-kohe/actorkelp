package csl.actor.example.delayedlabel;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.msgassoc.ActorAggregation;

public class DelayedLabelAggregation extends DelayedLabelManual {
    public static void main(String[] args) {
        run(new DelayedLabelAggregation(), args);
    }

    @Override
    public ActorRef learnerActor(ActorSystem system, ActorRef resultActor) {
        return new LernerActorAggregation(system, resultActor);
    }

    public static class LernerActorAggregation extends ActorAggregation {
        LearnerModel model;
        public LernerActorAggregation(ActorSystem system, String name, ActorRef resultActor) {
            super(system, name);
            model = new LearnerModel(resultActor);
        }

        public LernerActorAggregation(ActorSystem system, ActorRef resultActor) {
            super(system);
            model = new LearnerModel(resultActor);
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
    }
}
