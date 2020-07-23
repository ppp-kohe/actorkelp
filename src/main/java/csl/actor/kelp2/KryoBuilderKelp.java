package csl.actor.kelp2;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.cluster.KryoBuilderCluster;
import csl.actor.kelp2.behavior.ActorBehaviorKelp;
import csl.actor.kelp2.behavior.HistogramEntry;
import csl.actor.kelp2.behavior.KeyHistograms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class KryoBuilderKelp extends KryoBuilderCluster {

    public static Function<ActorSystem, Kryo> builder() {
        return builder(KryoBuilderKelp::new);
    }

    protected void buildRegisterActorRef(Kryo kryo) {
        kryo.addDefaultSerializer(ActorRef.class, new ActorRefShuffle.ActorRefShuffleSerializer(system)); //for sub-types
        kryo.addDefaultSerializer(KelpStage.KelpStageRefWrapper.class, new KelpStage.KelpStageRefWrapperSerializer(system));
    }

    @Override
    public List<Class<?>> getActorClasses() {
        List<Class<?>> cs = new ArrayList<>(super.getActorClasses());

        cs.addAll(Arrays.asList(
                ActorKelpSerializable.class,
                ActorKelp.ActorRefShuffleKelp.class,
                ActorKelp.MessageBundle.class,

                ActorKelpFunctions.KeyComparator.class,
                ActorKelpFunctions.KeyComparatorDefault.class,
                ActorKelpFunctions.KeyComparatorOrdered.class,

                ActorKelpFunctions.KeyExtractor.class,
                ActorKelpFunctions.KeyExtractorClass.class,
                ActorKelpFunctions.KeyExtractorFunction.class,
                ActorKelpFunctions.KeyExtractorList.class,

                ActorRefShuffle.ConnectTask.class,
                ActorRefShuffle.ShuffleEntry.class,
                ActorRefShuffle.ToShuffleTask.class,

                ConfigKelp.class,

                KelpStage.KelpStageRefWrapper.class,

                //behavior
                ActorBehaviorKelp.HistogramNodeLeaf1.class,
                ActorBehaviorKelp.HistogramNodeLeaf2.class,
                ActorBehaviorKelp.HistogramNodeLeaf3.class,
                ActorBehaviorKelp.HistogramNodeLeaf4.class,
                ActorBehaviorKelp.HistogramNodeLeafList.class,
                ActorBehaviorKelp.HistogramNodeLeafListReducible.class,
                ActorBehaviorKelp.HistogramNodeLeafListReducibleForStageEnd.class,
                ActorBehaviorKelp.HistogramNodeLeafN.class,

                HistogramEntry.TraversalProcess.class,

                KeyHistograms.HistogramLeafCell.class,
                KeyHistograms.HistogramLeafCellSerializedEnd.class,
                KeyHistograms.HistogramLeafList.class,
                KeyHistograms.HistogramNodeLeaf.class,
                KeyHistograms.HistogramNodeLeafMap.class,
                KeyHistograms.HistogramNodeTree.class,
                KeyHistograms.HistogramTree.class
        ));

        return cs;
    }
}
