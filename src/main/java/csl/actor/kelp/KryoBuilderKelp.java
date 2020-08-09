package csl.actor.kelp;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.cluster.KryoBuilderCluster;
import csl.actor.kelp.behavior.ActorBehaviorKelp;
import csl.actor.kelp.behavior.HistogramEntry;
import csl.actor.kelp.behavior.KeyHistograms;
import csl.actor.kelp.behavior.KeyHistogramsPersistable;
import csl.actor.kelp.shuffle.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class KryoBuilderKelp extends KryoBuilderCluster {

    public static Function<ActorSystem, Kryo> builder() {
        return builder(new Creator(KryoBuilderKelp.class));
    }

    protected void buildRegisterActorRef(Kryo kryo) {
        KelpStageSerializer ss = new KelpStageSerializer(system);
        kryo.addDefaultSerializer(ActorRef.class, ss); //for sub-types

        kryo.register(ActorRefShuffle.class, ss);
        kryo.register(ActorRefShuffleSingle.class, ss);
    }

    @Override
    public List<Class<?>> getActorClasses() {
        List<Class<?>> cs = new ArrayList<>(super.getActorClasses());

        cs.addAll(Arrays.asList(
                ActorRefShuffleKelp.class,
                ActorKelp.MessageBundle.class,

                ActorKelpFunctions.KeyComparator.class,
                ActorKelpFunctions.KeyComparatorDefault.class,
                ActorKelpFunctions.KeyComparatorOrdered.class,

                ActorKelpFunctions.KeyExtractor.class,
                ActorKelpFunctions.KeyExtractorClass.class,
                ActorKelpFunctions.KeyExtractorFunction.class,
                ActorKelpFunctions.KeyExtractorList.class,

                ActorKelpMerger.MergeTask.class,
                ActorKelpMerger.ToStateTask.class,

                ActorKelpSerializable.class,

                ActorRefShuffle.ConnectTask.class,
                ActorRefShuffle.ShuffleEntry.class,
                ActorRefShuffle.ToShuffleTask.class,

                ConfigKelp.class,

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
                KeyHistograms.HistogramTree.class,

                KeyHistogramsPersistable.HistogramLeafCellOnStorage.class,
                KeyHistogramsPersistable.HistogramLeafCellOnStorageFile.class,
                KeyHistogramsPersistable.HistogramLeafListPersistable.class,
                KeyHistogramsPersistable.HistogramNodeLeafOnStorage.class,
                KeyHistogramsPersistable.HistogramNodeTreeOnStorage.class,
                KeyHistogramsPersistable.HistogramPersistentOperationType.class,
                KeyHistogramsPersistable.HistogramTreePersistable.class,
                KeyHistogramsPersistable.NodeTreeData.class,
                KeyHistogramsPersistable.PersistentFileReaderSourceWithSize.class,
                KeyHistogramsPersistable.PutIndexHistory.class

        ));

        return cs;
    }
}
