package csl.actor.kelp;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.cluster.KryoBuilderCluster;
import csl.actor.kelp.actors.ActorKelpSubProcess;
import csl.actor.kelp.behavior.*;
import csl.actor.kelp.persist.*;
import csl.actor.kelp.shuffle.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorAddressSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class KryoBuilderKelp extends KryoBuilderCluster {

    public static Function<ActorSystem, Kryo> builder() {
        return builder(new Creator(KryoBuilderKelp.class));
    }

    protected void buildRegisterActorRef(Kryo kryo) {
        kryo.addDefaultSerializer(ActorAddress.class, new ActorAddressSerializer());
        KelpStageSerializer ss = new KelpStageSerializer(system);
        kryo.addDefaultSerializer(ActorRef.class, ss); //for sub-types

        kryo.register(ActorRefShuffle.class, ss);
        kryo.register(ActorRefShuffleSingle.class, ss);
    }

    @Override
    public List<Class<?>> getActorClasses() {
        List<Class<?>> cs = new ArrayList<>(super.getActorClasses());

        cs.addAll(Arrays.asList(
                KelpStageGraphActor.CompletedTask.class,
                KelpStageGraphActor.GetClockTask.class,
                KelpStageGraphActor.GetStageStatusOp.class,
                KelpStageGraphActor.GetStageStatusTask.class,
                KelpStageGraphActor.GraphBuilding.class,
                KelpStageGraphActor.StageEndTask.class,
                KelpStageGraphActor.StageStatus.class,
                KelpStageGraphActor.StageStatusKey.class,
                KelpStageGraphActor.StageStatusMulti.class,
                KelpStageGraphActor.WatchTask.class,
                KelpStageGraphActor.StageEndBackTask.class,

                KelpStage.CollectStates.class,
                KelpStage.MergerOperatorConcat.class,

                ConfigKelp.class,

                ActorKelpInternalFactory.DispatcherFactoryDispatcherAll.class,
                ActorKelpInternalFactory.DispatcherFactoryDispatcherShuffle.class,
                ActorKelpInternalFactory.DispatcherFactoryDispatcherRandomOne.class,
                ActorKelpInternalFactory.DispatcherFactoryDispatcherRandomPoisson1.class,

                ActorKelpSerializable.class,

                ActorPlacementKelp.StageGraphAdd.class,
                ActorPlacementKelp.StageGraphRemove.class,
                ActorPlacementKelp.StageGraphGet.class,

                ClusterKelp.ActorStat.class,
                ClusterKelp.HistogramStat.class,
                ClusterKelp.StageStat.class,
                ClusterKelp.CallableMessageNewActorStat.class,
                ClusterKelp.CallableMessageNewStageStat.class,

                ActorKelpFunctions.KeyComparator.class,
                ActorKelpFunctions.KeyComparatorDefault.class,
                ActorKelpFunctions.KeyComparatorOrdered.class,

                ActorKelpFunctions.KeyExtractor.class,
                ActorKelpFunctions.KeyExtractorClass.class,
                ActorKelpFunctions.KeyExtractorFunction.class,
                ActorKelpFunctions.KeyExtractorFunctionIdentity.class,
                ActorKelpFunctions.KeyExtractorList.class,

                ActorKelpFunctions.DispatcherFactory.class,

                ActorKelpMergerFunctions.Mergeable.class,

                //shuffle
                ActorRefShuffle.class,
                ActorRefShuffle.ConnectTask.class,
                ActorRefShuffle.ShuffleEntry.class,
                ActorRefShuffle.ToShuffleTask.class,

                ActorRefShuffleKelp.class,
                ActorRefShuffleSingle.class,

                ActorKelpMerger.MergeTask.class,
                ActorKelpMerger.ToStateTask.class,

                ActorKelpStateSharing.MergerOperator.class,
                ActorKelpStateSharing.SetStateFunction.class,
                ActorKelpStateSharing.SetStateFunctionThrowException.class,
                ActorKelpStateSharing.SetStateTask.class,
                ActorKelpStateSharing.StateSharingRequest.class,
                ActorKelpStateSharing.StateSharingRequestLambda.class,
                ActorKelpStateSharing.ToStateFunction.class,
                ActorKelpStateSharing.ToStateTask.class,

                ActorRefCombinedKelp.class,
                ActorRefCombinedKelp.DispatchUnitMember.class,

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
                KeyHistograms.HistogramLeafCellArray.class,
                KeyHistograms.HistogramLeafCellSerializedEnd.class,
                KeyHistograms.HistogramLeafList.class,
                HistogramTreeNodeLeaf.class,
                KeyHistograms.HistogramNodeLeafMap.class,
                HistogramTreeNodeTable.class,
                HistogramTree.class,

                HistogramLeafCellOnStorage.class,
                HistogramTreeNodeLeafOnStorage.class,
                HistogramTreeNodeTableOnStorage.class,
                KeyHistogramsPersistable.HistogramPersistentOperationType.class,
                HistogramTreePersistable.class,
                KeyHistogramsPersistable.NodeTreeData.class,
                HistogramTreePersistable.PutIndexHistory.class,

                KelpDispatcher.class,
                KelpDispatcher.DispatcherAll.class,
                KelpDispatcher.DispatcherRandomOne.class,
                KelpDispatcher.DispatcherRandomPoisson1.class,
                KelpDispatcher.DispatcherShuffle.class,
                KelpDispatcher.SelectiveDispatcher.class,

                ActorKelpStats.class,
                ActorKelpStats.ActorStats.class,
                ActorKelpStats.ActorKelpMessageHandledStats.class,
                ActorKelpStats.ActorKelpMailboxTreeStats.class,
                ActorKelpStats.ActorKelpProcessingStatsFileSplit.class,
                ActorKelpStats.GetActorStatTask.class,
                ActorKelpStats.ActorKelpStageEndStats.class,

                //actors
                ActorKelpSubProcess.ProcessSource.class,
                ActorKelpSubProcess.ProcessInputType.class,
                ActorKelpSubProcess.ProcessOutputType.class
        ));

        return cs;
    }
}
