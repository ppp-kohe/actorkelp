package csl.actor.cluster;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.ActorSystem;
import csl.actor.persist.MailboxPersistableReplacement;
import csl.actor.persist.PersistentFileManager;
import csl.actor.remote.KryoBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class KryoBuilderCluster extends KryoBuilder {
    public static Function<ActorSystem, Kryo> builder() {
        return builder(new Creator(KryoBuilderCluster.class));
    }

    @Override
    protected void buildRegisterActor(Kryo kryo) {
        buildRegisterPersistentFileManager(kryo);
        super.buildRegisterActor(kryo);
    }

    protected void buildRegisterPersistentFileManager(Kryo kryo) {
        kryo.addDefaultSerializer(PersistentFileManager.class,
                new PersistentFileManager.PersistentFileManagerSerializer(system));
    }

    @Override
    public List<Class<?>> getActorClasses() {
        List<Class<?>> ts = new ArrayList<>(super.getActorClasses());
        ts.addAll(Arrays.asList(
                ActorPlacement.ActorCreationRequest.class,
                ActorPlacement.AddressList.class,
                ActorPlacement.AddressListEntry.class,
                ActorPlacement.CallablePrimaryThreads.class,
                ActorPlacement.LeaveEntry.class,

                ClusterCommands.ClusterUnit.class,
                ClusterCommands.CommandBlock.class,
                ClusterCommands.CommandBlockLineContinue.class,
                ClusterCommands.CommandBlockNamed.class,
                ClusterCommands.CommandBlockRoot.class,
                ClusterCommands.CommandToken.class,
                ClusterCommands.CommandTokenType.class,

                ClusterDeployment.ClusterStats.class,
                ClusterDeployment.ConfigSet.class,
                ClusterDeployment.NetworkStats.class,
                ClusterDeployment.ShutdownTask.class,
                ClusterDeployment.SystemStats.class,

                ConfigDeployment.class,

                MailboxPersistableReplacement.MessageOnStorage.class,

                PersistentFileManager.PersistentFileEnd.class,
                PersistentFileManager.PersistentFileReaderSource.class));
        return ts;
    }
}
