package csl.actor.kelp;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.kelp.actors.ActorKelpFileReader;
import csl.actor.kelp.actors.ActorKelpFileWriter;
import csl.actor.kelp.actors.ActorKelpLambda;
import csl.actor.kelp.actors.ActorKelpSubProcess;
import csl.actor.kelp.behavior.ActorBehaviorBuilderKelp;
import csl.actor.kelp.behavior.KelpDispatcher;
import csl.actor.kelp.behavior.KeyHistograms;
import csl.actor.kelp.behavior.MailboxKelp;
import csl.actor.kelp.persist.KeyHistogramsPersistable;
import csl.actor.kelp.persist.PersistentConditionActor;
import csl.actor.kelp.shuffle.ActorKelpStateSharing;
import csl.actor.kelp.shuffle.ActorRefCombinedKelp;
import csl.actor.kelp.shuffle.ActorRefShuffle;
import csl.actor.kelp.shuffle.ActorRefShuffleKelp;
import csl.actor.persist.MailboxManageable;
import csl.actor.persist.MailboxPersistableIncoming;
import csl.actor.persist.PersistentConditionMailbox;
import csl.actor.persist.PersistentFileManager;
import csl.actor.util.FileSplitter;
import csl.actor.util.Staging;

import java.util.List;
import java.util.Random;
import java.util.function.Function;

/**
 * extension points for {@link ActorKelp}.
 *  the system implementing {@link ActorKelpBuilder#internalFactory()} can override to return a custom factory.
 */
public class ActorKelpInternalFactory {
    public static ActorKelpInternalFactory DEFAULT_FACTORY = new ActorKelpInternalFactory();

    public static ActorKelpInternalFactory createFromConfig(ConfigDeployment conf) {
        try {
            if (conf.internalFactoryType.isEmpty()) {
                return DEFAULT_FACTORY;
            }
            Class<?> type = Class.forName(conf.internalFactoryType);
            return (ActorKelpInternalFactory) type.getConstructor().newInstance();
        } catch (Exception ex) {
            ex.printStackTrace();
            return DEFAULT_FACTORY;
        }
    }

    public PersistentFileManager getPersistentFile(ActorKelp<?> self) {
        return PersistentFileManager.getPersistentFile(self.getSystem(), self.getMailboxPath());
    }

    public PersistentConditionActor initMemoryCondition(ActorKelp<?> self, PersistentFileManager m) {
        if (self.isPersist()) {
            if (self.isPersistRuntimeCondition()) {
                return new PersistentConditionActor.PersistentConditionActorRuntime(self,
                        self.getTotalActorMemoryRate(),
                        self.getMemoryCheckedThreshold(),
                        self.getMemoryPersistThreshold(),
                        self.getMailboxTreeSize(),
                        self.getReduceRuntimeCheckingThreshold(),
                        self.getSystem().getLogger());
            } else {
                return PersistentConditionActor.createSizeLimit(self,
                        m,
                        self.getMailboxOnMemorySize(),
                        self.getReduceRuntimeCheckingThreshold(), self.getReduceRuntimeRemainingBytesToSizeRatio(),
                        new KeyHistogramsPersistable.HistogramTreePersistableConfigKelp(self.getConfig()),
                        self.getSystem().getLogger());
            }
        } else {
            return PersistentConditionActor.createNever(self, self.getSystem().getLogger(),
                    self.getReduceRuntimeCheckingThreshold(), self.getReduceRuntimeRemainingBytesToSizeRatio());
        }
    }

    public MailboxManageable initMailboxDefault(ActorKelp<?> self, PersistentFileManager m) {
        PersistentConditionMailbox condition = self.getMemoryCondition().getConditionMailbox();
        MailboxManageable mbox;
        if (self.isPersist()) {
            mbox = new MailboxPersistableIncoming(m, condition);
        } else {
            mbox = new MailboxManageable.MailboxDefaultManageable(m, condition);
        }
        return mbox;
    }

    public KeyHistograms initTreeFactory(ActorKelp<?> self, PersistentFileManager m) {
        if (self.isPersist()) {
            return new KeyHistogramsPersistable(
                    new KeyHistogramsPersistable.HistogramTreePersistableConfigKelp(self.getConfig()),
                    m,
                    self.getMemoryCondition().getConditionHistogram());
        } else {
            return new KeyHistograms(m);
        }
    }

    public MailboxKelp initMailboxKelp(ActorKelp<?> self, MailboxManageable m, KeyHistograms treeFactory) {
        return new MailboxKelp(self.getMailboxTreeSize(), m, treeFactory);
    }

    public ActorBehaviorBuilderKelp behaviorBuilder(ActorKelp<?> self) {
        return new ActorBehaviorBuilderKelp(self.getMailboxAsKelp()::initMessageEntries, self::initSelectiveDispatchers);
    }

    public ActorRef setNextStage(ActorKelp<?> self, ActorRef prevNextStage, ActorRef nextStage) {
        return ActorRefCombinedKelp.combine(self.getSystem(), Staging.stageNameArray(self.getName(), "next"), prevNextStage, nextStage);
    }

    public void processMessageBefore(ActorKelp<?> self) {}
    public void processMessageAfter(ActorKelp<?> self) {}

    public void processStagingCompleted(ActorKelp<?> self, Object taskKey) {}

    public KelpStageGraphActor stageGraph(ActorKelp<?> self) throws Exception {
        return KelpStageGraphActor.get(self.getSystem(), self);
    }

    public void flush(ActorKelp<?> self) {
        ActorRefShuffle.flush(self.nextStageActor());
    }

    public void close(ActorKelp<?> self) {}

    @SuppressWarnings("unchecked")
    public <SelfType extends ActorKelp<SelfType>> ActorKelpSerializable<SelfType> toSerializable(ActorKelp<?> self, boolean includeMailbox) {
        return new ActorKelpSerializable<>((SelfType) self, includeMailbox);
    }

    public ActorKelpSerializable.InternalStateBuilder getInternalStateBuilder(ActorKelp<?> self) {
        return ActorKelpSerializable.getBuilder(self.getClass());
    }


    public ActorKelpStateSharing.StateSharingActor createSharingActor(ActorKelp<?> self) {
        return new ActorKelpStateSharing.StateSharingActor(self.getSystem(), self.getConfig());
    }

    @SuppressWarnings("unchecked")
    public <SelfType extends ActorKelp<SelfType>> ActorRefShuffleKelp<SelfType> createShuffle(ActorKelp<?> self,
                                                                                                 List<ActorRef> entries,
                                                                                                 List<KelpDispatcher.SelectiveDispatcher> extractorsAndDispatchers, int bufferSize) {
        return new ActorRefShuffleKelp<>(
                self.getSystem(),
                ActorRefShuffle.createDispatchUnits(entries, bufferSize),
                extractorsAndDispatchers,
                bufferSize, (Class<SelfType>) self.getClass(), self.getConfig(), self.getName());
    }

    public KelpDispatcher.DispatchUnit createShuffleEntryEmpty(ActorKelp<?> self) {
        return new ActorKelp.SelfDispatcher(self);
    }
    public ActorKelpFunctions.DispatcherFactory dispatchAll(ActorKelp<?> self) {
        return new DispatcherFactoryDispatcherAll();
    }

    public ActorKelpFunctions.DispatcherFactory dispatchShuffle(ActorKelp<?> self) {
        return new DispatcherFactoryDispatcherShuffle();
    }

    public ActorKelpFunctions.DispatcherFactory dispatchRandomOne(ActorKelp<?> self) {
        return new DispatcherFactoryDispatcherRandomOne(self.createDispatcherRandom());
    }

    public ActorKelpFunctions.DispatcherFactory dispatchRandomPoison1(ActorKelp<?> self) {
        return new DispatcherFactoryDispatcherRandomPoisson1(self.createDispatcherRandom());
    }

    public ActorKelpFunctions.DispatcherFactory dispatchRandomOne(ActorKelp<?> self, Random random) {
        return new DispatcherFactoryDispatcherRandomOne(random);
    }

    public ActorKelpFunctions.DispatcherFactory dispatchRandomPoison1(ActorKelp<?> self, Random random) {
        return new DispatcherFactoryDispatcherRandomPoisson1(random);
    }

    public static class DispatcherFactoryDispatcherAll implements ActorKelpFunctions.DispatcherFactory {
        public static final long serialVersionUID = -1;
        @Override
        public KelpDispatcher create() {
            return new KelpDispatcher.DispatcherAll();
        }
    }
    public static class DispatcherFactoryDispatcherShuffle implements ActorKelpFunctions.DispatcherFactory {
        public static final long serialVersionUID = -1;
        @Override
        public KelpDispatcher create() {
            return new KelpDispatcher.DispatcherShuffle();
        }
    }
    public static class DispatcherFactoryDispatcherRandomOne implements ActorKelpFunctions.DispatcherFactory {
        public static final long serialVersionUID = -1;
        public Random random;

        public DispatcherFactoryDispatcherRandomOne() {}

        public DispatcherFactoryDispatcherRandomOne(Random random) {
            this.random = random;
        }

        @Override
        public KelpDispatcher create() {
            return random == null ?
                    new KelpDispatcher.DispatcherRandomOne() :
                    new KelpDispatcher.DispatcherRandomOne(random);
        }
    }

    public static class DispatcherFactoryDispatcherRandomPoisson1 implements ActorKelpFunctions.DispatcherFactory {
        public static final long serialVersionUID = -1;
        public Random random;

        public DispatcherFactoryDispatcherRandomPoisson1() {}

        public DispatcherFactoryDispatcherRandomPoisson1(Random random) {
            this.random = random;
        }

        @Override
        public KelpDispatcher create() {
            return random == null ?
                    new KelpDispatcher.DispatcherRandomPoisson1() :
                    new KelpDispatcher.DispatcherRandomPoisson1(random);
        }
    }

    /////actor factory

    public ActorKelpLambda actor(ActorSystem system, ConfigKelp config, ActorKelpLambda.ActorBuilder builderFunction) {
        return new ActorKelpLambda(system, config, builderFunction);
    }

    public ActorKelpLambda actor(ActorSystem system, ConfigKelp config, String name, ActorKelpLambda.ActorBuilder builderFunction) {
        return new ActorKelpLambda(system, name, config, builderFunction);
    }

    public ActorKelpFileReader actorReader(ActorSystem system, ConfigKelp config) {
        return new csl.actor.kelp.actors.ActorKelpFileReader(system, config);
    }

    public ActorKelpFileReader actorReader(ActorSystem system, ConfigKelp config, String name) {
        return new ActorKelpFileReader(system, name, config);
    }

    public ActorKelpFileWriter actorWriter(ActorSystem system, ConfigKelp config) {
        return new ActorKelpFileWriter(system, config);
    }

    public ActorKelpFileWriter actorWriter(ActorSystem system, ConfigKelp config, String name) {
        return new ActorKelpFileWriter(system, name, config);
    }

    public ActorKelpSubProcess actorSubProcess(ActorSystem system, ConfigKelp config, Function<ActorKelpSubProcess.ProcessSource, ActorKelpSubProcess.ProcessSource> init) {
        return new ActorKelpSubProcess(system, config, init);
    }

    public ActorKelpSubProcess actorSubProcess(ActorSystem system, ConfigKelp config, String name, Function<ActorKelpSubProcess.ProcessSource, ActorKelpSubProcess.ProcessSource> init) {
        return new ActorKelpSubProcess(system, name, config, init);
    }

    //////////

    public Object getFileSplitInitMessage(Actor self, String path) {
        return new FileSplitter.FileSplit(path);
    }

}