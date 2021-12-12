package csl.actor.kelp;

import csl.actor.*;
import csl.actor.cluster.ClusterCommands;
import csl.actor.cluster.ClusterDeployment;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.kelp.behavior.HistogramEntry;
import csl.actor.kelp.behavior.HistogramTree;
import csl.actor.kelp.persist.HistogramTreePersistable;
import csl.actor.kelp.behavior.MailboxKelp;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;
import csl.actor.util.ConfigBase;
import csl.actor.util.ToJson;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <pre>
 *   package your.pack;
 *   public class PrimaryMain {
 *     public static void main(String[] args) throws Exception {
 *         try (ClusterKelp&lt;ConfigKelp&gt; c = ClusterKelp.createAndDeploy()) {
 *             ActorKelpFileReader f = c.actorReader();
 *             KelpStage&lt;MyActor&gt; a = f.connects(new MyActor(c.system(), c.config()));
 *                                     //suppose MyActor is a sub-class of ActorKelp
 *
 *             f.startReading(args[0]).get();
 *         }
 *     }
 *   }
 * </pre>
 *
 * <pre>
 *     java -cp ... csl.actor.kelp.ClusterKelp your.pack.PrimaryMain input-path.txt
 * </pre>
 *
 * @param <ConfigType> customized config type
 */
public class ClusterKelp<ConfigType extends ConfigKelp> extends ClusterDeployment<ConfigType, ActorPlacementKelp<ConfigType>>
        implements ActorKelpBuilder {
    @Override
    public ActorSystemKelp system() {
        return getSystem();
    }

    @Override
    public ConfigType config() {
        return getPrimaryConfig();
    }

    @Override
    public ActorKelpInternalFactory internalFactory() {
        return getSystem().internalFactory();
    }

    public static void main(String[] args) throws Exception {
        ClusterKelp.create()
                .runAsRemoteDriver(args);
    }

    public static void run(String configFile, String mainType, String... args) throws Exception {
        ClusterKelp.create()
                .runAsRemoteDriver(configFile, mainType, Arrays.asList(args));
    }

    public static void run(List<ClusterCommands.ClusterUnit<ConfigKelp>> units, String mainType, String... args) throws Exception {
        ClusterKelp.create()
                .runAsRemoteDriver(units, mainType, Arrays.asList(args));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public ClusterKelp(Class<ConfigType> defaultConfType, Class<ActorPlacementKelp> placeType) {
        super(defaultConfType, (Class<ActorPlacementKelp<ConfigType>>) (Class) placeType);
    }

    /**
     * @return create a cluster with {@link ConfigKelp} as the default config;
     *    if no cluster conf file is provided with {@link #deploy()} or {@link #deploy(String)} with ("-") or (""),
     *     then the {@link ConfigKelp} will be used.
     * @see #loadConfigFile(String) 
     * @see #primary() 
     */
    public static ClusterKelp<ConfigKelp> create() {
        return create(ConfigKelp.class);
    }

    public static  <ConfigType extends ConfigKelp> ClusterKelp<ConfigType> create(Class<ConfigType> confType) {
        return new ClusterKelp<>(confType, ActorPlacementKelp.class);
    }

    public static ClusterKelp<ConfigKelp> createAndDeploy() {
        return createAndDeploy(ConfigKelp.class);
    }

    public static  <ConfigType extends ConfigKelp> ClusterKelp<ConfigType> createAndDeploy(Class<ConfigType> confType) {
        ClusterKelp<ConfigType> c = create(confType);
        c.deploy();
        return c;
    }


    @Override
    protected void deployPrimaryInitSystem() {
        primary.log("primary %s: create system with serializer %s", primary.getDeploymentConfig().getAddress(), primary.getDeploymentConfig().kryoBuilderType);
        system = createSystemKelpPrimary(primary.getDeploymentConfig());
//        system.getLocalSystem().setLogger(new ConfigBase.SystemLoggerHeader(system.getLogger(), primary.getAppConfig()));
    }

    public ActorSystemKelp createSystemKelpPrimary(ConfigDeployment configDeployment) {
        return ActorSystemKelp.create(configDeployment, configDeployment.kryoBuilder(defaultBuilderType()));
    }

    @Override
    public ActorSystemKelp getSystem() {
        return (ActorSystemKelp) system;
    }

    protected Class<? extends KryoBuilder> defaultBuilderType() {
        return KryoBuilderKelp.class;
    }

    @Override
    protected ActorSystemRemote createAttachInitSystem() {
        return ActorSystemKelp.create(primary.getDeploymentConfig(), KryoBuilder.builder(getBuilderType(attachKryoBuilderType, defaultBuilderType())));
    }

    @Override
    public Class<?> getNodeMainType() {
        return NodeMainKelp.class;
    }

    public static class NodeMainKelp extends NodeMain {
        public static void main(String[] args) throws Exception {
            new NodeMainKelp().run(args);
        }

        @Override
        protected ActorSystemRemote initSystem() {
            return createSystemKelpNode(kryoBuilderType, this.configDeployment);
        }

        public ActorSystemKelp createSystemKelpNode(String buildType, ConfigDeployment configDeployment) {
            return ActorSystemKelp.create(configDeployment, KryoBuilder.builder(getBuilderType(buildType, defaultBuilderType())));
        }

        @Override
        protected Class<? extends KryoBuilder> defaultBuilderType() {
            return KryoBuilderKelp.class;
        }
    }

    //////////////

    @Override
    public ActorSystemKelp deploy() {
        return (ActorSystemKelp) super.deploy();
    }

    @Override
    public ActorSystemKelp deploy(ConfigType mergedConf) {
        return (ActorSystemKelp) super.deploy(mergedConf);
    }

    @Override
    public ActorSystemKelp deploy(String confFile) {
        return (ActorSystemKelp) super.deploy(confFile);
    }

    @Override
    public ActorSystemKelp deploy(String confFile, ConfigType mergedConf) {
        return (ActorSystemKelp) super.deploy(confFile, mergedConf);
    }

    @Override
    public ActorSystemKelp deploy(List<ClusterCommands.ClusterUnit<ConfigType>> clusterUnits) throws Exception {
        return (ActorSystemKelp) super.deploy(clusterUnits);
    }

    @Override
    public ActorSystemKelp deploy(ConfigType mergedConf, ConfigDeployment mergedDeployConf) {
        return (ActorSystemKelp) super.deploy(mergedConf, mergedDeployConf);
    }

    @Override
    public ActorSystemKelp deploy(String confFile, ConfigType mergedConf, ConfigDeployment mergedDeployConf) {
        return (ActorSystemKelp) super.deploy(confFile, mergedConf, mergedDeployConf);
    }

    ///////////// stats

    public <T> T placeGetForActor(ActorRef actor, CallableMessage<ActorPlacementKelp<ConfigType>, T> getter) {
        if (actor instanceof ActorRefRemote) {
            ActorAddress.ActorAddressRemote host = ((ActorRefRemote) actor).getAddress().getHostAddress();
            return placeGet(host, getter);
        } else {
            return getter.call(getPrimaryPlace());
        }
    }

    @PropertyInterface("actor-stats")
    public ActorStat getActorStat(ActorRef actor) {
        return placeGetForActor(actor, new CallableMessageNewActorStat<>(actor));
    }

    @PropertyInterface("stages")
    public List<StageStat> getStagesStat() {
        return getStagesStat(true);
    }

    @PropertyInterface("stages")
    public List<StageStat> getStagesStat(boolean detail) {
        List<ActorRef> stages = new ArrayList<>(placeGet(new ActorPlacementKelp.StageGraphGet<>()));
        List<StageStat> stats = new ArrayList<>(stages.size());
        for (ActorRef stage : stages) {
            try {
                StageStat s = placeGetForActor(stage, new CallableMessageNewStageStat<>(stage));
                stats.add(s);
                if (s != null && detail) {
                    s.actorStats = s.getActors().stream()
                            .map(this::getActorStat)
                            .collect(Collectors.toList());
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return stats;
    }

    public static class CallableMessageNewActorStat<ConfigType extends ConfigKelp>
            implements CallableMessage<ActorPlacementKelp<ConfigType>, ClusterKelp.ActorStat>, MessageDataStats {
        public static final long serialVersionUID = -1;
        public ActorRef actor;

        public CallableMessageNewActorStat() {}

        public CallableMessageNewActorStat(ActorRef actor) {
            this.actor = actor;
        }

        @Override
        public ActorStat call(ActorPlacementKelp<ConfigType> self) {
            return new ClusterKelp.ActorStat().set(actor.asLocal());
        }
    }
    public static class CallableMessageNewStageStat<ConfigType extends ConfigKelp>
            implements CallableMessage<ActorPlacementKelp<ConfigType>, StageStat>, MessageDataStats {
        public static final long serialVersionUID = -1;
        public ActorRef actor;

        public CallableMessageNewStageStat() {}

        public CallableMessageNewStageStat(ActorRef actor) {
            this.actor = actor;
        }

        @Override
        public StageStat call(ActorPlacementKelp<ConfigType> self) {
            return new ClusterKelp.StageStat().set((KelpStageGraphActor) actor.asLocal());
        }
    }

    public static class ActorStat implements Serializable, ToJson {
        public static final long serialVersionUID = 1L;

        public ActorRef ref;
        public String name;
        public String className;
        public int shuffleIndex;
        public int mergedCount;
        public boolean unit;
        public Set<String> mergedActorNames;
        public ActorRef nextStage;

        public String mailboxClassName;
        public long mailboxPreviousSize;
        public List<HistogramStat> histograms;

        public ActorStat set(Actor actor) {
            ref = actor;
            if (actor != null) {
                name = actor.getName();
                className = actor.getClass().getName();
                if (actor instanceof ActorKelp) {
                    ActorKelp<?> a = (ActorKelp<?>) actor;
                    setKelp(a);
                    setMailbox(a, a.getMailboxAsKelp());
                }
            }
            return this;
        }

        public void setKelp(ActorKelp<?> actor) {
            shuffleIndex = actor.getShuffleIndex();
            mergedActorNames = new HashSet<>(actor.getMergedActorNames());
            mergedCount = actor.getMergedCount();
            unit = actor.isUnit();
            nextStage = actor.nextStageActor();
        }

        public void setMailbox(ActorKelp<?> actor, MailboxKelp mbox) {
            mailboxClassName = mbox.getMailbox().getClass().getName();
            mailboxPreviousSize = mbox.getMailbox().getPreviousSizeOnMemory();
            histograms = new ArrayList<>(mbox.getEntrySize());
            for (HistogramEntry e : mbox.getEntries()) {
                histograms.add(new HistogramStat().set(actor, mbox, e));
            }
        }

        @Override
        public Map<String, Object> toJson(Function<Object, Object> valueConverter) {
            Map<String,Object> json = new LinkedHashMap<>();
            json.put("ref", toJson(valueConverter, ref, ""));
            json.put("name", toJson(valueConverter, name, ""));
            json.put("className", toJson(valueConverter, className, ""));
            json.put("shuffleIndex", toJson(valueConverter, (long) shuffleIndex));
            json.put("mergedCount", toJson(valueConverter, (long) mergedCount));
            json.put("unit", toJson(valueConverter, unit));
            json.put("mergedActorNames", toJson(valueConverter, mergedActorNames, Collections.emptySet()));
            json.put("nextStage", toJson(valueConverter, nextStage, ""));
            json.put("mailboxPreviousSize", toJson(valueConverter, mailboxPreviousSize));
            json.put("mailboxClassName", toJson(valueConverter, mailboxClassName, ""));
            json.put("histograms", toJson(valueConverter, histograms, new ArrayList<>()));
            return json;
        }
    }

    public static class HistogramStat  implements Serializable, ToJson {
        public static final long serialVersionUID = 1L;
        public int entryId;
        public boolean persistable;

        public long valueSize;
        public long leafSize;
        public long leafSizeNonZero;
        public long persistedSize;
        public float[] persistHistoryTotalMean;

        public HistogramStat set(ActorKelp<?> actor, MailboxKelp mbox, HistogramEntry e) {
            entryId = e.getEntryId();
            HistogramTree tree = e.getTree();
            if (tree != null) {
                valueSize = tree.getTreeSize();
                leafSize = tree.getLeafSize();
                leafSizeNonZero = tree.getLeafSizeNonZero();

                if (tree instanceof HistogramTreePersistable) {
                    persistable = true;
                    HistogramTreePersistable pTree = (HistogramTreePersistable) tree;
                    persistedSize = pTree.getPersistedSize();
                    persistHistoryTotalMean = pTree.getHistory().totalMean();
                }
            }
            return this;
        }

        @Override
        public Map<String, Object> toJson(Function<Object, Object> valueConverter) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("entryId", toJson(valueConverter, (long) entryId));
            json.put("persistable", toJson(valueConverter, persistable));
            json.put("valueSize", toJson(valueConverter, valueSize));
            json.put("leafSize", toJson(valueConverter, leafSize));
            json.put("leafSizeNonZero", toJson(valueConverter, leafSizeNonZero));
            json.put("persistedSize", toJson(valueConverter, persistedSize));
            if (persistHistoryTotalMean != null) {
                List<Object> ds = new ArrayList<>(persistHistoryTotalMean.length);
                for (float f : persistHistoryTotalMean) {
                    ds.add(valueConverter.apply((double) f));
                }
                json.put("persistHistoryTotalMean", ds);
            } else {
                json.put("persistHistoryTotalMean", new ArrayList<>());
            }
            return json;
        }
    }

    public static class StageStat implements Serializable, ToJson {
        public static final long serialVersionUID = 1L;
        public String name;
        public KelpStageGraphActor.StageStatusI status;
        public List<ActorRef> actors;
        public List<ActorStat> actorStats;

        public StageStat() {}

        public StageStat set(KelpStageGraphActor graph) {
            try {
                name = graph.getName();
                status = graph.getStageStatusAll().get(20, TimeUnit.SECONDS);
                actors = graph.getActorNodes().stream()
                        .map(KelpStageGraphActor.GraphActorNode::getActor)
                        .collect(Collectors.toList());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            return this;
        }

        public List<ActorRef> getActors() {
            return actors;
        }

        public Map<String, Object> toJson(KelpStageGraphActor.StageStatusI status, Function<Object, Object> valueConverter) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("finishedTime", toJson(valueConverter, status.getFinishedTime()));
            json.put("progress", toJson(valueConverter, status.getTotalProgress()));
            json.put("maxProgress", toJson(valueConverter, status.getMaxProgress()));
            if (status instanceof KelpStageGraphActor.StageStatusMulti) {
                KelpStageGraphActor.StageStatusMulti multi = (KelpStageGraphActor.StageStatusMulti) status;
                json.put("stages", multi.getStatusList().stream()
                        .map(e -> toJson(e, valueConverter))
                        .collect(Collectors.toList()));
            } else if (status instanceof KelpStageGraphActor.StageStatus) {
                KelpStageGraphActor.StageStatus st = (KelpStageGraphActor.StageStatus) status;
                json.put("key", st.getKey().format());
                json.put("remaining", st.getRemainingIndices().stream()
                        .sorted()
                        .map(i -> toJsonActor(i, valueConverter))
                        .collect(Collectors.toList()));
                json.put("finished", st.getFinishedIndices().stream()
                        .sorted()
                        .map(i -> toJsonActor(i, valueConverter))
                        .collect(Collectors.toList()));
            }
            return json;
        }

        public Object toJsonActor(int i, Function<Object, Object> valueConverter) {
            if (actorStats != null && i < actorStats.size()) {
                return actorStats.get(i).toJson(valueConverter);
            } else {
                return toJson(valueConverter, actors.get(i));
            }
        }

        @Override
        public Map<String, Object> toJson(Function<Object, Object> valueConverter) {
            if (status != null) {
                Map<String, Object> json = new LinkedHashMap<>();
                json.put("name", name);
                json.put("status", toJson(status, valueConverter));
                return json;
            } else {
                return new HashMap<>();
            }
        }
    }
}
