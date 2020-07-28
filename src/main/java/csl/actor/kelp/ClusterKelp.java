package csl.actor.kelp;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.CallableMessage;
import csl.actor.cluster.ClusterCommands;
import csl.actor.cluster.ClusterDeployment;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.kelp.behavior.HistogramEntry;
import csl.actor.kelp.behavior.KeyHistograms;
import csl.actor.kelp.behavior.KeyHistogramsPersistable;
import csl.actor.kelp.behavior.MailboxKelp;
import csl.actor.persist.MailboxManageable;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;
import csl.actor.util.ConfigBase;
import csl.actor.util.ToJson;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

/**
 * <pre>
 *   package your.pack;
 *   public class PrimaryMain {
 *     public static void main(String[] args) throws Exception {
 *         try (ClusterKelp&lt;ConfigKelp&gt; c = ClusterKelp.createAndDeploy()) {
 *             ActorKelp.FileReader f = new ActorKelp.FileReader(c.getSystem(), c.getPrimaryConfig());
 *             KelpStage&lt;MyActor&gt; a = f.connects(new MyActor(c.getSystem(), c.getPrimaryConfig()));
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
public class ClusterKelp<ConfigType extends ConfigKelp> extends ClusterDeployment<ConfigType, ActorPlacementKelp<ConfigType>> {
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
        system.getLocalSystem().setLogger(new ConfigBase.SystemLoggerHeader(system.getLogger(), primary.getAppConfig()));
    }

    public ActorSystemKelp createSystemKelpPrimary(ConfigDeployment configDeployment) {
        return ActorSystemKelp.create(configDeployment.kryoBuilder(defaultBuilderType()), configDeployment);
    }

    protected Class<? extends KryoBuilder> defaultBuilderType() {
        return KryoBuilderKelp.class;
    }

    @Override
    protected ActorSystemRemote createAttachInitSystem() {
        return ActorSystemKelp.create(KryoBuilder.builder(getBuilderType(attachKryoBuilderType, defaultBuilderType())), primary.getDeploymentConfig());
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
            ActorSystemRemote system = createSystemKelpNode(kryoBuilderType, this.configDeployment);
            system.getLocalSystem().setLogger(new ConfigBase.SystemLoggerHeader(system.getLogger(), configDeployment));
            return system;
        }

        public ActorSystemKelp createSystemKelpNode(String buildType, ConfigDeployment configDeployment) {
            return ActorSystemKelp.create(KryoBuilder.builder(getBuilderType(buildType, defaultBuilderType())), configDeployment);
        }

        @Override
        protected Class<? extends KryoBuilder> defaultBuilderType() {
            return KryoBuilderKelp.class;
        }
    }


    ///////////// stats

    public <T> T placeGetForActor(ActorRef actor, CallableMessage<ActorPlacementKelp<ConfigType>, T> getter) {
        if (actor instanceof ActorRefRemote) {
            ActorAddress.ActorAddressRemote host = ((ActorRefRemote) actor).getAddress().getHostAddress();
            return placeGet(host, getter);
        } else {
            return getter.call(getPrimaryPlace(), null);
        }
    }

    @PropertyInterface("actor-stats")
    public ActorStat getActorStat(ActorRef actor) {
        return placeGetForActor(actor, a -> new ClusterKelp.ActorStat().set(actor.asLocal()));
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
            if (mbox.getMailbox() instanceof MailboxManageable) {
                mailboxPreviousSize = ((MailboxManageable) mbox.getMailbox()).getPreviousSize();
            }
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
        public Instant nextSchedule;
        public Instant lastTraversal;
        public boolean persistable;

        public long valueSize;
        public long leafSize;
        public long leafSizeNonZero;
        public long persistedSize;
        public float[] persistHistoryTotalMean;


        public HistogramStat set(ActorKelp<?> actor, MailboxKelp mbox, HistogramEntry e) {
            entryId = e.getEntryId();
            nextSchedule = e.getNextSchedule();
            lastTraversal = e.getLastTraversal();
            KeyHistograms.HistogramTree tree = e.getTree();
            if (tree != null) {
                valueSize = tree.getTreeSize();
                leafSize = tree.getLeafSize();
                leafSizeNonZero = tree.getLeafSizeNonZero();

                if (tree instanceof KeyHistogramsPersistable.HistogramTreePersistable) {
                    persistable = true;
                    KeyHistogramsPersistable.HistogramTreePersistable pTree = (KeyHistogramsPersistable.HistogramTreePersistable) tree;
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
            json.put("nextSchedule", toJson(valueConverter, nextSchedule, Instant.EPOCH));
            json.put("lastTraversal", toJson(valueConverter, lastTraversal, Instant.EPOCH));
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
}
