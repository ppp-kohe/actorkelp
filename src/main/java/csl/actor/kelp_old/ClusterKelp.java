package csl.actor.kelp_old;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.CallableMessage;
import csl.actor.cluster.*;
import csl.actor.persist.MailboxPersistableReplacement;
import csl.actor.persist.PersistentFileManager;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.KryoBuilder;
import csl.actor.util.ResponsiveCalls;
import csl.actor.util.ToJson;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("rawtypes")
public class ClusterKelp extends ClusterDeployment<Config, ActorPlacementKelp> {

    public static void main(String[] args) throws Exception {
        ClusterKelp.create()
                .runAsRemoteDriver(args);
    }

    public static void run(String configFile, String mainType, String... args) throws Exception {
        ClusterKelp.create()
                .runAsRemoteDriver(configFile, mainType, Arrays.asList(args));
    }

    public static void run(List<ClusterCommands.ClusterUnit<Config>> units, String mainType, String... args) throws Exception {
        ClusterKelp.create()
                .runAsRemoteDriver(units, mainType, Arrays.asList(args));
    }

    public static ClusterKelp create() {
        return new ClusterKelp();
    }

    public static ClusterKelp createWithAttachKryoBuilderType(Class<? extends KryoBuilder> cls) {
        ClusterKelp c = create();
        c.setAttachKryoBuilderType(cls.getName());
        return c;
    }

    public ClusterKelp() {
        super(Config.class, ActorPlacementKelp.class);
    }

    public ClusterKelp(Class<Config> defaultConfType, Class<ActorPlacementKelp> placeType) {
        super(defaultConfType, placeType);
    }

    public <T> T placeGetForActor(ActorRef actor, CallableMessage<ActorPlacementKelp, T> getter) {
        if (actor instanceof ActorRefRemote) {
            ActorAddress.ActorAddressRemote host = ((ActorRefRemote) actor).getAddress().getHostAddress();
            return placeGet(host, getter);
        } else {
            return getter.call(getPrimaryPlace(), null);
        }
    }

    @PropertyInterface("actor-stats")
    public ActorStat getActorStat(ActorRef actor) {
        return placeGetForActor(actor, a -> new ActorStat().set(actor.asLocal()));
    }

    @PropertyInterface("actor-split")
    public RouterSplitStat getSplit(ActorRef actor, String path) {
        return getSplit(actor, parsePath(path));
    }

    public KelpRoutingSplit.SplitPath parsePath(String path) {
        KelpRoutingSplit.SplitPath p = new KelpRoutingSplit.SplitPath();
        for (char c : path.toCharArray()) {
            boolean b = (c == '1');
            p = p.add(b);
        }
        return p;
    }

    public RouterSplitStat getSplit(ActorRef actor, KelpRoutingSplit.SplitPath path) {
        return placeGetForActor(actor, a -> new RouterSplitStat().set(actor.asLocal(), path));
    }

    @PropertyInterface("actor-split-tree")
    public Map<String,RouterSplitStat> getSplitTree(ActorRef actor) {
        return placeGetForActor(actor, a -> toStatTree(actor.asLocal()));
    }

    public static Map<String, RouterSplitStat> toStatTree(Actor a) {
        Map<String, RouterSplitStat> router = new LinkedHashMap<>();
        if (a instanceof ActorKelp) {
            ActorKelp.State state = ((ActorKelp) a).getState();
            if (state instanceof KelpStateRouter) {
                toStatTree(router, ((KelpStateRouter) state).getSplit(),
                        new KelpRoutingSplit.SplitPath());
            }
        }
        return router;
    }

    public static void toStatTree(Map<String, RouterSplitStat> map, KelpRoutingSplit split, KelpRoutingSplit.SplitPath path) {
        if (split != null) {
            RouterSplitStat s = new RouterSplitStat().setSplit(split, path);
            map.put(s.getPathString(), s);
            if (split instanceof KelpRoutingSplit.RoutingSplitNode) {
                KelpRoutingSplit.RoutingSplitNode node = (KelpRoutingSplit.RoutingSplitNode) split;
                toStatTree(map, node.getLeft(), path.add(true));
                toStatTree(map, node.getRight(), path.add(false));
            }
        }
    }

    @PropertyInterface("router-split")
    public CompletableFuture<?> routerSplit(ActorRef actor, int height) {
        return ResponsiveCalls.<ActorKelp>sendTaskConsumer(getSystem(), actor,
                a -> awaits(a.routerSplit(height)));
    }

    private static void awaits(CompletableFuture<?> f) {
        try {
            f.get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @PropertyInterface("router-merge-inactive")
    public CompletableFuture<?> routerMergeInactive(ActorRef actor) {
        return ResponsiveCalls.<ActorKelp>sendTaskConsumer(getSystem(), actor,
                a -> awaits(a.routerMergeInactive()));
    }

    @PropertyInterface("router-split-or-merge")
    public CompletableFuture<?> routerSplitOrMerge(ActorRef actor, int height) {
        return ResponsiveCalls.<ActorKelp>sendTaskConsumer(getSystem(), actor,
                a -> awaits(a.routerSplitOrMerge(height)));
    }


    public static class ActorStat implements Serializable, ToJson {
        public static final long serialVersionUID = 1L;
        public ActorRef ref;
        public String name;
        public String className;
        public String stateType;
        public long processCount;
        public String outputFileHeader;

        public int mailboxSize;
        public int mailboxThreshold;
        public boolean mailboxPersistable;
        public List<HistogramStat> histograms;

        public ActorRef nextStage;

        //router
        public int maxHeight;
        public int height;
        public boolean parallelRouting;
        public List<ActorRef> canceled;
        //router or PhaseTerminalActor
        public List<PhaseStat> phase;

        public ActorStat set(Actor actor) {
            ref = actor;
            if (actor != null) {
                name = actor.getName();
                className = actor.getClass().getName();
                if (actor instanceof ActorKelp) {
                    ActorKelp a = (ActorKelp) actor;

                    PersistentFileManager m = PersistentFileManager.getPersistentFile(a.getSystem(), a.persistMailboxPath());
                    outputFileHeader = m.getPathModifier().expandPath(a.getOutputFileHeader());

                    setMailbox(a, a.getMailboxAsKelp());
                    setState(a.getState());
                } else if (actor instanceof PhaseShift.PhaseTerminalActor) {
                    stateType = "phaseTerminal";

                    PhaseShift.PhaseTerminalActor a = (PhaseShift.PhaseTerminalActor) actor;
                    phase = a.getCompleted().values().stream()
                            .map(e -> new PhaseStat().set(a, e))
                            .collect(Collectors.toList());
                } else {
                    stateType = "";
                }
                if (actor instanceof PhaseShift.StageSupported) {
                    nextStage = ((PhaseShift.StageSupported) actor).nextStage();
                }
            } else {
                stateType = "";
            }
            return this;
        }

        public void setMailbox(ActorKelp actor, MailboxKelp mailbox) {
            mailboxSize = mailbox.size();
            mailboxPersistable = mailbox.getMailbox() instanceof MailboxPersistableReplacement;
            mailboxThreshold = mailbox.getThreshold();

            histograms = new ArrayList<>(mailbox.getEntrySize());
            for (MailboxKelp.HistogramEntry e : mailbox.getEntries()) {
                histograms.add(new HistogramStat().set(actor, mailbox, e));
            }
        }

        public void setState(ActorKelp.State state) {
            if (state instanceof KelpStateRouter) {
                stateType = "router";
                KelpStateRouter router = (KelpStateRouter) state;
                processCount = state.getProcessCount();
                maxHeight = router.getMaxHeight();
                height = router.getHeight();
                parallelRouting = !router.isNonParallelRouting();
                canceled = new ArrayList<>(router.getCanceled());
                phase = router.getPhase().values().stream()
                    .map(v -> new PhaseStat().set(v))
                    .collect(Collectors.toList());

            } else if (state instanceof ActorKelp.StateUnit) {
                stateType = "unit";
                processCount = state.getProcessCount();

            } else if (state instanceof ActorKelp.StateCanceled) {
                stateType = "canceled";
                processCount = state.getProcessCount();

            }
        }

        @Override
        public Map<String, Object> toJson(Function<Object, Object> valueConverter) {
            Map<String,Object> json = new LinkedHashMap<>();
            json.put("ref", toJson(valueConverter, ref, ""));
            json.put("name", toJson(valueConverter, name, ""));
            json.put("className", toJson(valueConverter, className, ""));
            json.put("stateType", toJson(valueConverter, stateType, ""));
            json.put("processCount", toJson(valueConverter, processCount));
            json.put("outputFileHeader", toJson(valueConverter, outputFileHeader, ""));
            json.put("mailboxSize", toJson(valueConverter, (long) mailboxSize));
            json.put("mailboxThreshold", toJson(valueConverter, (long) mailboxThreshold));
            json.put("mailboxPersistable", toJson(valueConverter, mailboxPersistable));
            json.put("nextStage", toJson(valueConverter, nextStage, ""));
            json.put("histogram", toJson(valueConverter, histograms, new ArrayList<>()));
            json.put("maxHeight", toJson(valueConverter, (long) maxHeight));
            json.put("height", toJson(valueConverter, (long) height));
            json.put("parallelRouting", toJson(valueConverter, parallelRouting));
            json.put("canceled", toJson(valueConverter, canceled));
            json.put("phase", toJson(valueConverter, phase));
            return json;
        }
    }

    public static class PhaseStat implements Serializable, ToJson {
        public static final long serialVersionUID = 1L;
        public Object key;
        public Instant startTime;
        public Instant endTime;
        public ActorRef target;
        public Map<ActorRef, Boolean> finished;

        public PhaseStat set(KelpPhaseEntry e) {
            key = e.getKey();
            startTime = e.getOrigin().getStartTime();
            target = e.getOrigin().getTarget();
            finished = new HashMap<>(e.getFinished());
            return this;
        }

        public PhaseStat set(PhaseShift.PhaseTerminalActor a, PhaseShift.PhaseTerminalEntry e) {
            key = e.getKey();
            startTime = e.getStartTime();
            finished = new HashMap<>();
            target = a;
            if (e.future().isDone()) {
                try {
                    PhaseShift.PhaseCompleted completed = e.future().get();
                    finished.put(completed.getActor(), true);
                    endTime = completed.getTime();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            return this;
        }

        @Override
        public Map<String, Object> toJson(Function<Object, Object> valueConverter) {
            Map<String,Object> json = new LinkedHashMap<>();
            json.put("key", toJson(valueConverter, key));
            json.put("startTime", toJson(valueConverter, startTime));
            json.put("endTime", toJson(valueConverter, endTime));
            json.put("target", toJson(valueConverter, target));
            json.put("finished", toJson(valueConverter, finished));
            return json;
        }
    }

    public static class RouterSplitStat implements Serializable, ToJson {
        public static final long serialVersionUID = 1L;
        public String type = "";
        public KelpRoutingSplit.SplitPath path;
        public int depth;
        public List<long[]> history;
        public long processCount;
        public List<Object> processPoints;
        public ActorRef actor;

        public RouterSplitStat set(Actor actor, KelpRoutingSplit.SplitPath path)  {
            if (actor instanceof ActorKelp) {
                return set(((ActorKelp) actor).getState(), path);
            } else {
                type = "";
                return this;
            }
        }

        public RouterSplitStat set(ActorKelp.State state, KelpRoutingSplit.SplitPath path) {
            if (state instanceof KelpStateRouter) {
                setRouter((KelpStateRouter) state, path);
            } else {
                type = "";
            }
            return this;
        }

        public void setRouter(KelpStateRouter router, KelpRoutingSplit.SplitPath path) {
            KelpRoutingSplit split = router.getSplit();
            for (boolean left : path.toFlags()) {
                if (!(split instanceof KelpRoutingSplit.RoutingSplitNode)) {
                    break;
                }
                KelpRoutingSplit.RoutingSplitNode node = (KelpRoutingSplit.RoutingSplitNode) split;
                if (left) {
                    split = node.getLeft();
                } else {
                    split = node.getRight();
                }
            }
            setSplit(split, path);
        }
        public RouterSplitStat setSplit(KelpRoutingSplit split, KelpRoutingSplit.SplitPath path) {
            if (split instanceof KelpRoutingSplit.RoutingSplitNode) {
                KelpRoutingSplit.RoutingSplitNode node = (KelpRoutingSplit.RoutingSplitNode) split;
                type = "node";
                depth = node.getDepth();
                history = node.getHistory().toList().stream()
                        .map(h -> new long[] {h.left.get(), h.right.get()})
                        .collect(Collectors.toList());
                this.path = node.getPath();
                processCount = node.getProcessCount();
                processPoints = Arrays.asList(Arrays.copyOf(node.getSplitPoints(), node.getSplitPoints().length));
            } else if (split instanceof KelpRoutingSplit.RoutingSplitLeaf) {
                KelpRoutingSplit.RoutingSplitLeaf leaf = (KelpRoutingSplit.RoutingSplitLeaf) split;
                type = "leaf";
                this.path = path;
                this.depth = leaf.getDepth();
                this.processCount = leaf.getProcessCount();
                this.actor = leaf.getActor();
            } else {
                type = "";
            }
            return this;
        }

        @Override
        public Map<String, Object> toJson(Function<Object,Object> valueConverter) {
            Map<String,Object> json = new LinkedHashMap<>();
            json.put("type", toJson(valueConverter, type, ""));
            json.put("path", toJson(valueConverter, toString(path), ""));
            json.put("depth", toJson(valueConverter, (long) depth));
            json.put("processCount", toJson(valueConverter, processCount));
            json.put("processPoints", toJson(valueConverter, processPoints));
            json.put("history", history == null ? null : history.stream()
                .map(h -> Arrays.stream(h)
                        .boxed()
                        .map(valueConverter)
                        .collect(Collectors.toList()))
                .collect(Collectors.toList()));
            json.put("actor", toJson(valueConverter, actor, ""));
            return json;
        }

        protected String toString(KelpRoutingSplit.SplitPath path) {
            if (path == null) {
                return null;
            } else {
                return path.toBinaryString();
            }
        }

        public String getPathString() {
            return toString(path);
        }
    }

    public static class HistogramStat implements Serializable, ToJson {
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

        public HistogramStat set(ActorKelp actor,
                                 MailboxKelp mailbox, MailboxKelp.HistogramEntry e) {
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
