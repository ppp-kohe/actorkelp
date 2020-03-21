package csl.actor.keyaggregate;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.CallableMessage;
import csl.actor.cluster.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.KryoBuilder;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClusterKeyAggregation extends ClusterDeployment<Config, ActorPlacementKeyAggregation> {

    public static void main(String[] args) throws Exception {
        ClusterKeyAggregation.create()
                .runAsRemoteDriver(args);
    }

    public static void run(String configFile, String mainType, String... args) throws Exception {
        ClusterKeyAggregation.create()
                .runAsRemoteDriver(configFile, mainType, Arrays.asList(args));
    }

    public static ClusterKeyAggregation create() {
        return new ClusterKeyAggregation();
    }

    public static ClusterKeyAggregation createWithAttachKryoBuilderType(Class<? extends KryoBuilder> cls) {
        ClusterKeyAggregation c = create();
        c.setAttachKryoBuilderType(cls.getName());
        return c;
    }

    public ClusterKeyAggregation() {
        super(Config.class, ActorPlacementKeyAggregation.class);
    }

    public ClusterKeyAggregation(Class<Config> defaultConfType, Class<ActorPlacementKeyAggregation> placeType) {
        super(defaultConfType, placeType);
    }

    public <T> T placeGetForActor(ActorRef actor, CallableMessage<ActorPlacementKeyAggregation, T> getter) {
        if (actor instanceof ActorRefRemote) {
            ActorAddress.ActorAddressRemote host = ((ActorRefRemote) actor).getAddress().getHostAddress();
            return placeGet(host, getter);
        } else {
            return getter.call(getMasterPlace(), null);
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

    public KeyAggregationRoutingSplit.SplitPath parsePath(String path) {
        KeyAggregationRoutingSplit.SplitPath p = new KeyAggregationRoutingSplit.SplitPath();
        for (char c : path.toCharArray()) {
            boolean b = (c == '1');
            p = p.add(b);
        }
        return p;
    }

    public RouterSplitStat getSplit(ActorRef actor, KeyAggregationRoutingSplit.SplitPath path) {
        return placeGetForActor(actor, a -> new RouterSplitStat().set(actor.asLocal(), path));
    }

    @PropertyInterface("actor-split-tree")
    public Map<String,RouterSplitStat> getSplitTree(ActorRef actor) {
        return placeGetForActor(actor, a -> toStatTree(actor.asLocal()));
    }

    public static Map<String, RouterSplitStat> toStatTree(Actor a) {
        Map<String, RouterSplitStat> router = new LinkedHashMap<>();
        if (a instanceof ActorKeyAggregation) {
            ActorKeyAggregation.State state = ((ActorKeyAggregation) a).getState();
            if (state instanceof KeyAggregationStateRouter) {
                toStatTree(router, ((KeyAggregationStateRouter) state).getSplit(),
                        new KeyAggregationRoutingSplit.SplitPath());
            }
        }
        return router;
    }

    public static void toStatTree(Map<String, RouterSplitStat> map, KeyAggregationRoutingSplit split, KeyAggregationRoutingSplit.SplitPath path) {
        if (split != null) {
            RouterSplitStat s = new RouterSplitStat().setSplit(split, path);
            map.put(s.getPathString(), s);
            if (split instanceof KeyAggregationRoutingSplit.RoutingSplitNode) {
                KeyAggregationRoutingSplit.RoutingSplitNode node = (KeyAggregationRoutingSplit.RoutingSplitNode) split;
                toStatTree(map, node.getLeft(), path.add(true));
                toStatTree(map, node.getRight(), path.add(false));
            }
        }
    }

    @PropertyInterface("router-split")
    public CompletableFuture<?> routerSplit(ActorRef actor, int height) {
        return ResponsiveCalls.<ActorKeyAggregation>sendTaskConsumer(getSystem(), actor,
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
        return ResponsiveCalls.<ActorKeyAggregation>sendTaskConsumer(getSystem(), actor,
                a -> awaits(a.routerMergeInactive()));
    }

    @PropertyInterface("router-split-or-merge")
    public CompletableFuture<?> routerSplitOrMerge(ActorRef actor, int height) {
        return ResponsiveCalls.<ActorKeyAggregation>sendTaskConsumer(getSystem(), actor,
                a -> awaits(a.routerSplitOrMerge(height)));
    }


    public static class ActorStat implements Serializable, ClusterHttp.ToJson {
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
                if (actor instanceof ActorKeyAggregation) {
                    ActorKeyAggregation a = (ActorKeyAggregation) actor;

                    MailboxPersistable.PersistentFileManager m = MailboxPersistable.getPersistentFile(a.getSystem(), a::persistMailboxPath);
                    outputFileHeader = m.getPathModifier().expandPath(a.getOutputFileHeader());

                    setMailbox(a, a.getMailboxAsKeyAggregation());
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

        public void setMailbox(ActorKeyAggregation actor, MailboxKeyAggregation mailbox) {
            mailboxSize = mailbox.size();
            mailboxPersistable = mailbox.getMailbox() instanceof MailboxPersistable;
            mailboxThreshold = mailbox.getThreshold();

            histograms = new ArrayList<>(mailbox.getEntrySize());
            for (MailboxKeyAggregation.HistogramEntry e : mailbox.getEntries()) {
                histograms.add(new HistogramStat().set(actor, mailbox, e));
            }
        }

        public void setState(ActorKeyAggregation.State state) {
            if (state instanceof KeyAggregationStateRouter) {
                stateType = "router";
                KeyAggregationStateRouter router = (KeyAggregationStateRouter) state;
                processCount = state.getProcessCount();
                maxHeight = router.getMaxHeight();
                height = router.getHeight();
                parallelRouting = !router.isNonParallelRouting();
                canceled = new ArrayList<>(router.getCanceled());
                phase = router.getPhase().values().stream()
                    .map(v -> new PhaseStat().set(v))
                    .collect(Collectors.toList());

            } else if (state instanceof ActorKeyAggregation.StateUnit) {
                stateType = "unit";
                processCount = state.getProcessCount();

            } else if (state instanceof ActorKeyAggregation.StateCanceled) {
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

    public static class PhaseStat implements Serializable, ClusterHttp.ToJson {
        public Object key;
        public Instant startTime;
        public Instant endTime;
        public ActorRef target;
        public Map<ActorRef, Boolean> finished;

        public PhaseStat set(KeyAggregationPhaseEntry e) {
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

    public static class RouterSplitStat implements Serializable, ClusterHttp.ToJson {
        public String type = "";
        public KeyAggregationRoutingSplit.SplitPath path;
        public int depth;
        public List<long[]> history;
        public long processCount;
        public List<Object> processPoints;
        public ActorRef actor;

        public RouterSplitStat set(Actor actor, KeyAggregationRoutingSplit.SplitPath path)  {
            if (actor instanceof ActorKeyAggregation) {
                return set(((ActorKeyAggregation) actor).getState(), path);
            } else {
                type = "";
                return this;
            }
        }

        public RouterSplitStat set(ActorKeyAggregation.State state, KeyAggregationRoutingSplit.SplitPath path) {
            if (state instanceof KeyAggregationStateRouter) {
                setRouter((KeyAggregationStateRouter) state, path);
            } else {
                type = "";
            }
            return this;
        }

        public void setRouter(KeyAggregationStateRouter router, KeyAggregationRoutingSplit.SplitPath path) {
            KeyAggregationRoutingSplit split = router.getSplit();
            for (boolean left : path.toFlags()) {
                if (!(split instanceof KeyAggregationRoutingSplit.RoutingSplitNode)) {
                    break;
                }
                KeyAggregationRoutingSplit.RoutingSplitNode node = (KeyAggregationRoutingSplit.RoutingSplitNode) split;
                if (left) {
                    split = node.getLeft();
                } else {
                    split = node.getRight();
                }
            }
            setSplit(split, path);
        }
        public RouterSplitStat setSplit(KeyAggregationRoutingSplit split, KeyAggregationRoutingSplit.SplitPath path) {
            if (split instanceof KeyAggregationRoutingSplit.RoutingSplitNode) {
                KeyAggregationRoutingSplit.RoutingSplitNode node = (KeyAggregationRoutingSplit.RoutingSplitNode) split;
                type = "node";
                depth = node.getDepth();
                history = node.getHistory().toList().stream()
                        .map(h -> new long[] {h.left.get(), h.right.get()})
                        .collect(Collectors.toList());
                this.path = node.getPath();
                processCount = node.getProcessCount();
                processPoints = Arrays.asList(Arrays.copyOf(node.getSplitPoints(), node.getSplitPoints().length));
            } else if (split instanceof KeyAggregationRoutingSplit.RoutingSplitLeaf) {
                KeyAggregationRoutingSplit.RoutingSplitLeaf leaf = (KeyAggregationRoutingSplit.RoutingSplitLeaf) split;
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

        protected String toString(KeyAggregationRoutingSplit.SplitPath path) {
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

    public static class HistogramStat implements Serializable, ClusterHttp.ToJson {
        public int entryId;
        public Instant nextSchedule;
        public Instant lastTraversal;
        public boolean persistable;

        public long valueSize;
        public long leafSize;
        public long leafSizeNonZero;

        public long persistedSize;
        public float[] persistHistoryTotalMean;

        public HistogramStat set(ActorKeyAggregation actor,
                                 MailboxKeyAggregation mailbox, MailboxKeyAggregation.HistogramEntry e) {
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
