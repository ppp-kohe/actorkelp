package csl.actor.keyaggregate;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.CallableMessage;
import csl.actor.cluster.ClusterDeployment;
import csl.actor.cluster.ClusterHttp;
import csl.actor.cluster.MailboxPersistable;
import csl.actor.cluster.ResponsiveCalls;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClusterKeyAggregation extends ClusterDeployment<Config, ActorPlacementKeyAggregation> {
    public static ClusterKeyAggregation create() {
        return new ClusterKeyAggregation();
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

    @PropertyInterface("actor-stat")
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

    @PropertyInterface("actor-split")
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
        RouterSplitStat s = new RouterSplitStat().setSplit(split, path);
        map.put(s.getPathString(), s);
        if (split instanceof KeyAggregationRoutingSplit.RoutingSplitNode) {
            KeyAggregationRoutingSplit.RoutingSplitNode node = (KeyAggregationRoutingSplit.RoutingSplitNode) split;
            toStatTree(map, node.getLeft(), path.add(true));
            toStatTree(map, node.getLeft(), path.add(false));
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
        public String stateType = "";
        public long processCount;
        public String outputFileHeader;

        public int mailboxSize;
        public boolean mailboxPersistable;
        public List<HistogramStat> histograms;

        //router
        public int maxHeight;
        public int height;
        public boolean parallelRouting;

        public ActorStat set(Actor actor) {
            ref = actor;
            if (actor instanceof ActorKeyAggregation) {
                ActorKeyAggregation a = (ActorKeyAggregation) actor;
                name = actor.getName();
                className = actor.getClass().getName();
                outputFileHeader = a.getOutputFileHeader();

                setMailbox(a, a.getMailboxAsKeyAggregation());
                setState(a.getState());
            }
            return this;
        }

        public void setMailbox(ActorKeyAggregation actor, MailboxKeyAggregation mailbox) {
            mailboxSize = mailbox.size();
            mailboxPersistable = mailbox.getMailbox() instanceof MailboxPersistable;

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

            } else if (state instanceof ActorKeyAggregation.StateUnit) {
                stateType = "unit";
                processCount = state.getProcessCount();

            } else if (state instanceof ActorKeyAggregation.StateCanceled) {
                stateType = "canceled";
                processCount = state.getProcessCount();

            }
        }

        @Override
        public Map<String, Object> toJson(Function<Object, Object> valueConveter) {
            Map<String,Object> json = new LinkedHashMap<>();
            json.put("ref", valueConveter.apply(ref));
            json.put("name", name);
            json.put("className", className);
            json.put("stateType", stateType);
            json.put("processCount", processCount);
            json.put("outputFileHeader", outputFileHeader);
            json.put("mailboxSize", (long) mailboxSize);
            json.put("mailboxPersistable", mailboxPersistable);
            json.put("histogram", histograms == null ? null : histograms.stream()
                    .map(h -> h.toJson(valueConveter))
                    .collect(Collectors.toList()));
            json.put("maxHeight", (long) maxHeight);
            json.put("height", (long) height);
            json.put("parallelRouting", parallelRouting);
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
                    split = null;
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
            json.put("type", type);
            json.put("path", toString(path));
            json.put("depth", (long) depth);
            json.put("processCount", processCount);
            json.put("processPoints", processPoints == null ? null : processPoints.stream()
                .map(o -> o == null ? null : o.toString())
                .collect(Collectors.toList()));
            json.put("history", history == null ? null : history.stream()
                .map(h -> Arrays.stream(h)
                        .boxed()
                        .collect(Collectors.toList()))
                .collect(Collectors.toList()));
            json.put("actor", valueConverter.apply(actor));
            return json;
        }

        protected String toString(KeyAggregationRoutingSplit.SplitPath path) {
            if (path == null) {
                return null;
            } else {
                StringBuilder buf = new StringBuilder();
                for (boolean l : path.toFlags()) {
                    if (l) {
                        buf.append("1");
                    } else {
                        buf.append("0");
                    }
                }
                return buf.toString();
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
        public Map<String, Object> toJson(Function<Object, Object> valueConveter) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("entryId", (long) entryId);
            json.put("nextSchedule", toJson(nextSchedule));
            json.put("lastTraversal", toJson(lastTraversal));
            json.put("persistable", persistable);
            json.put("valueSize", valueSize);
            json.put("leafSize", leafSize);
            json.put("leafSizeNonZero", leafSizeNonZero);
            json.put("persistedSize", persistedSize);
            if (persistHistoryTotalMean != null) {
                List<Object> ds = new ArrayList<>(persistHistoryTotalMean.length);
                for (float f : persistHistoryTotalMean) {
                    ds.add((double) f);
                }
                json.put("persistHistoryTotalMean", ds);
            }
            return json;
        }

        private Object toJson(Instant n) {
            if (n == null) {
                return null;
            } else {
                return n.toString();
            }
        }
    }
}
