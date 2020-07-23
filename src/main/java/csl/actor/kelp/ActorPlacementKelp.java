package csl.actor.kelp;

import csl.actor.*;
import csl.actor.cluster.ClusterDeployment;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.util.FileSplitter;
import csl.actor.util.PathModifier;
import csl.actor.util.ResponsiveCalls;
import csl.actor.remote.ActorAddress;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("rawtypes")
public class ActorPlacementKelp extends ClusterDeployment.ActorPlacementForCluster<Config> {

    public ActorPlacementKelp(ActorSystem system, String name) {
        super(system, name);
    }

    public ActorPlacementKelp(ActorSystem system) {
        super(system);
    }

    public ActorPlacementKelp(ActorSystem system, String name, PlacementStrategy strategy) {
        super(system, name, strategy);
    }

    public ActorPlacementKelp(ActorSystem system, PlacementStrategy strategy) {
        super(system, strategy);
    }

    @Override
    protected PlacementStrategy initStrategy() {
        return new PlacementStrategyRoundRobinThreads();
    }

    @Override
    public Serializable toSerializable(Actor a, long num, Serializable previous, ActorAddress.ActorAddressRemoteActor target) {
        if (previous != null) {
            return previous;
        } else if (a instanceof ActorKelp) {
             return ((ActorKelp) a).toSerializable(num);
        } else {
            return null;
        }
    }

    protected ActorKelp.ActorKelpSerializable withConfig(ActorAddress target, ActorKelp.ActorKelpSerializable s) {
        s.config = remoteConfig.getOrDefault(target.getHostAddress(), s.config);
        return s;
    }

    @Override
    public Actor fromSerializable(Serializable s, long num) {
        if (s instanceof ActorKelp.ActorKelpSerializable) {
            try {
                ActorKelp.ActorKelpSerializable state = (ActorKelp.ActorKelpSerializable) s;
                state = withConfig(getSelfAddress().getHostAddress(), state);
                Actor a = state.create(getSystem(), num);
                getSystem().send(new Message.MessageNone(a));
                return a;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } else {
            throw new RuntimeException("failure: " + s);
        }
    }

    @Override
    protected ActorRef placeLocal(Actor a) {
        a.getSystem().send(new Message.MessageNone(a));
        return a;
    }

    public CompletableFuture<?> connectAndSplitStage(ActorRef... stageActors) {
        CompletableFuture<?> f = connectStage(stageActors);
        return splitStage(f, stageActors);
    }

    public CompletableFuture<?> connectStage(ActorRef... stageActors) {
        return composeChain(null, ActorKelp::setNextStage, stageActors);
    }

    public CompletableFuture<?> splitStage(CompletableFuture<?> prevTask, ActorRef... stageActors) {
        Task splitTask = (a) -> {
            try {
                return a.routerSplit((Integer) a.routerGetMaxHeight().get());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
        return compose(prevTask, splitTask, stageActors);
    }

    public CompletableFuture<?> compose(CompletableFuture<?> prevTask, Task task, ActorRef... actors) {
        CompletableFuture<?> f = prevTask;
        for (ActorRef next : actors) {
            if (f != null) {
                if (next instanceof ActorKelp) {
                    ActorKelp nextActor = (ActorKelp) next;
                    f = f.thenCompose((_v) -> task.apply(nextActor));
                } else {
                    ActorSystem system = getSystem();
                    f = f.thenCompose((_v) -> ResponsiveCalls.<ActorKelp>sendTaskConsumer(system, next, (a) -> {
                        try {
                            task.apply(a).get();
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }));
                }
            }
        }
        return f;
    }

    public CompletableFuture<?> composeChain(CompletableFuture<?> prevTask, TaskChain task, ActorRef... stageActors) {
        ActorRef prev = null;
        CompletableFuture<?> f = prevTask;
        for (ActorRef next : stageActors) {
            if (prev != null) {
                if (prev instanceof ActorKelp) {
                    ActorKelp prevActor = (ActorKelp) prev;
                    if (f != null) {
                        f = f.thenCompose((_v) -> task.apply(prevActor, next));
                    } else {
                        f = task.apply(prevActor, next);
                    }
                } else {
                    ActorSystem system = getSystem();
                    ActorRef prevActor = prev;
                    if (f != null) {
                        f = f.thenCompose((_v) -> ResponsiveCalls.sendTask(system, prevActor, (a) ->
                                task.apply((ActorKelp) a, next)));
                    } else {
                        f = ResponsiveCalls.sendTask(system, prevActor, (a) ->
                                task.apply((ActorKelp) a, next));
                    }
                }
            }
            prev = next;
        }
        return f;
    }

    @FunctionalInterface
    public interface Task extends Serializable {
        CompletableFuture<?> apply(ActorKelp prev);
    }

    @FunctionalInterface
    public interface TaskChain extends Serializable {
        CompletableFuture<?> apply(ActorKelp prev, ActorRef next);
    }

    public FileMapper fileMapper(FileSplitter splitter) {
        return new FileMapper(getSystem(), "fileMapper", getPrimaryConfig(), splitter);
    }

    public FileMapper fileMapperWithSplitCount(long splits) {
        return fileMapper(FileSplitter.getWithSplitCount(splits,
                PathModifier.getPathModifier(getSystem())));
    }

    public FileMapper fileMapperWithSplitLength(long length) {
        return fileMapper(FileSplitter.getWithSplitLength(length,
                PathModifier.getPathModifier(getSystem())));
    }

    public ActorKelpOneShot actor(String name, InitBuilder builderFunction) {
        return new ActorKelpOneShot(getSystem(), name, getPrimaryConfig(), builderFunction);
    }

    public interface InitBuilder extends Serializable {
        void build(ActorKelpOneShot self, ActorBehaviorBuilderKelp builder);
    }

    public static class ActorKelpOneShot extends ActorKelp<ActorKelpOneShot> {
        protected InitBuilder builder;
        protected LineWriter writer;

        public ActorKelpOneShot(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        public ActorKelpOneShot(ActorSystem system, String name, Config config, InitBuilder builder) {
            super(system, name, config);
            this.builder = builder;
            initBuilderFromBuilder();
        }

        @Override
        protected void initSerializedInternalState(Serializable s) {
            OneShotState os = (OneShotState) s;
            if (os != null) {
                builder = os.builder;
                initBuilderFromBuilder();
                writer = os.writer;
                if (writer != null) {
                    writer.setOwner(this);
                }
            }
        }

        protected void initBuilderFromBuilder() {
            ActorBehaviorBuilderKelp b = behaviorBuilder();
            builder.build(this, b);
            behavior = b.build();
        }

        @Override
        protected Serializable toSerializableInternalState() {
            return new OneShotState(builder, writer);
        }

        @Override
        protected ActorBehavior initBehavior() { //nothing
            return null;
        }

        @Override
        protected void initClone(ActorKelpOneShot original) {
            super.initClone(original);
            initBuilderFromBuilder();
            LineWriter w = original.writer;
            if (w != null) {
                writer = w.copy(this);
            }
        }

        public LineWriter writer() {
            return writer(DEFAULT_PREFIX, DEFAULT_SUFFIX);
        }

        public LineWriter writer(String prefix, String suffix) {
            if (writer == null) {
                writer = new LineWriter(prefix, suffix, this);
            } else if (!prefix.equals(writer.getPrefix()) || !suffix.equals(writer.getSuffix())) {
                writer.close();
                writer = new LineWriter(prefix, suffix, this);
            }
            return writer;
        }
    }

    public static class OneShotState implements Serializable {
        public static final long serialVersionUID = 1L;
        public InitBuilder builder;
        public LineWriter writer;

        public OneShotState(InitBuilder builder, LineWriter writer) {
            this.builder = builder;
            this.writer = writer;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + builder + ", " + writer + ")";
        }
    }

    public static String DEFAULT_PREFIX = "%a/output-";
    public static String DEFAULT_SUFFIX = ".txt";

    public static class LineWriter implements Serializable {
        public static final long serialVersionUID = 1L;
        protected String prefix;
        protected String suffix;
        protected transient String path;
        protected transient Path expandedPath;
        protected transient PrintWriter writer;
        protected transient ScheduledFuture<?> flushTask;
        protected transient ActorKelp owner;

        public LineWriter(ActorKelp owner) {
            this(DEFAULT_PREFIX, DEFAULT_SUFFIX, owner);
        }

        public LineWriter(String prefix, String suffix, ActorKelp owner) {
            this.prefix = prefix;
            this.suffix = suffix;
            this.owner = owner;
        }

        public LineWriter copy(ActorKelp owner) {
            return new LineWriter(prefix, suffix, owner);
        }

        public void setOwner(ActorKelp owner) {
            this.owner = owner;
        }

        public ActorKelp getOwner() {
            return owner;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(prefix=" + prefix + ", suffix=" + suffix +
                    "current-path=" + path + ", current-expandedPath=" + expandedPath + ")";
        }

        public String getSuffix() {
            return suffix;
        }

        public String getPrefix() {
            return prefix;
        }

        public void print(Object value) {
            try {
                if (writer == null) {
                    init();
                }
                writer.print(value);
            } catch (Exception ex) {
                owner.getSystem().getLogger().log(true, owner.config.logColor, ex, "write failure: path=%s, expandedPath=%s, %s", path, expandedPath, owner);
            }
        }

        public void println(Object value) {
            try {
                if (writer == null) {
                    init();
                }
                writer.println(value);
            } catch (Exception ex) {
                owner.getSystem().getLogger().log(true, owner.config.logColor, ex, "write failure: path=%s, expandedPath=%s, %s", path, expandedPath, owner);
            }
        }

        protected void init() throws Exception {
            ActorKelp self = owner;
            path = getPath(self);
            expandedPath = PathModifier.getPathModifier(self.getSystem()).getExpanded(path);
            Path parent = expandedPath.getParent();
            if (parent != null && !parent.toString().isEmpty()) {
                Files.createDirectories(expandedPath.getParent());
            }
            writer = initWriter(expandedPath);
            flushTask = initFlushTask(self);
        }

        protected PrintWriter initWriter(Path expandedPath) throws Exception {
            return new PrintWriter(new FileWriter(expandedPath.toFile(), StandardCharsets.UTF_8));

        }

        protected ScheduledFuture<?> initFlushTask(ActorKelp self) {
            return self.getSystem().getScheduledExecutor().scheduleAtFixedRate(() ->
                    self.tell(CallableMessage.callableMessageConsumer((a) -> this.flush())), 3, 3, TimeUnit.SECONDS);
        }

        public void flush() {
            if (writer != null) {
                writer.flush();
            }
        }

        public String getPath(ActorKelp self) {
            return prefix + self.getOutputFileHeader() + suffix;
        }

        public void close() {
            if (writer != null) {
                writer.close();
            }
            if (flushTask != null) {
                flushTask.cancel(false);
            }
        }
    }
}
