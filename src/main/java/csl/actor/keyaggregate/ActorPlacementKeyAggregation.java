package csl.actor.keyaggregate;

import csl.actor.*;
import csl.actor.cluster.ClusterDeployment;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.cluster.FileSplitter;
import csl.actor.cluster.ResponsiveCalls;
import csl.actor.remote.ActorAddress;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

public class ActorPlacementKeyAggregation extends ClusterDeployment.ActorPlacementForCluster<Config> {

    public ActorPlacementKeyAggregation(ActorSystem system, String name) {
        super(system, name);
    }

    public ActorPlacementKeyAggregation(ActorSystem system) {
        super(system);
    }

    public ActorPlacementKeyAggregation(ActorSystem system, String name, PlacementStrategy strategy) {
        super(system, name, strategy);
    }

    public ActorPlacementKeyAggregation(ActorSystem system, PlacementStrategy strategy) {
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
        } else if (a instanceof ActorKeyAggregation) {
             return ((ActorKeyAggregation) a).toSerializable(num);
        } else {
            return null;
        }
    }

    protected ActorKeyAggregation.ActorKeyAggregationSerializable withConfig(ActorAddress target, ActorKeyAggregation.ActorKeyAggregationSerializable s) {
        s.config = remoteConfig.getOrDefault(target.getHostAddress(), s.config);
        return s;
    }

    @Override
    public Actor fromSerializable(Serializable s, long num) {
        if (s instanceof ActorKeyAggregation.ActorKeyAggregationSerializable) {
            try {
                ActorKeyAggregation.ActorKeyAggregationSerializable state = (ActorKeyAggregation.ActorKeyAggregationSerializable) s;
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
        return composeChain(null, ActorKeyAggregation::setNextStage, stageActors);
    }

    public CompletableFuture<?> splitStage(CompletableFuture<?> prevTask, ActorRef... stageActors) {
        Task splitTask = (a) -> {
            try {
                return a.routerSplit(a.routerGetMaxHeight().get());
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
                if (next instanceof ActorKeyAggregation) {
                    ActorKeyAggregation nextActor = (ActorKeyAggregation) next;
                    f = f.thenCompose((_v) -> task.apply(nextActor));
                } else {
                    ActorSystem system = getSystem();
                    f = f.thenCompose((_v) -> ResponsiveCalls.<ActorKeyAggregation>sendTaskConsumer(system, next, (a) -> {
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
                if (prev instanceof ActorKeyAggregation) {
                    ActorKeyAggregation prevActor = (ActorKeyAggregation) prev;
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
                                task.apply((ActorKeyAggregation) a, next)));
                    } else {
                        f = ResponsiveCalls.sendTask(system, prevActor, (a) ->
                                task.apply((ActorKeyAggregation) a, next));
                    }
                }
            }
            prev = next;
        }
        return f;
    }

    @FunctionalInterface
    public interface Task extends Serializable {
        CompletableFuture<?> apply(ActorKeyAggregation prev);
    }

    @FunctionalInterface
    public interface TaskChain extends Serializable {
        CompletableFuture<?> apply(ActorKeyAggregation prev, ActorRef next);
    }

    public FileMapper fileMapper(FileSplitter splitter) {
        return new FileMapper(getSystem(), "fileMapper", getMasterConfig(), splitter);
    }

    public FileMapper fileMapperWithSplitCount(long splits) {
        return fileMapper(FileSplitter.getWithSplitCount(splits,
                ConfigDeployment.getPathModifier(getSystem())));
    }

    public FileMapper fileMapperWithSplitLength(long length) {
        return fileMapper(FileSplitter.getWithSplitLength(length,
                ConfigDeployment.getPathModifier(getSystem())));
    }

    public ActorKeyAggregation actor(String name, InitBuilder builderFunction) {
        return new ActorKeyAggregationOneShot(getSystem(), name, getMasterConfig(), builderFunction);
    }

    public interface InitBuilder extends Serializable {
        void build(ActorKeyAggregation self, ActorBehaviorBuilderKeyAggregation builder);
    }

    public static class ActorKeyAggregationOneShot extends ActorKeyAggregation {
        protected InitBuilder builder;
        public ActorKeyAggregationOneShot(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        public ActorKeyAggregationOneShot(ActorSystem system, String name, Config config, InitBuilder builder) {
            super(system, name, config);
        }

        @Override
        protected void initSerializedInternalState(Serializable s) {
            builder = (InitBuilder) s;
        }

        @Override
        protected Serializable toSerializableInternalState() {
            return builder;
        }

        @Override
        protected ActorBehavior initBehavior() {
             ActorBehaviorBuilderKeyAggregation b = behaviorBuilder();
             builder.build(this, b);
             return b.build();
        }
    }
}
