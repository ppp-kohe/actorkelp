package csl.actor.keyaggregate;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

public class ActorPlacementKeyAggregation extends ActorPlacement.ActorPlacementDefault {
    protected Map<ActorAddress, Config> remoteConfig = new HashMap<>();
    protected BlockingQueue<ActorAddress> completed = new LinkedBlockingDeque<>();
    protected Set<ActorAddress> configSetSent = new HashSet<>();

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
    protected ActorBehavior initBehavior() {
        return behaviorBuilder()
                .matchWithSender(AddressList.class, this::receive)
                .matchWithSender(ActorCreationRequest.class, this::create)
                .match(ConfigSet.class, this::receiveConfigSet)
                .build();
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

    public Map<ActorAddress, Config> getRemoteConfig() {
        return remoteConfig;
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
                ex.printStackTrace();
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    protected ActorRef placeLocal(Actor a) {
        a.getSystem().send(new Message.MessageNone(a));
        return a;
    }

    @Override
    protected void updateTotalThreads() {
        super.updateTotalThreads();
        synchronized (this) {
            getCluster().stream()
                    .map(AddressListEntry::getPlacementActor)
                    .forEach(this::joined);
        }
    }

    protected void joined(ActorAddress addr) {
        if (!configSetSent.contains(addr)) {
            ActorRefRemote.get(getSystem(), addr)
                    .tell(new ConfigSet(remoteConfig), this);
            configSetSent.add(addr);
            completed.offer(addr);
        }
    }

    public BlockingQueue<ActorAddress> getCompleted() {
        return completed;
    }

    public void receiveConfigSet(ConfigSet set) {
        remoteConfig.putAll(set.getRemoteConfig());
    }

    public static class ConfigSet implements Serializable, KryoSerializable {
        public Map<ActorAddress, Config> remoteConfig;

        public ConfigSet() {}

        public ConfigSet(Map<ActorAddress, Config> remoteConfig) {
            this.remoteConfig = remoteConfig;
        }

        public Map<ActorAddress, Config> getRemoteConfig() {
            return remoteConfig;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            kryo.writeClassAndObject(output, remoteConfig);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void read(Kryo kryo, Input input) {
            remoteConfig = (Map<ActorAddress, Config>) kryo.readClassAndObject(input);
        }
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
                    f = f.thenCompose((_v) -> ResponsiveCalls.<ActorKeyAggregation>sendTaskConsumer(system, next, (a,s) -> {
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
                        f = f.thenCompose((_v) -> ResponsiveCalls.sendTask(system, prevActor, (a,s) ->
                                task.apply((ActorKeyAggregation) a, next)));
                    } else {
                        f = ResponsiveCalls.sendTask(system, prevActor, (a,s) ->
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

}
