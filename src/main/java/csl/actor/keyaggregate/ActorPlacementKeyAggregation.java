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
}
