package csl.actor.kelp;

import csl.actor.*;
import csl.actor.cluster.ActorPlacement;
import csl.actor.cluster.ClusterDeployment;
import csl.actor.remote.ActorAddress;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class ActorPlacementKelp<ConfigType extends ConfigKelp> extends ClusterDeployment.ActorPlacementForCluster<ConfigType> {
    protected Set<ActorRef> stages = new HashSet<>();

    public ActorPlacementKelp(ActorSystem system) {
        super(system);
    }

    public ActorPlacementKelp(ActorSystem system, PlacementStrategy strategy) {
        super(system, strategy);
    }

    public ActorPlacementKelp(ActorSystem system, String name) {
        super(system, name);
    }

    public ActorPlacementKelp(ActorSystem system, String name, PlacementStrategy strategy) {
        super(system, name, strategy);
    }

    public static ActorPlacementKelp<?> getPlacement(ActorSystem system) {
        ActorPlacement p = ActorPlacement.getPlacement(system);
        if (p instanceof ActorPlacementKelp<?>) {
            return (ActorPlacementKelp<?>) p;
        } else {
            return null;
        }
    }

    @Override
    public Serializable toSerializable(Actor a, long num, Serializable previous, ActorAddress.ActorAddressRemoteActor target) {
        if (previous != null) {
            return previous;
        } else if (a instanceof ActorKelp) {
            return ((ActorKelp<?>) a).toSerializable();
        } else {
            return null;
        }
    }

    @Override
    protected ActorRef placeLocal(Actor a) {
        if (a instanceof ActorKelp<?>) {
            ((ActorKelp<?>) a).initRestorePlaceLocal();
        }
        return super.placeLocal(a);
    }

    @Override
    public Actor fromSerializable(Serializable s, long num) {
        if (s instanceof ActorKelpSerializable<?>) {
            try {
                ActorKelpSerializable<?> actorSrc = (ActorKelpSerializable<?>) s;
                Actor a = actorSrc.restorePlace(getSystem(), num, getConfig(actorSrc));
                a.tellMessage(new Message.MessageNone(a));
                return a;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    protected ConfigType getConfig(ActorKelpSerializable<?> actorSrc) {
        return getRemoteConfig().getOrDefault(getSelfAddress().getHostAddress(), (ConfigType) actorSrc.config);
    }

    @Override
    public void receiveConfigSet(ClusterDeployment.ConfigSet set) {
        super.receiveConfigSet(set);
        if (getSystem() instanceof ActorSystemKelp) {
            ActorSystemKelp kelp = (ActorSystemKelp) getSystem();
            ConfigKelp conf = (ConfigKelp) set.getRemoteConfig().get(kelp.getServerAddress());
            log("receiveConfigSet: %s %s", kelp, conf);
            if (conf != null) {
                kelp.setConfig(conf);
            }
        }
    }

    public static class StageGraphAdd<ConfigType extends ConfigKelp>
            implements Serializable, CallableMessage.CallableMessageConsumer<ActorPlacementKelp<ConfigType>>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        public ActorRef stageGraph;

        public StageGraphAdd() {}

        public StageGraphAdd(ActorRef stageGraph) {
            this.stageGraph = stageGraph;
        }

        @Override
        public void accept(ActorPlacementKelp<ConfigType> self) {
            self.stages.add(stageGraph);
        }
    }

    public static class StageGraphRemove<ConfigType extends ConfigKelp>
            implements Serializable, CallableMessage.CallableMessageConsumer<ActorPlacementKelp<ConfigType>>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        public ActorRef stageGraph;

        public StageGraphRemove() {}

        public StageGraphRemove(ActorRef stageGraph) {
            this.stageGraph = stageGraph;
        }

        @Override
        public void accept(ActorPlacementKelp<ConfigType> self) {
            self.stages.remove(stageGraph);
        }
    }

    public static class StageGraphGet<ConfigType extends ConfigKelp>
            implements Serializable, CallableMessage<ActorPlacementKelp<ConfigType>, Set<ActorRef>>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;

        public StageGraphGet() {}

        @Override
        public Set<ActorRef> call(ActorPlacementKelp<ConfigType> self) {
            return new HashSet<>(self.stages);
        }
    }
}
