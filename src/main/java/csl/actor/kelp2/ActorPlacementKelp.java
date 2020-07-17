package csl.actor.kelp2;

import csl.actor.Actor;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.cluster.ClusterDeployment;
import csl.actor.remote.ActorAddress;

import java.io.Serializable;

public class ActorPlacementKelp extends ClusterDeployment.ActorPlacementForCluster<ConfigKelp> {
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
    public Actor fromSerializable(Serializable s, long num) {
        if (s instanceof ActorKelp.ActorKelpSerializable<?>) {
            try {
                ActorKelp.ActorKelpSerializable<?> actorSrc = (ActorKelp.ActorKelpSerializable<?>) s;
                Actor a = actorSrc.restore(getSystem(), num, getConfig(actorSrc));
                a.tellMessage(new Message.MessageNone(a));
                return a;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return null;
    }

    protected ConfigKelp getConfig(ActorKelp.ActorKelpSerializable<?> actorSrc) {
        return getRemoteConfig().getOrDefault(getSelfAddress().getHostAddress(), actorSrc.config);
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
}
