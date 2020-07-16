package csl.actor.kelp2;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.cluster.ActorSystemCluster;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ActorSystemKelp extends ActorSystemRemote {
    protected Map<ActorAddress, Breaker> breakers;

    public ActorSystemKelp() {
        super(new ActorSystemCluster.ActorSystemDefaultForCluster(), KryoBuilder.builder());
    }

    public ActorSystemKelp(ActorSystemDefault localSystem, Function<ActorSystem, Kryo> kryoFactory) {
        super(localSystem, kryoFactory);
    }

    @Override
    protected void init() {
        super.init();
        breakers = new ConcurrentHashMap<>();
    }

    public static class Breaker {

    }
}
