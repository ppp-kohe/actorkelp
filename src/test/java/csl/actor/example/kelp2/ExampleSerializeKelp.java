package csl.actor.example.kelp2;

import csl.actor.*;
import csl.actor.example.ExampleSerialize;
import csl.actor.example.TestToolSerialize;
import csl.actor.kelp.ActorRefShuffle;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.remote.KryoBuilder;

import java.util.*;

public class ExampleSerializeKelp extends ExampleSerialize {
    public static void main(String[] args) throws Exception {
        new ExampleSerializeKelp().run();
    }
    KryoBuilder.SerializerPoolDefault p;
    ActorSystemDefault system;
    public void run() {
        system = new ActorSystemKelp.ActorSystemDefaultForKelp();
        p = new KryoBuilder.SerializerPoolDefault(system);

        new TestToolSerialize().writeRead(p, new ActorRefShuffle(system,
                new HashMap<>(), new ArrayList<>(), 10, true));

        system.close();
    }

}
