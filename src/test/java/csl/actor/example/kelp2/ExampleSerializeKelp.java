package csl.actor.example.kelp2;

import csl.actor.*;
import csl.actor.cluster.PersistentFileManager;
import csl.actor.example.ExampleSerialize;
import csl.actor.kelp.*;
import csl.actor.kelp2.ActorRefShuffle;
import csl.actor.kelp2.ActorSystemKelp;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.KryoBuilder;

import java.util.*;
import java.util.stream.Collectors;

public class ExampleSerializeKelp extends ExampleSerialize {
    public static void main(String[] args) throws Exception {
        new ExampleSerializeKelp().run();
    }
    KryoBuilder.SerializerPoolDefault p;
    ActorSystemDefault system;
    public void run() {
        system = new ActorSystemKelp.ActorSystemDefaultForKelp();
        p = new KryoBuilder.SerializerPoolDefault(system);

        writeRead(p, new ActorRefShuffle(system,
                new HashMap<>(), new ArrayList<>(), 10, true));

        system.close();
    }

}
