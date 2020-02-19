package csl.actor.example;

import csl.actor.ActorRefLocalNamed;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.msgassoc.ActorAggregationReplicable;
import csl.actor.msgassoc.KeyHistograms;
import csl.actor.msgassoc.KeyHistogramsPersistable;
import csl.actor.remote.KryoBuilder;

import java.util.ArrayList;
import java.util.List;

public class SerializeExample2 extends SerializeExample{
    public static void main(String[] args) throws Exception {
        new SerializeExample2().run();
    }
    KryoBuilder.SerializerPoolDefault p;
    ActorSystemDefault system;
    public void run() {
        system = new ActorSystemDefault();
        p = new KryoBuilder.SerializerPoolDefault(system);

        runNodeTreeData();
        runStateLeaf();
        runHistogramLeafList();
    }

    private void runNodeTreeData() {
        KeyHistogramsPersistable.NodeTreeData d = new KeyHistogramsPersistable.NodeTreeData();
        d.height = 123;
        d.leaf = true;
        d.keyStart = "hello";
        d.size = 123456789;
        d.keyEnd = "word";

        writeRead(p, d, (a,b) ->
                a != b &&
                a.height == b.height &&
                        a.leaf == b.leaf &&
                        a.keyStart.equals(b.keyStart) &&
                        a.size == b.size &&
                        a.keyEnd.equals(b.keyEnd)
        );
    }

    private void runStateLeaf() {
        ExampleActor a = new ExampleActor(system, "a");
        ActorAggregationReplicable.StateLeaf leaf = new ActorAggregationReplicable.StateLeaf(a);
        writeRead(p, leaf, (s,c) -> c.getRouter().asLocal().equals(a));
    }

    private void runHistogramLeafList() {
        KeyHistograms.HistogramLeafList l = new KeyHistograms.HistogramLeafList();
        l.add(null, "hello");
        l.add(null, 12345L);
        for (int i = 0; i < 1000; i++) {
            l.add(null, i);
        }
        writeRead(p, l, (a,b) ->
                a.count() == b.count() &&
                toList(a).equals(toList(b)));
    }

    private List<Object> toList(KeyHistograms.HistogramLeafList l) {
        List<Object> os = new ArrayList<>();
        l.iterator().forEachRemaining(os::add);
        return os;
    }
}
