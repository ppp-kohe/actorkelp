package csl.actor.example;

import csl.actor.*;
import csl.actor.cluster.MailboxPersistable;
import csl.actor.cluster.PersistentFileManager;
import csl.actor.kelp.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.KryoBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ExampleSerialize2 extends ExampleSerialize {
    public static void main(String[] args) throws Exception {
        new ExampleSerialize2().run();
    }
    KryoBuilder.SerializerPoolDefault p;
    ActorSystemDefault system;
    public void run() {
        system = new ActorSystemDefault();
        p = new KryoBuilder.SerializerPoolDefault(system);

        runNodeTreeData();
        runStateLeaf();
        runHistogramLeafList();
        runActorAddressRemote();
        runActorAddressRemoteActor();
        runCallableMessage();
        runCallableFailure();
        runHistogramTree();
        runActorReplicableSerializableState();

        system.close();
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
        ActorKelp.StateUnit leaf = new ActorKelp.StateUnit(a);
        writeRead(p, leaf, (s,c) -> c.getRouter().asLocal().equals(a));
    }

    private void runHistogramLeafList() {
        KeyHistograms.HistogramLeafList l = new KeyHistograms.HistogramLeafList();
        l.add(null, "hello");
        l.add(null, 12345L);
        for (int i = 0; i < 1000; i++) {
            l.add(null, i);
        }
        writeRead(p, l, false, (a,b) ->
                a.count() == b.count() &&
                toList(a).equals(toList(b)));
    }

    private List<Object> toList(KeyHistograms.HistogramLeafList l) {
        List<Object> os = new ArrayList<>();
        l.iterator().forEachRemaining(os::add);
        return os;
    }

    private void runActorAddressRemote() {
        writeRead(p, ActorAddress.ActorAddressRemote.get("hello", 12345));
    }

    private void runActorAddressRemoteActor() {
        writeRead(p, ActorAddress.ActorAddressRemote.get("hello", 12345, "world"));
    }

    private void runCallableMessage() {
        String data = this.toString();
        writeRead(p, CallableMessage.callableMessage((a) -> "HELLO" + data), false,
                (src,dst) ->
                src.call(null, null).equals(dst.call(null, null)));
    }

    private void runCallableFailure() {
        RuntimeException e = new RuntimeException("error");
        e.fillInStackTrace();
        writeRead(p, new CallableMessage.CallableFailure(e), (a,b) ->
                a.getError().toString().equals(b.getError().toString()));
    }

    private void runHistogramTree() {
        KeyHistograms.HistogramTree tree = new KeyHistograms.HistogramTree(null,
                new ActorBehaviorBuilderKelp.KeyComparatorDefault<>(), 10,
                PersistentFileManager.getPersistentFile(null, () -> ""));

        List<Object> os = values();
        int i = 0;
        for (Object o : os) {
            String key = valueToKey(o, i);
            KeyHistograms.HistogramPutContext ctx = new KeyHistograms.HistogramPutContextMap();
            ctx.putValue = o;
            ctx.putTree = tree;
            ctx.putPosition = 0;
            ctx.putRequiredSize = 0;
            tree.put(key, ctx);
            ++i;
        }
        writeRead(p, tree, false, (a,b) -> checkTree(a, "pre") && checkTree(b, "post"));
    }

    String keyString = "abcdefghijklmnopqrstuw";

    public List<Object> values() {
        List<Object> os = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            String key = "" + keyString.charAt(i % keyString.length());
            os.add(key + "-" + i);
        }
        return os;
    }

    public String valueToKey(Object o, int i) {
        String k = o.toString();
        return "" + k.charAt(0);
    }

    public boolean checkTree(Object o, String s) {
        List<Boolean> rs = new ArrayList<>();
        KeyHistograms.HistogramTree tree = (KeyHistograms.HistogramTree) o;
        KeyHistograms.HistogramNodeTree node1 = (KeyHistograms.HistogramNodeTree) tree.getRoot();
        rs.add(checkTree(s, node1, "a", "w", 100L, 2, 4, null));

        KeyHistograms.HistogramNodeTree node2 = (KeyHistograms.HistogramNodeTree) node1.getChildren().get(0);
        rs.add(checkTree(s, node2, "a", "e", 25L, 1, 5, node1));
        rs.add(checkLeaf(s, node2.getChildren().get(0), "a", 0, node2, Arrays.asList("a-0", "a-22", "a-44", "a-66", "a-88")));
        rs.add(checkLeaf(s, node2.getChildren().get(1), "b", 0, node2, Arrays.asList("b-1", "b-23", "b-45", "b-67", "b-89")));
        rs.add(checkLeaf(s, node2.getChildren().get(2), "c", 0, node2, Arrays.asList("c-2", "c-24", "c-46", "c-68", "c-90")));
        rs.add(checkLeaf(s, node2.getChildren().get(3), "d", 0, node2, Arrays.asList("d-3", "d-25", "d-47", "d-69", "d-91")));
        rs.add(checkLeaf(s, node2.getChildren().get(4), "e", 0, node2, Arrays.asList("e-4", "e-26", "e-48", "e-70", "e-92")));

        KeyHistograms.HistogramNodeTree node3 = (KeyHistograms.HistogramNodeTree) node1.getChildren().get(1);
        rs.add(checkTree(s, node3, "f", "j", 25L, 1, 5, node1));
        rs.add(checkLeaf(s, node3.getChildren().get(0), "f", 0, node3, Arrays.asList("f-5", "f-27", "f-49", "f-71", "f-93")));
        rs.add(checkLeaf(s, node3.getChildren().get(1), "g", 0, node3, Arrays.asList("g-6", "g-28", "g-50", "g-72", "g-94")));
        rs.add(checkLeaf(s, node3.getChildren().get(2), "h", 0, node3, Arrays.asList("h-7", "h-29", "h-51", "h-73", "h-95")));
        rs.add(checkLeaf(s, node3.getChildren().get(3), "i", 0, node3, Arrays.asList("i-8", "i-30", "i-52", "i-74", "i-96")));
        rs.add(checkLeaf(s, node3.getChildren().get(4), "j", 0, node3, Arrays.asList("j-9", "j-31", "j-53", "j-75", "j-97")));

        KeyHistograms.HistogramNodeTree node4 = (KeyHistograms.HistogramNodeTree) node1.getChildren().get(2);
        rs.add(checkTree(s, node4, "k", "o", 22L, 1, 5, node1));
        rs.add(checkLeaf(s, node4.getChildren().get(0), "k", 0, node4, Arrays.asList("k-10", "k-32", "k-54", "k-76", "k-98")));
        rs.add(checkLeaf(s, node4.getChildren().get(1), "l", 0, node4, Arrays.asList("l-11", "l-33", "l-55", "l-77", "l-99")));
        rs.add(checkLeaf(s, node4.getChildren().get(2), "m", 0, node4, Arrays.asList("m-12", "m-34", "m-56", "m-78")));
        rs.add(checkLeaf(s, node4.getChildren().get(3), "n", 0, node4, Arrays.asList("n-13", "n-35", "n-57", "n-79")));
        rs.add(checkLeaf(s, node4.getChildren().get(4), "o", 0, node4, Arrays.asList("o-14", "o-36", "o-58", "o-80")));

        KeyHistograms.HistogramNodeTree node5 = (KeyHistograms.HistogramNodeTree) node1.getChildren().get(3);
        rs.add(checkTree(s, node5, "p", "w", 28L, 1, 7, node1));
        rs.add(checkLeaf(s, node5.getChildren().get(0), "p", 0, node5, Arrays.asList("p-15", "p-37", "p-59", "p-81")));
        rs.add(checkLeaf(s, node5.getChildren().get(1), "q", 0, node5, Arrays.asList("q-16", "q-38", "q-60", "q-82")));
        rs.add(checkLeaf(s, node5.getChildren().get(2), "r", 0, node5, Arrays.asList("r-17", "r-39", "r-61", "r-83")));
        rs.add(checkLeaf(s, node5.getChildren().get(3), "s", 0, node5, Arrays.asList("s-18", "s-40", "s-62", "s-84")));
        rs.add(checkLeaf(s, node5.getChildren().get(4), "t", 0, node5, Arrays.asList("t-19", "t-41", "t-63", "t-85")));
        rs.add(checkLeaf(s, node5.getChildren().get(5), "u", 0, node5, Arrays.asList("u-20", "u-42", "u-64", "u-86")));
        rs.add(checkLeaf(s, node5.getChildren().get(6), "w", 0, node5, Arrays.asList("w-21", "w-43", "w-65", "w-87")));

        rs.add(check(s, 22L, tree.getLeafSize()));
        rs.add(check(s, 22L, tree.getLeafSizeNonZero()));

        return rs.stream().allMatch(b -> b);
    }

    private boolean check(String label, Object a, Object b) {
        boolean e = Objects.equals(a, b);
        if (e) {
            return true;
        } else {
            System.err.println("error: " + label + ": " + a + " vs " + b);
            return false;
        }
    }

    private boolean checkTree(String label, KeyHistograms.HistogramNodeTree node, String from, String to, long size, int height,
                              int children, KeyHistograms.HistogramNodeTree parent) {
        return check(label, from, node.keyStart()) &&
                check(label, to, node.keyEnd()) &&
                check(label, size, node.size()) &&
                check(label, height, node.height()) &&
                check(label, children, node.getChildren().size()) &&
                check(label, parent, node.getParent());
    }

    private boolean checkLeaf(String label, KeyHistograms.HistogramNode node, String key, int height,
                              KeyHistograms.HistogramNode parent, List<Object> list) {
        KeyHistograms.HistogramNodeLeaf leaf = (KeyHistograms.HistogramNodeLeaf) node;
        return check(label, key, leaf.getKey()) &&
                check(label, height, leaf.height()) &&
                check(label, parent, leaf.getParent()) &&
                check(label, (long) list.size(), leaf.size()) &&
                check(label, list, toList(leaf.getStructList().get(0)));
    }

    private void runActorReplicableSerializableState() {
        Config c = new Config();
        c.mailboxThreshold = 123456;
        c.mailboxTreeSize = 10;
        MyActor a = new MyActor(system, "hello", c);
        a.getMailbox().offer(new Message<>(a, null, "msg1"));
        a.getMailbox().offer(new Message<>(a, null, "msg2"));
        a.getMailbox().offer(new Message<>(a, null, "msg3"));
        MailboxKelp.HistogramEntry e = a.getMailboxAsKelp().getEntries().get(0);


        for (Object o : values()) {
            e.getProcessor().process(a, new Message<>(a, null, o));
        }
        checkTree(e.getTree(), "actor-construction");

        ActorKelp.ActorKelpSerializable s = a.toSerializable(123);
        writeRead(p, s, false, (pre,post) ->
                check("post.name", MyActor.class, post.actorType) &&
                check("post.name", "hello", post.name) &&
                check("post.size", 1, post.histograms.size()) &&
                check("post.mailbox.data", Arrays.asList("msg1", "msg2", "msg3"),
                        Arrays.stream(post.messages).map(Message::getData).collect(Collectors.toList())) &&
                check("post.mailbox.tgt", Arrays.asList(a, a, a),
                                Arrays.stream(post.messages).map(Message::getTarget)
                                        .map(system::resolveActor)
                                        .collect(Collectors.toList())) &&
                checkTree(post.histograms.get(0), "post.tree") &&
                check("post.config", 123456, post.config.mailboxThreshold));
    }

    public static class MyActor extends ActorKelp<MyActor> {
        public MyActor(ActorSystem system, String name, Config config) {
            super(system, name, config);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(String.class, s -> "" + s.charAt(0))
                    .reduce((k,ls) -> ls)
                    .forEachKeyList(0, (k,vs) -> System.out.println(vs))
                    .build();
        }
    }
}
