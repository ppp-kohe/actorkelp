package csl.actor.example.kelp;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystemDefault;
import csl.actor.kelp.ActorBehaviorBuilderKelp;
import csl.actor.kelp.KeyHistograms;
import csl.actor.kelp.KeyHistogramsPersistable;
import csl.actor.cluster.MailboxPersistable;
import csl.actor.remote.KryoBuilder;

import java.io.ByteArrayInputStream;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.function.BiPredicate;

public class ExampleKeyHistogramsPersistableTest {
    public static void main(String[] args) {
        new ExampleKeyHistogramsPersistableTest().runCheckingKryoInput();
        new ExampleKeyHistogramsPersistableTest().runPersistTree();
        new ExampleKeyHistogramsPersistableTest().runPersistLargeLeaves();
        new ExampleKeyHistogramsPersistableTest().runAuto();
        new ExampleKeyHistogramsPersistableTest().runAutoSerialize();
    }

    private long fail;

    public void runCheckingKryoInput() {
        Output out = new Output(100, Integer.MAX_VALUE); //if no stream supplied, the output limits to its buffer size

        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < 100; ++i) {
            buf.append("Hello");
        }
        String data = buf.toString(); //500

        KryoBuilder.SerializerPoolDefault pool = new KryoBuilder.SerializerPoolDefault(null);
        out.reset();
        pool.write(out, data);
        pool.write(out, data);
        out.flush();
        System.err.println(String.format("after write: pos=%,d total=%,d capacity=%,d",
                out.position(), out.total(), out.getBuffer().length));

        ByteArrayInputStream in = new ByteArrayInputStream(out.getBuffer(), 0, out.position());
        Input input = new Input(in, 10);
        print(input, "");
        Object o = pool.read(input);
        print(input, o);
        o = pool.read(input);
        print(input, o);
    }

    private void print(Input input, Object data) {
        System.err.println(String.format("Input: pos=%,d lim=%,d total=%,d capacity=%,d",
                input.position(), input.limit(), input.total(), input.getBuffer().length));
        System.err.println("> " + data);
    }

    public void runPersistTree() {
        System.err.println("------------- runPersistTree");
        KeyHistogramsPersistable kh = new KeyHistogramsPersistable(new Conf(Long.MAX_VALUE), new MailboxPersistable.PersistentFileManager("target/debug-persist",
                new KryoBuilder.SerializerPoolDefault(null), Paths::get, new ActorSystemDefault.SystemLoggerErr()));
        KeyHistogramsPersistable.HistogramTreePersistable tree = kh.create(new ActorBehaviorBuilderKelp.KeyComparatorDefault<>(), 3);

        KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
        input(tree, ctx);

//        try (ActorSystemDefault sys = new ActorSystemDefault()) {
//            ActorToGraph save = new ActorToGraph(sys, new File("target/debug-persist/tree.dot"), null)
//                    .setSaveLeafNode(true);
//            save.save(null, tree, 0);
//            save.finish();
//        }


        tree.persistTree(tree.getTreeSize());
        check(tree, ctx);
    }
    public void runPersistLargeLeaves() {
        System.err.println("------------- runPersistLargeLeaves");
        KeyHistogramsPersistable kh = new KeyHistogramsPersistable(new Conf(Long.MAX_VALUE), new MailboxPersistable.PersistentFileManager("target/debug-persist",
                new KryoBuilder.SerializerPoolDefault(null), Paths::get, new ActorSystemDefault.SystemLoggerErr()));
        KeyHistogramsPersistable.HistogramTreePersistable tree = kh.create(new ActorBehaviorBuilderKelp.KeyComparatorDefault<>(), 3);

        KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
        input(tree, ctx);

        tree.persistLargeLeaves();
        check(tree, ctx);
    }

    public void runAuto() {
        System.err.println("------------- runAuto");
        KeyHistogramsPersistable kh = new KeyHistogramsPersistable(new Conf(1000), new MailboxPersistable.PersistentFileManager("target/debug-persist",
                new KryoBuilder.SerializerPoolDefault(null), Paths::get, new ActorSystemDefault.SystemLoggerErr()));
        KeyHistogramsPersistable.HistogramTreePersistable tree = kh.create(new ActorBehaviorBuilderKelp.KeyComparatorDefault<>(), 3);

        KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
        input(tree, ctx);

//        try (ActorSystemDefault sys = new ActorSystemDefault()) {
//            ActorToGraph save = new ActorToGraph(sys, new File("target/debug-persist/tree.dot"), null)
//                    .setSaveLeafNode(true);
//            save.save(null, tree, 0);
//            save.finish();
//        }


        check(tree, ctx);
    }

    public void runAutoSerialize() {
        System.err.println("------------- runAutoSerialize");
        KryoBuilder.SerializerPool sp = new KryoBuilder.SerializerPoolDefault(null);
        KeyHistogramsPersistable kh = new KeyHistogramsPersistable(new Conf(1000), new MailboxPersistable.PersistentFileManager("target/debug-persist",
                sp, Paths::get, new ActorSystemDefault.SystemLoggerErr()));
        KeyHistogramsPersistable.HistogramTreePersistable tree = kh.create(new ActorBehaviorBuilderKelp.KeyComparatorDefault<>(), 3);

        KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
        input(tree, ctx);

//        try (ActorSystemDefault sys = new ActorSystemDefault()) {
//            ActorToGraph save = new ActorToGraph(sys, new File("target/debug-persist/tree.dot"), null)
//                    .setSaveLeafNode(true);
//            save.save(null, tree, 0);
//            save.finish();
//        }

        Output out = new Output(1_000_000);
        sp.write(out, tree);
        long n = out.total();
        byte[] data = out.getBuffer();
        System.err.println(String.format("persisted: %,d bytes", n));
        Input in = new Input(data, 0, (int) n);
        KeyHistogramsPersistable.HistogramTreePersistable p = (KeyHistogramsPersistable.HistogramTreePersistable) sp.read(in);
        p.init(kh.getPersistent());
        check(p, ctx);
    }


    private void input(KeyHistogramsPersistable.HistogramTreePersistable tree, KeyHistograms.HistogramPutContextMap ctx) {
        ctx.putPosition = 0;
        ctx.putRequiredSize = 2;
        String key = "abcdefghik";
        boolean consumed = false;
        for (int i = 0; i < 10_000; ++i) {
            String k = "" + key.charAt(i % key.length());
            ctx.putValue = k + (i / key.length());
            tree.put(k, ctx);
            if (!consumed && tree.getPersistedSize() > 0) {
                consumed = travConsume(tree, tree.getRoot(), ctx);
            }
        }

        log("input j");
        for (int i = 0; i < 10_000; ++i) {
            ctx.putValue = "j" + i;
            tree.put("j", ctx);
        }
    }

    private boolean travConsume(KeyHistogramsPersistable.HistogramTreePersistable tree, KeyHistograms.HistogramNode node,
                             KeyHistograms.HistogramPutContextMap ctx) {
        if (node instanceof KeyHistograms.HistogramNodeTree) {
            for (KeyHistograms.HistogramNode n : ((KeyHistograms.HistogramNodeTree) node).getChildren()) {
                if (n.keyIn(tree.getComparator(), "a") == 0) {
                    return travConsume(tree, n, ctx);
                }
            }
            return false;
        } else if (node instanceof KeyHistogramsPersistable.HistogramNodeLeafOnStorage) {
            KeyHistograms.HistogramNodeLeaf l = ((KeyHistogramsPersistable.HistogramNodeLeafOnStorage) node).load(ctx);
            if (l.size() > 3) {
                ctx.take(tree, l);
                return true;
            }
        }
        return false;
    }

    private void check(KeyHistogramsPersistable.HistogramTreePersistable tree, KeyHistograms.HistogramPutContextMap ctx) {
        //loading
        log(String.format("persisted: %,d / %,d", tree.getPersistedSize(), tree.getTreeSize()));
        load(tree, tree.getRoot(), ctx);
        check("persisted after read", tree.getPersistedSize(), 0L, Objects::equals);
        check("size after read", tree.getTreeSize(), 0L, Objects::equals);
        if (fail == 0) {
            System.err.println(formatColor(76, "[OK]"));
        }
    }

    public void load(KeyHistograms.HistogramTree tree, KeyHistograms.HistogramNode node, KeyHistograms.HistogramPutContext context) {
        if (node instanceof KeyHistograms.HistogramNodeTree) {
            for (KeyHistograms.HistogramNode ch : ((KeyHistograms.HistogramNodeTree) node).getChildren()) {
                load(tree, ch, context);
            }
        } else if (node instanceof KeyHistograms.HistogramNodeLeaf) {
            if (node instanceof KeyHistogramsPersistable.HistogramNodeLeafOnStorage) {
                try {
                    node = ((KeyHistogramsPersistable.HistogramNodeLeafOnStorage) node).load(context);
                } catch (Exception ex) {
                    System.err.println("error: " + node);
                    throw ex;
                }
            }
            KeyHistograms.HistogramNodeLeafMap leaf = (KeyHistograms.HistogramNodeLeafMap) node;
            KeyHistograms.HistogramLeafList list = leaf.getStructList().get(0);
            long nodeSize = node.size();
            long size = 0;
            int prevNum = -1;
            while (!list.isEmpty()) {
                Object o = leaf.take(1, tree)[0];
                if (o == null) {
                    break;
                }
                check(node + " key " + size, "" + o.toString().charAt(0), node.keyStart().toString(), Objects::equals);
                int num = Integer.parseInt(o.toString().substring(1));
                check(node + " value " + size, num, prevNum, (next,prev) -> next > prev);
                prevNum = num;
                ++size;
            }
            check(node + " size", size, nodeSize, Objects::equals);
        }
    }
    private <E> void check(String msg, E r, E obj, BiPredicate<E, E> p) {
        if (p.test(r, obj)) {
            //System.out.println(formatColor(76, "[OK]"));
        } else {
            ++fail;
            System.err.println(msg + " : " + formatColor(196, "DIFF") + " : " + r + " vs " + obj);
        }
    }

    private String formatColor(int c, String s) {
        return String.format("\033[38;5;%dm%s\033[0m",c, s);
    }

    public void log(String str) {
        System.err.println(str);
    }

    static class Conf implements KeyHistogramsPersistable.HistogramTreePersistableConfig {
        long limit;

        public Conf(long limit) {
            this.limit = limit;
        }

        @Override
        public long histogramPersistSizeLimit() {
            return limit;
        }

        @Override
        public long histogramPersistOnMemorySize() {
            return 3;
        }

        @Override
        public long histogramPersistRandomSeed() {
            return 1235;
        }
    }
}
