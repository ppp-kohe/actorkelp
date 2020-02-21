package csl.actor.example.msgassoc;

import csl.actor.ActorSystemDefault;
import csl.actor.example.delayedlabel.ActorToGraph;
import csl.actor.msgassoc.*;
import csl.actor.remote.KryoBuilder;

import java.io.File;
import java.util.Objects;
import java.util.function.BiPredicate;

public class KeyHistogramsPersistableTest {
    public static void main(String[] args) {
        new KeyHistogramsPersistableTest().run();
    }

    public void run() {
        KeyHistogramsPersistable kh = new KeyHistogramsPersistable(new Conf(), new MailboxPersistable.PersistentFileManager("target/debug-persist",
                new KryoBuilder.SerializerPoolDefault(null)));
        KeyHistogramsPersistable.HistogramTreePersistable tree = kh.create(new ActorBehaviorBuilderKeyValue.KeyComparatorDefault<>(), 3);

        KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
        ctx.putPosition = 0;
        ctx.putRequiredSize = 2;
        String key = "abcdefghik";
        for (int i = 0; i < 10_000; ++i) {
            String k = "" + key.charAt(i % key.length());
            ctx.putValue = k + (i / key.length());
            tree.put(k, ctx);
        }

        log("input j");
        for (int i = 0; i < 10_000; ++i) {
            ctx.putValue = "j" + i;
            tree.put("j", ctx);
        }

//        try (ActorSystemDefault sys = new ActorSystemDefault()) {
//            ActorToGraph save = new ActorToGraph(sys, new File("target/debug-persist/tree.dot"), null)
//                    .setSaveLeafNode(true);
//            save.save(null, tree, 0);
//            save.finish();
//        }


        tree.persistTree();

        //loading
        log(String.format("persisted: %,d", tree.getPersistedSize()));
        load(tree, tree.getRoot(), ctx);
        check("persisted after read", tree.getPersistedSize(), 0L, Objects::equals);
        check("size after read", tree.getTreeSize(), 0L, Objects::equals);
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
        @Override
        public long histogramPersistSizeLimit() {
            return Long.MAX_VALUE;
        }
    }
}
