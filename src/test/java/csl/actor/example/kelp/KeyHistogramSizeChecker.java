package csl.actor.example.kelp;

import csl.actor.Actor;
import csl.actor.cluster.ResponsiveCalls;
import csl.actor.kelp.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KeyHistogramSizeChecker {

    public static void analyzeSize(String method, KeyHistogramSizeChecker.SizeInfo before, KeyHistogramSizeChecker.SizeInfo after) {
        boolean bi = before.hasInvalid();
        boolean ai = after.hasInvalid();
        long bs = before.sumBottom();
        long as = after.sumBottom();
        if (bi || ai || bs != as || before.size != after.size) {

            System.err.println("### " + method + " before hasInvalid=" + bi + String.format(" sumBottom=%,d  size=%,d", bs, before.size));
            before.print(0);
            System.err.println("### " + method + " after hasInvalid=" + ai + String.format(" sumBottom=%,d  size=%,d", as, after.size));
            after.print(0);
        }
    }

    public static KeyHistogramSizeChecker.SizeInfo checkSize(Actor self, KelpRoutingSplit sp, String path) {
        if (sp instanceof KelpRoutingSplit.RoutingSplitNode) {
            return new KeyHistogramSizeChecker.SizeInfo(path,
                    checkSize(self, ((KelpRoutingSplit.RoutingSplitNode) sp).getLeft(), path + "/l"),
                    checkSize(self, ((KelpRoutingSplit.RoutingSplitNode) sp).getRight(), path + "/r"));
        } else if (sp instanceof KelpRoutingSplit.RoutingSplitLeaf) {
            try {
                return ResponsiveCalls.sendTask(self.getSystem(),
                        ((KelpRoutingSplit.RoutingSplitLeaf) sp).getActor(),
                        (a) -> checkSize(a, path)).get(10, TimeUnit.SECONDS);
            } catch (Exception ex) {
                return new KeyHistogramSizeChecker.SizeInfo(path + "<ERROR:" + ex + ">");
            }
        } else {
            return new KeyHistogramSizeChecker.SizeInfo(path);
        }
    }

    public static KeyHistogramSizeChecker.SizeInfo checkSizeStart(ActorKelp<?> self) {
        try {
            Thread.sleep(self.traverseDelayTimeMs() + 100);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return checkSize(self, self.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(self)));
    }

    static KeyHistogramSizeChecker.SizeInfo checkSize(Actor a, String path) {
        if (a instanceof ActorKelp) {
            List<KeyHistogramSizeChecker.SizeInfo> cs = new ArrayList<>();

            List<MailboxKelp.HistogramEntry> es = ((ActorKelp<?>) a).getMailboxAsKelp().getEntries();
            IntStream.range(0, es.size()).mapToObj(i ->
                    checkSize(es.get(i).getTree(), path + "/sp" + i))
                    .forEach(cs::add);

            if (((ActorKelp<?>) a).getState() instanceof KelpStateRouter) {
                KelpStateRouter r = (KelpStateRouter) ((ActorKelp<?>) a).getState();
                cs.add(checkSize(a, r.getSplit(), path + "/state"));
            }

            return new KeyHistogramSizeChecker.SizeInfo(path, cs);
        } else {
            return new KeyHistogramSizeChecker.SizeInfo(path);
        }
    }

    public static KeyHistogramSizeChecker.SizeInfo checkSize(KeyHistograms.HistogramTree tree, String path) {
        KeyHistogramSizeChecker.SizeInfo i = new KeyHistogramSizeChecker.SizeInfo(path, tree.getTreeSize());

        i.children.add(checkSize(tree.getRoot(), path + "/root"));

        return i;
    }

    public static KeyHistogramSizeChecker.SizeInfo checkSize(KeyHistograms.HistogramNode node, String path) {
        if (node instanceof KeyHistograms.HistogramNodeTree) {
            long s = node.size();
            KeyHistogramSizeChecker.SizeInfo info = new KeyHistogramSizeChecker.SizeInfo(path, s);
            List<KeyHistograms.HistogramNode> ns = ((KeyHistograms.HistogramNodeTree) node).getChildren();
            IntStream.range(0, ns.size())
                    .mapToObj(i -> checkSize(ns.get(i), path + "/n" + i))
                    .forEach(info.children::add);
            return info;
        } else if (node instanceof KeyHistograms.HistogramNodeLeaf) {
            KeyHistogramSizeChecker.SizeInfo info = new KeyHistogramSizeChecker.SizeInfo(path, node.size());

            List<KeyHistograms.HistogramLeafList> ls = ((KeyHistograms.HistogramNodeLeaf) node).getStructList();
            IntStream.range(0, ls.size())
                    .mapToObj(i -> new KeyHistogramSizeChecker.SizeInfo(path + "/v" + i, ls.get(i).count()))
                    .forEach(info.children::add);

            return info;
        } else {
            return new KeyHistogramSizeChecker.SizeInfo(path);
        }
    }

    public static class SizeInfo implements Serializable {
        public static final long serialVersionUID = 1L;
        public String key;
        public long size;
        public List<SizeInfo> children;

        public SizeInfo(String key, long size) {
            this.key = key;
            this.size = size;
            this.children = new ArrayList<>();
        }

        public SizeInfo(String key, SizeInfo... children) {
            this(key, Arrays.asList(children));
        }

        public SizeInfo(String key, List<SizeInfo> is) {
            this.key = key;
            children = is;
            size = sum();
        }

        public long sum() {
            return this.children.stream()
                    .mapToLong(s -> s.size)
                    .sum();
        }

        public boolean hasInvalid() {
            return (size != sum() && !children.isEmpty()) ||
                    children.stream().anyMatch(SizeInfo::hasInvalid);
        }

        public void print(int indent) {
            String is = IntStream.range(0, indent).mapToObj(i -> "   ")
                    .collect(Collectors.joining());
            long s = sum();
            if (indent == 0 || (size != s && !children.isEmpty()) || (size != sumBottom() && !children.isEmpty())) {
                System.err.println(is + "" + key + "[" + children.size() + "] : " +
                        String.format("%,d", size) + (!children.isEmpty() && size != s ? String.format(" != %,d <INVALID>", s) : ""));
                children.forEach(c -> c.print(indent + 1));
            }
        }

        public long sumBottom() {
            if (children.isEmpty()) {
                return size;
            } else {
                return children.stream()
                        .mapToLong(SizeInfo::sumBottom)
                        .sum();
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static class StateSplitRouterSizeDebug extends KelpStateRouter {
        @Override
        public void split(ActorKelp self, int height) {
            SizeInfo before = checkSizeStart(self);
            super.split(self, height);
            SizeInfo after = checkSizeStart(self);
            analyzeSize("split", before, after);
        }

        @Override
        public void splitOrMerge(ActorKelp self, int height) {
            SizeInfo before = checkSizeStart(self);
            super.splitOrMerge(self, height);
            SizeInfo after = checkSizeStart(self);
            analyzeSize("splitOrMerge", before, after);
        }

        @Override
        public void mergeInactive(ActorKelp self) {
            SizeInfo before = checkSizeStart(self);
            super.mergeInactive(self);
            SizeInfo after = checkSizeStart(self);
            analyzeSize("mergeInactive", before, after);
        }

    }
}
