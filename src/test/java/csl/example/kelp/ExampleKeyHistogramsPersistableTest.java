package csl.example.kelp;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.kelp.behavior.HistogramTreeNodeLeaf;
import csl.actor.kelp.behavior.HistogramTreeNodeTable;
import csl.actor.kelp.behavior.HistogramTree;
import csl.actor.kelp.persist.*;
import csl.example.TestTool;
import csl.actor.kelp.ActorKelpFunctions;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.kelp.behavior.KeyHistograms;
import csl.actor.persist.PersistentFileManager;
import csl.actor.remote.KryoBuilder;

import java.io.ByteArrayInputStream;
import java.nio.file.Paths;
import java.util.*;

public class ExampleKeyHistogramsPersistableTest {
    public static void main(String[] args) {
        new ExampleKeyHistogramsPersistableTest().runCheckingKryoInput();
        new ExampleKeyHistogramsPersistableTest().runCheckingList();
        new ExampleKeyHistogramsPersistableTest().runPersistList();
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

        try (ActorSystem sys = new ActorSystemKelp.ActorSystemDefaultForKelp()) {
            KryoBuilder.SerializerPool pool = new KryoBuilder.SerializerPoolDefault(sys);
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
            TestTool.assertEquals("data 1", data, o);
            o = pool.read(input);
            print(input, o);
            TestTool.assertEquals("data 2", data, o);
        }
    }

    private void print(Input input, Object data) {
        System.err.println(String.format("Input: pos=%,d lim=%,d total=%,d capacity=%,d",
                input.position(), input.limit(), input.total(), input.getBuffer().length));
        System.err.println("> " + data);
    }

    public void runCheckingList() {
        System.err.println("------------- runCheckingList");
        try (ActorSystem sys = new ActorSystemKelp.ActorSystemDefaultForKelp()) {
            KryoBuilder.SerializerPool sp = new KryoBuilder.SerializerPoolDefault(sys);
            KeyHistogramsPersistable kh = new KeyHistogramsPersistable(new Conf(Long.MAX_VALUE), new PersistentFileManager("target/debug-persist",
                    sp, Paths::get, new ActorSystemDefault.SystemLoggerErr()));
            HistogramTreePersistable tree = kh.create(new ActorKelpFunctions.KeyComparatorDefault<>(), 3);

            KeyHistograms.HistogramLeafList list = new KeyHistograms.HistogramLeafList();
            List<Object> added = listAdd(tree, list, "v", 1000);

            checkList(tree, list, added, "list");
            List<Object> vs = new ArrayList<>();
            list.iterator(tree, null).forEachRemaining(vs::add);
            TestTool.assertEquals("list iter", added, vs);
            checkList(tree, list, added, "list after iterator(tree) ");

            KeyHistograms.HistogramLeafList listCopy = list.copy();
            checkList(tree, listCopy, added, "list copy");

            List<Object> polled = listPoll(tree, list, 500);
            List<Object> addedPolled = added.subList(0, 500);
            TestTool.assertEquals("list polled", addedPolled, polled);
            List<Object> addedAfterPolled = added.subList(500, added.size());
            vs = new ArrayList<>();
            list.iterator(tree, null).forEachRemaining(vs::add);
            TestTool.assertEquals("list polled iter", addedAfterPolled, vs);

            checkList(tree, listCopy, added, "list copy after polled");

            List<Object> polledRemain = listPoll(tree, list, 500);
            TestTool.assertEquals("list polled remain", addedAfterPolled, polledRemain);

            checkList(tree, list, Collections.emptyList(), "list after all polled");
        }
    }

    private List<Object> listAdd(HistogramTree tree, KeyHistograms.HistogramLeafList list, String prefix, int size, List<Object> existing) {
        List<Object> added = new ArrayList<>(existing);
        for (int i = 0; i < size; ++i) {
            Object v = prefix + i;
            list.add(tree, v);
            added.add(v);
        }
        return added;
    }
    private List<Object> listAdd(HistogramTree tree, KeyHistograms.HistogramLeafList list, String prefix, int size) {
        return listAdd(tree, list, prefix, size, Collections.emptyList());
    }
    private List<Object> listPoll(HistogramTree tree, KeyHistograms.HistogramLeafList list, int size) {
        List<Object> added = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            added.add(list.poll(tree, null));
        }
        return added;
    }

    public void runPersistList() {
        System.err.println("------------- runPersistList");
        try (ActorSystem sys = new ActorSystemKelp.ActorSystemDefaultForKelp()) {
            KryoBuilder.SerializerPool sp = new KryoBuilder.SerializerPoolDefault(sys);
            KeyHistogramsPersistable kh = new KeyHistogramsPersistable(new Conf(Long.MAX_VALUE), new PersistentFileManager("target/debug-persist",
                    sp, Paths::get, new ActorSystemDefault.SystemLoggerErr()));
            HistogramTreePersistable tree = kh.create(new ActorKelpFunctions.KeyComparatorDefault<>(), 3);

            KeyHistograms.HistogramLeafList list = new KeyHistograms.HistogramLeafList();
            List<Object> added = listAdd(tree, list, "A", 500);
            listPersist(tree, list);
            checkList(tree, list, added, "list after persist"); //500

            List<Object> addedTotal = listAdd(tree, list, "B", 500, added);
            checkList(tree, list, addedTotal, "list after persist add"); //1000

            KeyHistograms.HistogramLeafList listCopy = list.copy();

            List<Object> storagePoll = listPoll(tree, list, 250);
            List<Object> afterStoragePoll = addedTotal.subList(0, 250);
            List<Object> afterStoragePollRemain = addedTotal.subList(250, addedTotal.size());
            TestTool.assertEquals("polled storage", afterStoragePoll, storagePoll);
            checkList(tree, list, afterStoragePollRemain, "polled storage"); //750

            List<Object> addedAfterPollAdd = listAdd(tree, list, "C", 250, afterStoragePollRemain);
            checkList(tree, list, addedAfterPollAdd, "polled storage add"); //1000

            checkList(tree, listCopy, addedTotal, "preserved copy");

            List<Object> storagePollRemain = listPoll(tree, list, 600);
            List<Object> storagePollRemainExp = addedAfterPollAdd.subList(0, 600);
            List<Object> afterStorageAllPollRemain = addedAfterPollAdd.subList(600, addedAfterPollAdd.size());
            TestTool.assertEquals("polled all storage remain", storagePollRemainExp, storagePollRemain);
            checkList(tree, list, afterStorageAllPollRemain, "polled all storage remain"); //400

            //////////////////////
            listPersist(tree, list);

            TestTool.assertEquals("maxLinkDepth", 0, ((HistogramLeafCellOnStorage) list.head).getMaxLinkDepth());
            List<Object> addedAfterRePersist = listAdd(tree, list, "D", 100, afterStorageAllPollRemain);
            checkList(tree, list, addedAfterRePersist, "re-persist1 before"); //500

            listPersist(tree, list);
            TestTool.assertEquals("maxLinkDepth", 1, ((HistogramLeafCellOnStorage) list.head).getMaxLinkDepth());
            checkList(tree, list, addedAfterRePersist, "re-persist1"); //500

            List<Object> pollAfterRePersist = listPoll(tree, list, 100);

            List<Object> pollAfterRePersistExp = addedAfterRePersist.subList(0, 100);
            List<Object> addedPollAfterRePersist = addedAfterRePersist.subList(100, addedAfterRePersist.size());
            TestTool.assertEquals("poll after re-persist1", pollAfterRePersistExp, pollAfterRePersist);
            checkList(tree, list, addedPollAfterRePersist, "re-persist1 after poll"); //400

            List<Object> addedAfterRePersist2 = listAdd(tree, list, "E", 100, addedPollAfterRePersist);
            checkList(tree, list, addedAfterRePersist2, "re-persist2 before"); //500

            listPersist(tree, list); //persist depth2

            TestTool.assertEquals("maxLinkDepth", 2, ((HistogramLeafCellOnStorage) list.head).getMaxLinkDepth());
            checkList(tree, list, addedAfterRePersist2, "re-persist2"); //500

            List<Object> addedAfterRePersist3 = listAdd(tree, list, "F", 100, addedAfterRePersist2);
            checkList(tree, list, addedAfterRePersist3, "re-persist3 before"); //600

            listPersist(tree, list); //persist depth3 (limit)

            TestTool.assertEquals("maxLinkDepth", 3, ((HistogramLeafCellOnStorage) list.head).getMaxLinkDepth());
            checkList(tree, list, addedAfterRePersist3, "re-persist3");//600

            List<Object> addedAfterRePersist4 = listAdd(tree, list, "G", 100, addedAfterRePersist3);
            checkList(tree, list, addedAfterRePersist4, "re-persist4 before"); //700

            KeyHistograms.HistogramLeafList persist3Copy = list.copy();

            listPersist(tree, list); //persist depth3 (limit -> expanded)
            TestTool.assertEquals("maxLinkDepth", 0, ((HistogramLeafCellOnStorage) list.head).getMaxLinkDepth());
            checkList(tree, list, addedAfterRePersist4, "re-persist4"); //700

            List<Object> p3cAll = listPoll(tree, persist3Copy, 700);
            List<Object> p4All = listPoll(tree, list, 700);
            TestTool.assertEquals("poll persist3 copy", addedAfterRePersist4, p3cAll);
            TestTool.assertEquals("poll persist4", addedAfterRePersist4, p4All);
            checkList(tree, persist3Copy, Collections.emptyList(), "after poll all persist3 copy");
            checkList(tree, list, Collections.emptyList(), "after poll all persist4");
        }
    }

    private void listPersist(HistogramTree tree, KeyHistograms.HistogramLeafList list) {
        PersistentFileManager.PersistentFileWriter writer = tree.getPersistent().createWriterForHead("runPersistList");
        HistogramLeafCellOnStorage.HistogramLeafCellOnStorageWriting w = HistogramLeafCellOnStorage.HistogramLeafCellOnStorageWriting
                .writeCell(tree, null, list.head, writer);
        writer.close();
        list.replaceRest(list.head, w.cell);
    }

    private void checkList(HistogramTree tree, KeyHistograms.HistogramLeafList list, List<Object> added, String msg) {
        System.err.println("\n" + msg +" :");
        TestTool.assertEquals(msg + " size", (long) added.size(), list.count());
        TestTool.assertEquals(msg + " isEmpty", added.isEmpty(), list.isEmpty());
        KeyHistograms.HistogramLeafCell cell = list.head;

        List<Object> fromCellIter = new ArrayList<>();
        int i = 0;
        while (cell != null) {
            System.err.printf("cell [%d] type=%s size=%,d sizeOnMem=%,d nonEmpty=%s hasRemain=%s %n",
                    i, cell.getClass().getSimpleName(), cell.size(), cell.sizeOnMemory(), cell.isNonEmpty(), cell.hasRemaining());
            if (cell instanceof HistogramLeafCellOnStorage) {
                HistogramLeafCellOnStorage s = (HistogramLeafCellOnStorage) cell;
                System.err.printf("   %s maxLinkDepth=%d %s%n", s.getSource(), s.getMaxLinkDepth(), s.getCurrentSegment());
            }
            cell.iterator(tree, null).forEachRemaining(fromCellIter::add);
            cell = cell.next;
            ++i;
        }
        TestTool.assertEquals(msg + " values from cellIter", added, fromCellIter);

        List<Object> fromListIter = new ArrayList<>();
        list.iterator(tree, null).forEachRemaining(fromListIter::add);
        TestTool.assertEquals(msg + " values from cellIter", added, fromListIter);
    }

    public void runPersistTree() {
        System.err.println("------------- runPersistTree");
        try (ActorSystem sys = new ActorSystemKelp.ActorSystemDefaultForKelp()) {
            KryoBuilder.SerializerPool sp = new KryoBuilder.SerializerPoolDefault(sys);
            KeyHistogramsPersistable kh = new KeyHistogramsPersistable(new Conf(Long.MAX_VALUE), new PersistentFileManager("target/debug-persist",
                    sp, Paths::get, new ActorSystemDefault.SystemLoggerErr()));
            HistogramTreePersistable tree = kh.create(new ActorKelpFunctions.KeyComparatorDefault<>(), 3);

            KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
            input(tree, ctx);

            checkTreeSize(tree, ctx);

            tree.persistTree(tree.getTreeSize());
            check(tree, ctx);

            checkTreeSize(tree, ctx);
        }
    }
    public void runPersistLargeLeaves() {
        System.err.println("------------- runPersistLargeLeaves");

        try (ActorSystem sys = new ActorSystemKelp.ActorSystemDefaultForKelp()) {
            KryoBuilder.SerializerPool sp = new KryoBuilder.SerializerPoolDefault(sys);

            KeyHistogramsPersistable kh = new KeyHistogramsPersistable(new Conf(Long.MAX_VALUE), new PersistentFileManager("target/debug-persist",
                    sp, Paths::get, new ActorSystemDefault.SystemLoggerErr()));
            HistogramTreePersistable tree = kh.create(new ActorKelpFunctions.KeyComparatorDefault<>(), 3);

            KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
            input(tree, ctx);

            tree.persistLargeLeaves();
            check(tree, ctx);

            checkTreeSize(tree, ctx);
        }
    }

    public void runAuto() {
        System.err.println("------------- runAuto");

        try (ActorSystem sys = new ActorSystemKelp.ActorSystemDefaultForKelp()) {
            KryoBuilder.SerializerPool sp = new KryoBuilder.SerializerPoolDefault(sys);
            KeyHistogramsPersistable kh = new KeyHistogramsPersistable(new Conf(1000), new PersistentFileManager("target/debug-persist",
                    sp, Paths::get, new ActorSystemDefault.SystemLoggerErr()));
            HistogramTreePersistable tree = kh.create(new ActorKelpFunctions.KeyComparatorDefault<>(), 3);

            KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
            input(tree, ctx);

            check(tree, ctx);

            checkTreeSize(tree, ctx);
        }
    }

    public void runAutoSerialize() {
        System.err.println("------------- runAutoSerialize");
        try (ActorSystem sys = new ActorSystemKelp.ActorSystemDefaultForKelp()) {
            KryoBuilder.SerializerPool sp = new KryoBuilder.SerializerPoolDefault(sys);
            KeyHistogramsPersistable kh = new KeyHistogramsPersistable(new Conf(1000), new PersistentFileManager("target/debug-persist",
                    sp, Paths::get, new ActorSystemDefault.SystemLoggerErr()));
            HistogramTreePersistable tree = kh.create(new ActorKelpFunctions.KeyComparatorDefault<>(), 3);

            KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
            input(tree, ctx);

            Output out = new Output(1_000_000);
            sp.write(out, tree);
            long n = out.total();
            byte[] data = out.getBuffer();
            System.err.println(String.format("persisted: %,d bytes", n));
            Input in = new Input(data, 0, (int) n);
            HistogramTreePersistable p = (HistogramTreePersistable) sp.read(in);
            p.init(kh.getPersistent());
            check(p, ctx);

            checkTreeSize(tree, ctx);
        }
    }


    private void input(HistogramTreePersistable tree, KeyHistograms.HistogramPutContextMap ctx) {
        ctx.putTree = tree;
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

    private boolean travConsume(HistogramTreePersistable tree, KeyHistograms.HistogramTreeNode node,
                                KeyHistograms.HistogramPutContextMap ctx) {
        if (node instanceof HistogramTreeNodeTable) {
            for (KeyHistograms.HistogramTreeNode n : ((HistogramTreeNodeTable) node).getChildren(tree)) {
                if (n.keyIn(tree.getComparator(), "a") == 0) {
                    return travConsume(tree, n, ctx);
                }
            }
            return false;
        } else if (node instanceof HistogramTreeNodeLeafOnStorage) {
            HistogramTreeNodeLeaf l = ((HistogramTreeNodeLeafOnStorage) node).load(ctx);
            if (l.size() > 3) {
                ctx.take(tree, l);
                return true;
            }
        }
        return false;
    }

    private void check(HistogramTreePersistable tree, KeyHistograms.HistogramPutContextMap ctx) {
        //loading
        log(String.format("persisted: %,d / %,d (%1.3f%%)", tree.getPersistedSize(), tree.getTreeSize(), (tree.getPersistedRate() * 100.0)));
        load(tree, tree.getRoot(), ctx);
        TestTool.assertEquals("persisted after read", 0L, tree.getPersistedSize(), Objects::equals);
        TestTool.assertEquals("size after read", 0L, tree.getTreeSize(), Objects::equals);
        if (TestTool.fail.get() == 0) {
            new TestTool().printOk("");
        }
    }

    public void load(HistogramTree tree, KeyHistograms.HistogramTreeNode node, KeyHistograms.HistogramPutContext context) {
        if (node instanceof HistogramTreeNodeTable) {
            for (KeyHistograms.HistogramTreeNode ch : ((HistogramTreeNodeTable) node).getChildren(tree)) {
                load(tree, ch, context);
            }
        } else if (node instanceof HistogramTreeNodeLeaf) {
            if (node instanceof HistogramTreeNodeLeafOnStorage) {
                try {
                    node = ((HistogramTreeNodeLeafOnStorage) node).load(context);
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
                new TestTool(false).check(node + " key " + size, node.keyStart().toString(), "" + o.toString().charAt(0), Objects::equals);
                int num = Integer.parseInt(o.toString().substring(1));
                new TestTool(false).check(node + " value " + size, prevNum, num, (prev, next) -> next > prev);
                prevNum = num;
                ++size;
            }
            TestTool.assertEquals(node + " size", nodeSize, size, Objects::equals);
        }
    }

    long leaves;
    long leafNZ;
    long nodeMem;
    long leafMem;
    long values;
    long valuesMem;

    void checkTreeSize(HistogramTree tree, KeyHistograms.HistogramPutContext ctx) {
        long leafTree = tree.getLeafSize();
        long leafNZTree = tree.getLeafSizeNonZero();
        long nodeTree = tree.getNodeSizeOnMemory();
        long leafMemTree = tree.getLeafSizeOnMemory();
        long valuesTree = tree.getTreeSize();
        long valuesMemTree = tree.getTreeSizeOnMemory();
        leaves = 0;
        leafNZ = 0;
        nodeMem = 0;
        leafMem = 0;
        values = 0;
        valuesMem = 0;
        checkTreeSize(tree, tree.getRoot(), ctx);
        TestTool.assertEquals("tree " + tree + " leaf", leaves, leafTree);
        TestTool.assertEquals("tree " + tree + " leafNZ", leafNZ, leafNZTree);
        TestTool.assertEquals("tree " + tree + " nodeMem", nodeMem, nodeTree);
        TestTool.assertEquals("tree " + tree + " leafMem", leafMem, leafMemTree);
        TestTool.assertEquals("tree " + tree + " values", values, valuesTree);
        TestTool.assertEquals("tree " + tree + " valuesMem", valuesMem, valuesMemTree);
    }

    long checkTreeSize(HistogramTree tree, KeyHistograms.HistogramTreeNode node, KeyHistograms.HistogramPutContext ctx) {
        if (node instanceof HistogramTreeNodeLeaf) {
            leaves++;
            if (node.isPersisted()) {
                node = node.load(ctx);
            } else {
                leafMem++;
            }

            long ls = ((HistogramTreeNodeLeaf) node).getStructList().stream()
                    .mapToLong(this::checkTreeSizeList)
                    .sum();

            TestTool.assertEquals("listSize " + node, ls, node.size());

            if (node.size() != 0) {
                leafNZ++;
            }

            return node.size();
        } else if (node instanceof HistogramTreeNodeTable) {
            if (!node.isPersisted()) {
                nodeMem++;
            }
            long subSum = ((HistogramTreeNodeTable) node).getChildren(tree).stream()
                    .mapToLong(n -> checkTreeSize(tree, n, ctx))
                    .sum();
            if (!node.isPersisted()) {
                TestTool.assertEquals("subSum " + node, subSum, node.size());
            }
            return node.size();
        } else {
            return 0;
        }
    }

    long checkTreeSizeList(KeyHistograms.HistogramLeafList list) {
        KeyHistograms.HistogramLeafCell cell = list.head;
        long size = 0;
        while (cell != null) {
            values += cell.size();
            size += cell.size();
            valuesMem += cell.sizeOnMemory();
            cell = cell.next;
        }
        return size;
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
        public long getHistogramPersistSizeLimit() {
            return limit;
        }

        @Override
        public long getHistogramPersistOnMemorySize() {
            return 3;
        }

        @Override
        public long getHistogramPersistRandomSeed() {
            return 1235;
        }
    }
}
