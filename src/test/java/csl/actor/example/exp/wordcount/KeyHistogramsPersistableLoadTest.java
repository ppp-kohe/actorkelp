package csl.actor.example.exp.wordcount;

import csl.actor.ActorSystemDefault;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.cluster.MailboxPersistable;
import csl.actor.keyaggregate.*;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class KeyHistogramsPersistableLoadTest {
    KeyHistogramsPersistable.HistogramTreePersistable tree;
    public static void main(String[] args) throws Exception {
        new KeyHistogramsPersistableLoadTest().run(args);
    }
    public void run(String[] args) throws Exception {
        String arg = args[0];
        Path current = Paths.get(".").toAbsolutePath();
        try (Stream<Path> ps = Files.walk(Paths.get(arg))) {
            ps.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().startsWith("histtree"))
                    .map(current::relativize)
                    .map(Path::toString)
                    .forEach(this::test);
        }
    }

    class TestFileManager extends MailboxPersistable.PersistentFileManager {

        public TestFileManager(String path, KryoBuilder.SerializerFunction serializer, ConfigDeployment.PathModifier pathModifier) {
            super(path, serializer, pathModifier, new ActorSystemDefault.SystemLoggerErr());
        }

        @Override
        public Path getPath(String path) {
            Path base = Paths.get(this.path);
            Path p = Paths.get(path);
            Path f = base.resolve(p.getFileName());
            if (Files.exists(f)) {
                return f;
            } else {
                return p;
            }
        }
    }

    public void test(String path) {
        ActorSystemRemote r = new ActorSystemRemote();

        MailboxPersistable.PersistentFileManager pm = new TestFileManager(Paths.get(path).getParent().toString(),
                r.getSerializer(), Paths::get);
        MailboxPersistable.PersistentFileReaderSource src = new MailboxPersistable.PersistentFileReaderSource(path, 0,
                pm);


        System.err.println("============ " + path);
        try (MailboxPersistable.PersistentFileReader reader = src.createReader()) {
            //while (true) {
                long pos = reader.position();
                long sibling = reader.nextLong();
                Object obj = reader.next();
                KeyHistogramsPersistable.HistogramNodeTreeOnStorage h;
                if (obj instanceof MailboxPersistable.PersistentFileEnd) {
                    //break;
                    System.err.println("None");
                    return;
                } else if (obj instanceof MailboxPersistable.PersistentFileReaderSource) {
                    MailboxPersistable.PersistentFileReaderSource nSrc = (MailboxPersistable.PersistentFileReaderSource) obj;
                    obj = reader.next();
                    KeyHistogramsPersistable.NodeTreeData d = (KeyHistogramsPersistable.NodeTreeData) obj;
                    h = new KeyHistogramsPersistable.HistogramNodeTreeOnStorage(d, nSrc);
                } else {
                    KeyHistogramsPersistable.NodeTreeData d = (KeyHistogramsPersistable.NodeTreeData) obj;
                    h = new KeyHistogramsPersistable.HistogramNodeTreeOnStorage(d,
                            src.newSource(pos));
                }
                tree = new KeyHistogramsPersistable.HistogramTreePersistable(h,
                        new ActorBehaviorBuilderKeyAggregation.KeyComparatorDefault<>(), 1000,
                        new KeyHistogramsPersistable.HistogramTreePersistableConfig() {}, pm,
                        new KeyHistogramsPersistable.PersistentConditionHistogramSizeLimit(1, 1));
                long c = run(h);
                System.err.println(String.format(" tested %,d", c));
//                if (sibling <= 0) {
//                    break;
//                }
            //}
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public long run(KeyHistogramsPersistable.HistogramNodeTreeOnStorage tree) {
        tree.load(null);
        long count = 0;
        for (KeyHistograms.HistogramNode c : tree.getChildren()) {
            if (c instanceof KeyHistogramsPersistable.HistogramNodeTreeOnStorage) {
                count += run((KeyHistogramsPersistable.HistogramNodeTreeOnStorage) c);
            } else if (c instanceof KeyHistograms.HistogramNodeLeaf) {
                count += load((KeyHistograms.HistogramNodeLeaf) c);
            }
        }
        if (tree.size() == count) {
            //ok
        } else {
            System.err.println(String.format("%s: tree.size=%,d vs count=%,d", tree, tree.size(), count));
        }
        return count;
    }

    public long load(KeyHistograms.HistogramNodeLeaf l) {
        KeyHistograms.HistogramNodeLeaf al = l;
        if (l instanceof KeyHistogramsPersistable.HistogramNodeLeafOnStorage) {
            KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
            ctx.putTree = tree;
            al = (KeyHistograms.HistogramNodeLeaf)
                    ((KeyHistogramsPersistable.HistogramNodeLeafOnStorage) l).load(ctx);
        }
        long n = l.size();
        long count = 0;
        for (KeyHistograms.HistogramLeafList list : al.getStructList()) {
            while (!list.isEmpty()) {
                Object o = list.poll(tree);
                if (o != null) {
                    count++;
                }
            }
        }
        if (n == count) {
            //ok
        } else {
            System.err.println(String.format("%s: leaf.size=%,d vs count=%,d", l, n, count));
        }
        return n;
    }
}
