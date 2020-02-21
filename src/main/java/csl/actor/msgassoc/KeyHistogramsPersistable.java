package csl.actor.msgassoc;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.remote.KryoBuilder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class KeyHistogramsPersistable extends KeyHistograms {
    protected MailboxPersistable.PersistentFileManager persistent;
    protected HistogramTreePersistableConfig config;

    public KeyHistogramsPersistable(HistogramTreePersistableConfig config, MailboxPersistable.PersistentFileManager persistent) {
        this.config = config;
        this.persistent = persistent;
    }

    @Override
    public HistogramTreePersistable create(KeyComparator<?> comparator, int treeLimit) {
        return new HistogramTreePersistable(comparator, treeLimit, config, persistent);
    }

    public interface HistogramTreePersistableConfig {
        default int histogramPersistHistoryEntrySize() { return 10; }
        default int histogramPersistHistoryEntryLimit() { return 100; }
        default long histogramPersistSizeLimit() { return 1000; }
        default long histogramPersistOnMemorySize() { return 100; }
        default double histogramPersistSizeRatioThreshold() { return 0.00001; }
        default long histogramPersistRandomSeed() { return 0; }
    }

    public static class HistogramTreePersistable extends KeyHistograms.HistogramTree implements HistogramTreePersistableConfig {
        protected PutIndexHistory history;
        protected int historyEntrySize;
        protected int historyEntryLimit;
        protected long sizeLimit;
        protected long onMemorySize;
        protected double sizeRatioThreshold;
        protected long randomSeed;
        protected Random random;

        protected long persistedSize;

        protected MailboxPersistable.PersistentFileManager persistent;

        public HistogramTreePersistable(KeyHistograms.KeyComparator<?> comparator, int treeLimit,
                                        HistogramTreePersistableConfig config,
                                        MailboxPersistable.PersistentFileManager persistent) {
            super(comparator, treeLimit);
            initConfig(config);
            this.persistent = persistent;
            initHistory();
        }

        public HistogramTreePersistable(KeyHistograms.HistogramNode root, KeyHistograms.KeyComparator<?> comparator, int treeLimit,
                                        HistogramTreePersistableConfig config,
                                        MailboxPersistable.PersistentFileManager persistent) {
            super(root, comparator, treeLimit);
            initConfig(config);
            this.persistent = persistent;
            initHistory();
        }

        protected void initConfig(HistogramTreePersistableConfig config) {
            this.historyEntrySize = config.histogramPersistHistoryEntrySize();
            this.historyEntryLimit = config.histogramPersistHistoryEntryLimit();
            this.sizeLimit = config.histogramPersistSizeLimit();
            this.onMemorySize = config.histogramPersistOnMemorySize();
            this.sizeRatioThreshold = config.histogramPersistSizeRatioThreshold();
            this.randomSeed = config.histogramPersistRandomSeed();
            if (randomSeed == 0) {
                random = new Random();
            } else {
                random = new Random(randomSeed);
            }
        }

        protected void initHistory() {
            PutIndexHistory h = null;
            PutIndexHistory top = null;
            for (int i = 0, l = historyEntrySize; i < l; ++i) {
                if (h == null) {
                    h = new PutIndexHistory();
                    top = h;
                } else {
                    h.next = new PutIndexHistory();
                    h = h.next;
                }
            }

            history = h;
            if (h != null) {
                h.next = top;
            }
        }

        ////// config

        @Override
        public int histogramPersistHistoryEntrySize() { return this.historyEntrySize; }
        @Override
        public int histogramPersistHistoryEntryLimit() { return this.historyEntryLimit; }
        @Override
        public long histogramPersistSizeLimit() { return this.sizeLimit; }
        @Override
        public long histogramPersistOnMemorySize() { return this.onMemorySize; }
        @Override
        public double histogramPersistSizeRatioThreshold() { return this.sizeRatioThreshold; }
        @Override
        public long histogramPersistRandomSeed() { return this.randomSeed; }

        @Override
        public HistogramTree createTree(HistogramNode root) {
            return new HistogramTreePersistable(root, comparator, treeLimit, this, persistent);
        }

        @Override
        public HistogramLeafList createEmptyList() {
            return new HistogramLeafListPersistable();
        }

        @Override
        public void put(Object key, KeyHistograms.HistogramPutContext context) {
            super.put(key, context);
            updateHistory(context);
            checkPersist();
        }

        protected void updateHistory(HistogramPutContext context) {
            history.add(context.putIndexCurrentRangeStart + context.putIndexCurrentRangeLength / 2f);
            if (history.count > historyEntryLimit) {
                history = history.next;
                history.clear();
            }
        }

        protected void checkPersist() {
            if (getTreeSize() - persistedSize > this.sizeLimit) {
                if (leafSizeNonZeroToSizeRatio() < sizeRatioThreshold) {
                    persistLargeLeaves();
                } else {
                    persistTree();
                }
            }
        }

        /**
         * @return leafSizeNonZero / treeSize.
         *     a small value means few leaves accumulates many values.
         *     a large value means many leaves accumulates few values.
         *     1.0 means each leaf contains only one value.
         */
        public double leafSizeNonZeroToSizeRatio() {
            long s = this.getTreeSize();
            long l = this.getLeafSizeNonZero();
            if (s == 0) {
                return 0.5;
            } else {
                return l / (double) s;
            }
        }

        public void persistLargeLeaves() {
            persistLargeLeaves(onMemorySize);
        }

        public void persistLargeLeaves(long keepSize) {
            if (root != null) {
                try (MailboxPersistable.PersistentFileWriter w = getPersistentManager()
                        .createWriter("histleaf")) {
                    persistLargeLeaves(root, w, keepSize);
                }
            }
        }

        public void persistLargeLeaves(HistogramNode node, MailboxPersistable.PersistentFileWriter w, long keepSize) {
            if (node.size() > keepSize) {
                if (node instanceof HistogramNodeTree &&
                    !(node instanceof HistogramNodeTreeOnStorage)) {
                    for (HistogramNode child : ((HistogramNodeTree) node).getChildren()) {
                        persistLargeLeaves(child, w, keepSize);
                    }
                } else if (node instanceof HistogramNodeLeaf) {
                    persistList((((HistogramNodeLeaf) node).getStructList()), w, keepSize);
                }
            }
        }

        public void persistList(List<HistogramLeafList> valuesList, MailboxPersistable.PersistentFileWriter w, long keepSize) {
            int size = valuesList.size();
            HistogramLeafCell[] currents = new HistogramLeafCell[size];
            int li = 0;
            for (HistogramLeafList l : valuesList) {
                currents[li] = l.head;
                ++li;
            }
            long onMemoryRemaining = Math.max(1, keepSize); //1: skip top HistogramLeafCellOnStorage
            int sizeRemaining = size;
            while (sizeRemaining > 0 && onMemoryRemaining > 0) {
                for (int i = 0; i < li; ++i) {
                    HistogramLeafCell c = currents[i];
                    if (c != null) {
                        HistogramLeafCell next = currents[i].next;
                        currents[i] = next;
                        if (next == null) {
                            sizeRemaining--;
                        }
                        onMemoryRemaining--;
                        if (onMemoryRemaining < 0) {
                            break;
                        }
                    }
                }
            }
            //persist remaining items
            int i = 0;
            for (HistogramLeafCell last : currents) {
                HistogramLeafCell c = last.next;
                MailboxPersistable.PersistentFileReaderSource src = w.createReaderSourceFromCurrentPosition();
                long persistedSize = 0;
                while (c != null) {
                    w.write(c.value);
                    c = c.next;
                    ++persistedSize;
                }
                last.next = new HistogramLeafCellOnStorageFile(new PersistentFileReaderSourceWithSize(src, persistedSize));
                this.persistedSize += persistedSize;
                ++i;
            }
        }

        public long getPersistedSize() {
            return persistedSize;
        }

        protected void decrementPersistedSize() {
            persistedSize--;
        }

        protected MailboxPersistable.PersistentFileManager getPersistentManager() {
            return persistent;
        }

        public void persistTree() {
            float[] ms = toPersistTreeDist(this.history.totalMean());
            persistTree(getTreeSize() - sizeLimit, ms);
        }

        private float[] toPersistTreeDist(float[] ms) {
            for (int i = 0, l = ms.length; i < l; ++i) {
                ms[i] = 1f - ms[i];
            }
            return ms;
        }

        public void persistTree(long persistingLimit, float... distribution) {
            MailboxPersistable.PersistentFileManager m = getPersistentManager();
            try (TreeWriting w = new TreeWriting(m, m.createPath("histtree"))) {
                persistTreeNode(root, 0, 1f, distribution, persistingLimit, w);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public HistogramNode persistTreeNode(HistogramNode node, float rangeStart, float rangeLength, float[] distribution, long remaining,
                                             TreeWriting pw) {
            if (node instanceof HistogramNodeOnStorage) {
                return node;
            } else if (node instanceof HistogramNodeTree) {
                List<HistogramNode> cs = ((HistogramNodeTree) node).getChildren();
                int csSize = cs.size();

                boolean[] persisted = new boolean[csSize];
                long persistedSize = 0;

                long nSize = node.size();

                TreeWriting w = pw.subWriting();
                for (int i = 0; i < csSize; ++i) {
                    float p = rangeStart + (i / (float) csSize) * rangeLength;
                    float d = distribution[(int) Math.min(distribution.length - 1, Math.max(0, distribution.length * p))];

                    long cSize = cs.get(i).size();

                    if (random.nextDouble() <= d) {
                        //persist
                        persistedSize += cSize;
                        persisted[i] = true;

                        cs.set(i, persistTreeNodeReplace(cs.get(i), w));
                    }
                }

                if (persistedSize < remaining) {
                    remaining -= persistedSize;
                    float r = (1 / (float) csSize) * rangeLength;
                    for (int i = 0; i < csSize; ++i) {
                        if (!persisted[i]) {

                            long cSize = cs.get(i).size();
                            double sizeP = cSize / (double) nSize;

                            float p = rangeStart + (i / (float) csSize) * rangeLength;
                            cs.set(i, persistTreeNode(cs.get(i), p, r, distribution, (long) (remaining * sizeP), w));
                        }
                    }
                }

                return node;

            } else if (node instanceof HistogramNodeLeaf) {
                if (remaining > 0) {
                    return persistTreeNodeReplace(node, pw);
                } else {
                    return node;
                }
            } else {
                return node;
            }
        }

        public HistogramNode persistTreeNodeReplace(HistogramNode node, TreeWriting w) {
            try {
                if ((node instanceof HistogramNodeOnStorage) &&
                    ((HistogramNodeOnStorage) node).isPersisted()) {
                    return node;
                } else {
                    boolean leaf = (node instanceof HistogramNodeLeaf);
                    MailboxPersistable.PersistentFileReaderSource src = w.writeBegin();
                    NodeTreeData data = new NodeTreeData();
                    data.leaf = leaf;
                    data.keyStart = node.keyStart();
                    data.keyEnd = node.keyEnd();
                    data.height = node.height();
                    data.size = node.size();
                    w.write(data);

                    if (leaf) {
                        w.write(node.getClass());
                        List<HistogramLeafList> lists = ((HistogramNodeLeaf) node).getStructList();
                        int len = lists.size();
                        long[] listPointers = new long[len];
                        w.writeLong(len);
                        for (int i = 0; i < len; ++i) {
                            listPointers[i] = w.writeLong(0);
                        }
                        long[] persistedSizePointers = new long[len];
                        for (int i = 0; i < len; ++i) {
                            persistedSizePointers[i] = w.writeLong(0);
                        }

                        long[] persistedSizes = new long[len];
                        for (int i = 0; i < len; ++i) {
                            HistogramLeafList list = lists.get(i);
                            w.writePositionToPointer(listPointers[i]);
                            HistogramLeafCell cell = list.head;
                            if (cell instanceof HistogramLeafCellOnStorage) {
                                HistogramLeafCellOnStorage head = (HistogramLeafCellOnStorage) cell;
                                while (head.hasNext()) {
                                    Object v = head.readNext(this);
                                    w.write(v);
                                    persistedSizes[i]++;
                                }
                                cell = cell.next;
                            }
                            while (cell != null) {
                                w.write(cell.value);
                                persistedSizes[i]++;
                                cell = cell.next;
                            }
                            w.write(new MailboxPersistable.PersistentFileEnd());
                        }

                        w.writeLongsToPointer(persistedSizes, persistedSizePointers[0]);
                        for (int i = 0; i < len; ++i) {
                            persistedSize += persistedSizes[i];
                        }
                    }
                    w.writeEnd();

                    if (!leaf) {
                        TreeWriting sw = w.subWriting();
                        for (HistogramNode child: ((HistogramNodeTree) node).getChildren()) {
                            persistTreeNodeReplace(child, sw);
                        }
                        return new HistogramNodeTreeOnStorage(data, src);
                    } else {
                        return new HistogramNodeLeafOnStorage(data, src);
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex); //TODO error ?
            }
        }
    }

    public static class TreeWriting implements Cloneable, AutoCloseable {
        protected long pointerTarget = -1L;
        protected long lastPosition;
        protected long lastLength;
        protected MailboxPersistable.PersistentFileManager manager;
        protected Path path;
        protected RandomAccessFile dataStore;
        protected KryoBuilder.SerializerFunction serializer;
        protected Output out;

        public TreeWriting(MailboxPersistable.PersistentFileManager manager, Path path) throws IOException {
            this.manager = manager;
            this.serializer = manager.getSerializer();
            this.path = path;
            dataStore = new RandomAccessFile(path.toFile(), "rw");
            out = new Output(4096);
        }

        @Override
        public void close() throws IOException {
            dataStore.close();
        }

        public TreeWriting subWriting() {
            try {
                TreeWriting w = (TreeWriting) super.clone();
                w.pointerTarget = -1L;
                return w;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

        public MailboxPersistable.PersistentFileReaderSource writeBegin() throws IOException {
            writePositionToPointer(pointerTarget);
            lastPosition = dataStore.getFilePointer();
            lastLength = 0;
            dataStore.writeLong(0L); //sibling pointer
            lastLength += 8L;
            return new MailboxPersistable.PersistentFileReaderSource(path.toString(), lastPosition, manager);
        }

        public void write(Object obj) throws IOException {
            out.reset();
            serializer.write(out, obj);
            out.flush();
            long len = out.total();
            lastLength += len;
            dataStore.write(out.getBuffer(), 0, (int) len);
        }

        public long writeLong(long n) throws IOException {
            long pos = dataStore.getFilePointer();
            dataStore.writeLong(n);
            lastLength += 8L;
            return pos;
        }

        public void writePositionToPointer(long p) throws IOException {
            if (p >= 0) {
                long pos = dataStore.getFilePointer();
                dataStore.seek(p);
                dataStore.writeLong(pos);
                dataStore.seek(pos);
            }
        }

        public void writeLongsToPointer(long[] ns, long p) throws IOException {
            if (p >= 0) {
                long pos = dataStore.getFilePointer();
                dataStore.seek(p);
                for (long n : ns) {
                    dataStore.writeLong(n);
                }
                dataStore.seek(pos);
            }
        }

        public void writeEnd() throws IOException  {
            pointerTarget = lastPosition;
        }

    }

    public static class PutIndexHistory implements Serializable {
        public int count;
        public int[] indexHistogram = new int[10];
        public PutIndexHistory next;

        public void add(float v) {
            int len = indexHistogram.length;
            indexHistogram[Math.max(0, Math.min(len - 1, (int) (len * v)))]++;
            ++count;
        }

        public void clear() {
            count = 0;
            for (int i = 0, len = indexHistogram.length; i < len; ++i) {
                indexHistogram[i] = 0;
            }
        }

        public float[] totalMean() {
            float[] ms = new float[indexHistogram.length];
            PutIndexHistory h = this;
            long n = 0;
            while (true) {
                for (int i = 0, len = h.indexHistogram.length; i < len; ++i) {
                    ms[i] += h.indexHistogram[i];
                }
                n += h.count;
                h = h.next;
                if (h == this) {
                    break;
                }
            }
            for (int i = 0, len = h.indexHistogram.length; i < len; ++i) {
                ms[i] /= (float) n;
            }
            return ms;
        }
    }

    public static class HistogramLeafListPersistable extends HistogramLeafList {

        @Override
        public Object poll(HistogramTree tree) {
            if (head instanceof HistogramLeafCellOnStorage) {
                Object n = ((HistogramLeafCellOnStorage) head).readNext(tree);
                if (n == null) {
                    super.poll(tree); //remove the storage
                } else {
                    return n;
                }
            }
            return super.poll(tree);
        }

        @Override
        public boolean isEmpty() {
            if (head instanceof HistogramLeafCellOnStorage) {
                if (!((HistogramLeafCellOnStorage) head).hasNext()) {
                    super.poll(null); //just remove the item: the super does not touch the tree
                    return isEmpty();
                } else {
                    return true;
                }
            } else {
                return super.isEmpty();
            }
        }
    }

    public static abstract class HistogramLeafCellOnStorage extends HistogramLeafCell {
        public HistogramLeafCellOnStorage() {
            super(null);
        }
        public abstract boolean hasNext();
        public abstract Object readNext(HistogramTree tree);
    }

    public static class HistogramLeafCellOnStorageFile extends HistogramLeafCellOnStorage {
        protected PersistentFileReaderSourceWithSize source;
        protected transient MailboxPersistable.PersistentFileReader reader;
        protected HistogramLeafCellOnStorageFile current;
        protected Object next;

        public HistogramLeafCellOnStorageFile(PersistentFileReaderSourceWithSize source) {
            this.value = source;
            this.source = source;
        }

        public boolean hasNext() {
            return next != null ||
                    reader == null; //not yet read: remaining
        }

        @Override
        public Object readNext(HistogramTree tree) {
            Object n = (reader == null ? lookAhead(tree) : next);
            next = lookAhead(tree);
            return n;
        }

        public Object lookAhead(HistogramTree tree) {
            try {
                if (current != null) {
                    Object r = current.readNext(tree);
                    if (r != null) {
                        return r;
                    } else {
                        current = null;
                    }
                }
                if (reader == null) {
                    reader = source.createReader();
                }
                Object v = reader.next();
                ((HistogramTreePersistable) tree).decrementPersistedSize();
                source.remainingSize--;
                if (v instanceof MailboxPersistable.PersistentFileEnd) {
                    reader.close();
                    reader = null;
                    return null;
                } else {
                    if (v instanceof PersistentFileReaderSourceWithSize) {
                        PersistentFileReaderSourceWithSize rs = (PersistentFileReaderSourceWithSize) v;
                        rs.setManager(source.getManager());
                        current = new HistogramLeafCellOnStorageFile(rs);
                    }
                    return lookAhead(tree);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static class PersistentFileReaderSourceWithSize implements Serializable {
        public MailboxPersistable.PersistentFileReaderSource source;
        public long remainingSize;

        public PersistentFileReaderSourceWithSize(MailboxPersistable.PersistentFileReaderSource source, long remainingSize) {
            this.source = source;
            this.remainingSize = remainingSize;
        }

        public void setManager(MailboxPersistable.PersistentFileManager manager) {
            source.setManager(manager);
        }

        public MailboxPersistable.PersistentFileManager getManager() {
            return source.getManager();
        }


        public MailboxPersistable.PersistentFileReader createReader() {
            return source.createReader();
        }
    }

    public interface HistogramNodeOnStorage {
        boolean isPersisted();
        HistogramNode load(HistogramPutContext context);
    }

    public static class HistogramNodeTreeOnStorage extends HistogramNodeTree implements HistogramNodeOnStorage {
        protected MailboxPersistable.PersistentFileReaderSource source;

        public HistogramNodeTreeOnStorage(NodeTreeData data, MailboxPersistable.PersistentFileReaderSource source) {
            super(data.height, Collections.emptyList());
            this.source = source;
            this.keyStart = data.keyStart;
            this.keyEnd = data.keyEnd;
            this.size = data.size;
        }

        @Override
        protected void updateChildren() {
            if (!children.isEmpty()) {
                super.updateChildren();
            }
        }

        @Override
        protected void updateKeys() {
            if (!children.isEmpty()) {
                super.updateKeys();
            }
        }

        @Override
        public HistogramNode put(KeyComparator<?> comparator, Object key, HistogramPutContext context) {
            load(context);
            return super.put(comparator, key, context);
        }

        @Override
        protected HistogramNode split() {
            load(null);
            return super.split();
        }

        @Override
        public HistogramNode merge(int treeLimit, KeyComparator<?> comparator, HistogramNode lowerNode) {
            load(null);
            return super.merge(treeLimit, comparator, lowerNode);
        }

        @Override
        public boolean isPersisted() {
            return children.isEmpty();
        }

        @Override
        public HistogramNode load(HistogramPutContext context) {
            if (children.isEmpty()) {
                ArrayList<HistogramNode> cs = new ArrayList<>();
                try (MailboxPersistable.PersistentFileReader r = source.createReader()) {
                    long thisSibling = r.nextLong();
                    NodeTreeData thisData = (NodeTreeData) r.next();
                    int heightDiff = thisData.height - this.height;

                    while (true) {
                        long pos = r.position();
                        long sibling = r.nextLong();
                        NodeTreeData child = (NodeTreeData) r.next();
                        child.height += heightDiff;
                        if (child.leaf) {
                            cs.add(new HistogramNodeLeafOnStorage(child, source.newSource(pos)));
                        } else {
                            cs.add(new HistogramNodeTreeOnStorage(child, source.newSource(pos)));
                        }
                        if (sibling > 0) {
                            r.position(sibling);
                        } else {
                            break;
                        }
                    }
                    cs.trimToSize();
                    this.children = cs;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            return this;
        }

        @Override
        protected HistogramNode putChildAt(KeyComparator<?> comparator, Object key, HistogramPutContext context, int index) {
            HistogramNode child = children.get(index);
            if (child instanceof HistogramNodeOnStorage &&
                ((HistogramNodeOnStorage) child).isPersisted()) {
                HistogramNode newChild = ((HistogramNodeOnStorage) child).load(context);
                if (newChild != child) {
                    children.set(index, newChild);
                }
            }
            return super.putChildAt(comparator, key, context, index);
        }
    }

    public static class HistogramNodeLeafOnStorage extends HistogramNodeLeaf implements HistogramNodeOnStorage {
        protected MailboxPersistable.PersistentFileReaderSource source;
        public HistogramNodeLeafOnStorage(NodeTreeData data, MailboxPersistable.PersistentFileReaderSource source) {
            super(data.keyStart, null, data.height);
            this.source = source;
            this.size = data.size;
        }

        @Override
        protected void initStruct(HistogramPutContext context) { }

        @Override
        protected void putValueStruct(HistogramPutContext context) { }

        @Override
        protected boolean completedAfterPut(HistogramPutContext context) {
            return false;
        }

        @Override
        public List<HistogramLeafList> getStructList() {
            return Collections.emptyList();
        }

        @Override
        public void setStructList(int i, HistogramLeafList list) { }

        @Override
        public boolean isPersisted() {
            return true;
        }

        @Override
        public HistogramNode load(HistogramPutContext context) {
            try (MailboxPersistable.PersistentFileReader r = source.createReader()) {
                long thisSibling = r.nextLong();
                NodeTreeData thisData = (NodeTreeData) r.next();
                Class<?> leafType = (Class<?>) r.next();
                HistogramNodeLeaf leaf = (HistogramNodeLeaf) leafType
                        .getConstructor(Object.class, HistogramPutContext.class, int.class)
                        .newInstance(key, context, height);

                long listSize = r.nextLong();
                long[] listPos = new long[(int) listSize];
                for (int i = 0; i < listSize; ++i) {
                    listPos[i] = r.nextLong();
                }
                long[] listSizes = new long[(int) listSize];
                for (int i = 0; i < listSize; ++i) {
                    listSizes[i] = r.nextLong();
                }
                List<HistogramLeafCellOnStorage> cells = new ArrayList<>((int) listSize);
                for (int i = 0; i < listSize; ++i) {
                    cells.add(new HistogramLeafCellOnStorageFile(new PersistentFileReaderSourceWithSize(source.newSource(listPos[i]),
                            listSizes[i])));
                }
                List<HistogramLeafList> structs = leaf.getStructList();
                for (int i = 0; i < listSize; ++i) {
                    HistogramLeafList list;
                    if (i < structs.size()) {
                        list = structs.get(i);
                    } else {
                        list = context.createEmptyList();
                        leaf.setStructList(i, list);
                    }
                    list.tail = cells.get(i);
                }
                return leaf;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * <pre>
     * node:
     *   long sibling,
     *   NodeTreeData (false, keyStart, keyEnd, height, size)
     *   child0,
     *   ...
     *
     * leaf:
     *   long sibling
     *   NodeTreeData (true, keyStart, keyEnd, height, size),
     *   Class node,
     *   long listSize,
     *   long listPointer[0..listSize] (&list0, &list1, ..., &list(listSize-1))
     *   long listSizes[0..listSize]
     *   Object list0, ..., PersistFileEnd,
     *   Object list1, ..., PersistFileEnd,
     *   ...
     *   Object list(listSize-1), ..., PersistFileEnd,
     *  </pre>
     */
    public static class NodeTreeData implements Serializable {
        public boolean leaf;
        public int height;
        public long size;
        public Object keyStart;
        public Object keyEnd;
    }
}
