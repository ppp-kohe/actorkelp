package csl.actor.msgassoc;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class KeyHistogramsPersistable extends KeyHistograms {
    protected MailboxPersistable.PersistentFileManager persistent;

    public KeyHistogramsPersistable(MailboxPersistable.PersistentFileManager persistent) {
        this.persistent = persistent;
    }

    @Override
    public HistogramTree create(KeyComparator<?> comparator, int treeLimit) {
        return new HistogramTreePersistable(comparator, treeLimit, 10, 100, persistent);
    }

    public static class HistogramTreePersistable extends KeyHistograms.HistogramTree {
        protected PutIndexHistory history;
        protected int historyEntrySize;
        protected int historyEntryLimit;
        protected long sizeLimit = 1000;
        protected long onMemorySize = 1000;
        protected double sizeRatioThreshold = 0.00001;
        protected Random random = new Random();

        protected long persistedSize;

        protected MailboxPersistable.PersistentFileManager persistent;

        public HistogramTreePersistable(KeyHistograms.KeyComparator<?> comparator,
                                        int historyEntrySize, int historyEntryLimit,
                                        MailboxPersistable.PersistentFileManager persistent) {
            super(comparator);
            this.historyEntrySize = historyEntrySize;
            this.historyEntryLimit = historyEntryLimit;
            this.persistent = persistent;
            initHistory();
        }

        public HistogramTreePersistable(KeyHistograms.KeyComparator<?> comparator, int treeLimit,
                                        int historyEntrySize, int historyEntryLimit,
                                        MailboxPersistable.PersistentFileManager persistent) {
            super(comparator, treeLimit);
            this.historyEntrySize = historyEntrySize;
            this.historyEntryLimit = historyEntryLimit;
            this.persistent = persistent;
            initHistory();
        }

        public HistogramTreePersistable(KeyHistograms.HistogramNode root, KeyHistograms.KeyComparator<?> comparator, int treeLimit,
                                        int historyEntrySize, int historyEntryLimit,
                                        MailboxPersistable.PersistentFileManager persistent) {
            super(root, comparator, treeLimit);
            this.historyEntrySize = historyEntrySize;
            this.historyEntryLimit = historyEntryLimit;
            this.persistent = persistent;
            initHistory();
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

        @Override
        public HistogramTree createTree(HistogramNode root) {
            return new HistogramTreePersistable(root, comparator, treeLimit, historyEntrySize, historyEntryLimit, persistent);
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
            if (node instanceof HistogramNodeTreeOnStorage ||
                node instanceof HistogramNodeLeafOnStorage) {
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
                if (node instanceof HistogramNodeTreeOnStorage ||
                    node instanceof HistogramNodeLeafOnStorage) {
                    return node;
                } else {
                    /*
                     * node:
                     *   NodeTreeData (keyStart, keyEnd, height, size)
                     *   long sibling,
                     *   child0,
                     *   ...
                     *
                     * leaf:
                     *   NodeTreeData (keyStart, keyEnd, height, size),
                     *   Class node,
                     *   long listSize,
                     *   long listPointer[0..listSize] (&list0, &list1, ..., &list(listSize-1))
                     *   Object list0, ..., PersistFileEnd,
                     *   Object list1, ..., PersistFileEnd,
                     *   ...
                     *   Object list(listSize-1), ..., PersistFileEnd,
                     *   long listSizes[0..listSize]
                     *   long sibling
                     */
                    MailboxPersistable.PersistentFileReaderSource src = w.writeBegin();
                    NodeTreeData data = new NodeTreeData();
                    data.keyStart = node.keyStart();
                    data.keyEnd = node.keyEnd();
                    data.height = node.height();
                    data.size = node.size();
                    w.write(data);

                    if (node instanceof HistogramNodeLeaf) {
                        w.write(node.getClass());
                        List<HistogramLeafList> lists = ((HistogramNodeLeaf) node).getStructList();
                        long[] listPointers = new long[lists.size()];
                        w.writeLong(listPointers.length);
                        for (int i = 0; i < listPointers.length; ++i) {
                            listPointers[i] = w.writeLong(0);
                        }

                        long[] persistedSizes = new long[lists.size()];
                        int i = 0;
                        for (HistogramLeafList list : lists) {
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
                            ++i;
                        }

                        for (int pi = 0; pi < persistedSize; ++pi) {
                            w.writeLong(persistedSizes[pi]);
                            persistedSize += persistedSizes[pi];
                        }
                    }
                    w.writeEnd();

                    if (node instanceof HistogramNodeTree) {
                        TreeWriting sw = w.subWriting();
                        for (HistogramNode child: ((HistogramNodeTree) node).getChildren()) {
                            persistTreeNodeReplace(child, sw);
                        }
                        return new HistogramNodeTreeOnStorage(data, src);
                    } else if (node instanceof HistogramNodeLeaf) {
                        return new HistogramNodeLeafOnStorage(data, src);
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex); //TODO error ?
            }
            return node;
        }
    }

    public static class TreeWriting implements Cloneable, AutoCloseable {
        protected long pointerTarget = -1L;
        protected long lastPosition;
        protected long lastLength;
        protected MailboxPersistable.PersistentFileManager manager;
        protected Path path;
        protected RandomAccessFile dataStore;
        protected Kryo serializer;
        protected Output out = new Output();

        public TreeWriting(MailboxPersistable.PersistentFileManager manager, Path path) throws IOException {
            this.manager = manager;
            this.path = path;
            dataStore = new RandomAccessFile(path.toFile(), "w");
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
            lastPosition = dataStore.getFilePointer();
            lastLength = 0;
            return new MailboxPersistable.PersistentFileReaderSource(path.toString(), lastPosition, manager);
        }

        public void write(Object obj) throws IOException {
            out.reset();
            serializer.writeClassAndObject(out, obj);
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

        public void writeEnd() throws IOException  {
            dataStore.writeLong(0L); //sibling pointer
            lastLength += 8L;
            if (pointerTarget >= 0) {
                long pos = dataStore.getFilePointer();
                dataStore.seek(pointerTarget);
                dataStore.writeLong(lastPosition);
                dataStore.seek(pos);
            }
            pointerTarget = lastPosition + lastLength - 8L;
        }

    }

    public static class PutIndexHistory {
        public int count;
        public int[] indexHistogram = new int[10];
        public PutIndexHistory next;

        public void add(float v) {
            int len = indexHistogram.length;
            indexHistogram[Math.max(0, Math.min(len, (int) (len * v)))]++;
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

    public static class HistogramNodeTreeOnStorage extends HistogramNodeTree {
        protected MailboxPersistable.PersistentFileReaderSource source;

        public HistogramNodeTreeOnStorage(NodeTreeData data, MailboxPersistable.PersistentFileReaderSource source) {
            super(data.height, Collections.emptyList());
            this.source = source;
            this.keyStart = data.keyStart;
            this.keyEnd = data.keyEnd;
            this.size = data.size;
        }

        //TODO restore

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
    }

    public static class HistogramNodeLeafOnStorage extends HistogramNodeLeaf {
        protected MailboxPersistable.PersistentFileReaderSource source;
        public HistogramNodeLeafOnStorage(NodeTreeData data, MailboxPersistable.PersistentFileReaderSource source) {
            super(data.keyStart, null, data.height);
            this.source = source;
            this.size = data.size;
        }

        @Override
        protected void initStruct(HistogramPutContext context) { }

        @Override
        protected void putValueStruct(HistogramPutContext context) {
            //TODO
        }

        @Override
        protected boolean completedAfterPut(HistogramPutContext context) {
            return false;
        }

        @Override
        public List<HistogramLeafList> getStructList() {
            return Collections.emptyList();
        }
    }

    public static class NodeTreeData implements Serializable {
        public int height;
        public long size;
        public Object keyStart;
        public Object keyEnd;
    }
}
