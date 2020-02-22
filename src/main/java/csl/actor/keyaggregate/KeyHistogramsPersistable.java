package csl.actor.keyaggregate;

import com.esotericsoftware.kryo.io.Output;
import csl.actor.keyaggregate.MailboxPersistable.PersistentFileReaderSource;
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
                        HistogramLeafCell next = c.next;
                        currents[i] = next;
                        if (next == null) {
                            sizeRemaining--;
                        }
                        onMemoryRemaining--;
                        if (onMemoryRemaining <= 0) {
                            break;
                        }
                    }
                }
            }
            //persist remaining items
            int i = 0;
            for (HistogramLeafCell last : currents) {
                HistogramLeafCell c = last.next;
                PersistentFileReaderSource src = w.createReaderSourceFromCurrentPosition();
                long persistedSize = 0;
                while (c != null) {
                    w.write(c.value);
                    c = c.next;
                    ++persistedSize;
                }
                w.write(new MailboxPersistable.PersistentFileEnd());
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

                try (TreeWriting w = pw.subWriting()) {
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
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
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
                    w.writeBegin();
                    HistogramNodeOnStorage nOnS = (HistogramNodeOnStorage) node;
                    w.write(nOnS.getSource()); //PersistentFileReaderSource
                    w.write(nOnS.toData()); //NodeTreeData
                    w.writeEnd();
                    return node;
                } else {
                    boolean leaf = (node instanceof HistogramNodeLeaf);
                    PersistentFileReaderSource src = w.writeBegin(); //long sibling=0
                    NodeTreeData data = new NodeTreeData();
                    data.leaf = leaf;
                    data.keyStart = node.keyStart();
                    data.keyEnd = node.keyEnd();
                    data.height = node.height();
                    data.size = node.size();
                    w.write(data); //NodeTreeData

                    if (leaf) {
                        w.write(node.getClass()); //Class nodeType
                        List<HistogramLeafList> lists = ((HistogramNodeLeaf) node).getStructList();
                        int len = lists.size();
                        long[] listPointers = new long[len];
                        w.writeLong(len); //long listCount
                        for (int i = 0; i < len; ++i) {
                            listPointers[i] = w.writeLong(0); //long listPointer[0..listCount-1]
                        }
                        long[] persistedSizePointers = new long[len];
                        for (int i = 0; i < len; ++i) {
                            persistedSizePointers[i] = w.writeLong(0); //long listSizes[0..listCount-1]
                        }

                        long[] persistedSizes = new long[len];
                        for (int i = 0; i < len; ++i) {
                            w.writePositionToPointer(listPointers[i]); //listPointer[i]=&list[i]
                            persistedSizes[i] = persistList(w, lists.get(i));
                        }

                        w.writeLongsToPointer(persistedSizes, persistedSizePointers[0]); //listSizes[0,...]= size0,size1,...
                        for (int i = 0; i < len; ++i) {
                            persistedSize += persistedSizes[i];
                        }
                        return replaceEnd(node, new HistogramNodeLeafOnStorage(data, src), w);

                    } else {
                        try (TreeWriting sw = w.subWriting()) {
                            for (HistogramNode child : ((HistogramNodeTree) node).getChildren()) {
                                persistTreeNodeReplace(child, sw);
                            }
                        }
                        w.write(0L); //long sibling; invalid
                        w.write(new MailboxPersistable.PersistentFileEnd()); //PersistentFileEnd
                        return replaceEnd(node, new HistogramNodeTreeOnStorage(data, src), w);
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex); //TODO error ?
            }
        }

        protected long persistList(TreeWriting w, HistogramLeafList list) throws IOException {
            HistogramLeafCell cell = list.head;
            long persistedSize = 0;
            if (cell instanceof HistogramLeafCellOnStorage &&
                    ((HistogramLeafCellOnStorage) cell).isReading()) {
                HistogramLeafCellOnStorage head = (HistogramLeafCellOnStorage) cell;
                while (head.hasNext()) {
                    Object v = head.readNext(this);
                    if (v != null) {
                        w.write(v); //Object list[i]
                        persistedSize++;
                    } else {
                        break;
                    }
                }
                cell = cell.next;
            }
            while (cell != null) {
                w.write(cell.value); //Object list[i]
                persistedSize++;
                cell = cell.next;
            }
            w.write(new MailboxPersistable.PersistentFileEnd()); //list[N]=PersistentFileEnd
            return persistedSize;
        }

        protected HistogramNode replaceEnd(HistogramNode node, HistogramNode newNode, TreeWriting w) throws IOException {
            w.writeEnd();
            newNode.setParent(node.getParent());
            return newNode;
        }

    }


    public static class TreeWriting implements Cloneable, AutoCloseable {
        protected TreeWriting parent;
        protected long position;
        protected long pointerTarget = -1L;
        protected long positionStart;
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
            out = new Output(4096, Integer.MAX_VALUE);
        }

        @Override
        public void close() throws IOException {
            if (parent == null) {
                dataStore.close();
            } else {
                parent.position = position;
            }
        }

        public TreeWriting subWriting() {
            try {
                TreeWriting w = (TreeWriting) super.clone();
                w.parent = this;
                w.pointerTarget = -1L;
                w.position = this.position;
                return w;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

        public PersistentFileReaderSource writeBegin() throws IOException {
            writePositionToPointer(pointerTarget);
            positionStart = position;
            writeLongRaw(0L); //sibling pointer
            position += 8L;
            return new PersistentFileReaderSource(path.toString(), positionStart, manager);
        }

        public void write(Object obj) throws IOException {
            out.reset();
            serializer.write(out, obj);
            out.flush();
            long len = out.total();
            dataStore.write(out.getBuffer(), 0, (int) len);
            position += len;
        }

        protected void writeLongRaw(long n) throws IOException { //little endian
            out.reset();
            out.writeLong(n);
            out.flush();
            dataStore.write(out.getBuffer(), 0, 8);
        }

        public long writeLong(long n) throws IOException {
            long p = position;
            writeLongRaw(n);
            position += 8L;
            return p;
        }

        public void writePositionToPointer(long p) throws IOException {
            if (p >= 0) {
                dataStore.seek(p);
                writeLongRaw(position);
                dataStore.seek(position);
            }
        }

        public void writeLongsToPointer(long[] ns, long p) throws IOException {
            if (p >= 0) {
                dataStore.seek(p);
                for (long n : ns) {
                    writeLongRaw(n);
                }
                dataStore.seek(position);
            }
        }

        public void writeEnd() throws IOException  {
            pointerTarget = positionStart;
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

    public static class HistogramLeafListPersistable extends HistogramLeafList { //does not override iterator()

        @Override
        public Object poll(HistogramTree tree) {
            if (head instanceof HistogramLeafCellOnStorage) {
                Object n = ((HistogramLeafCellOnStorage) head).readNext(tree);
                if (n == null) {
                    super.poll(tree); //remove the storage
                    if (head == null) {
                        return null;
                    }
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
                    return false;
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
        public abstract boolean isReading();
    }

    public static class HistogramLeafCellOnStorageFile extends HistogramLeafCellOnStorage {
        protected PersistentFileReaderSourceWithSize source; //offset: listPointer
        protected transient MailboxPersistable.PersistentFileReader reader;
        protected HistogramLeafCellOnStorageFile current;
        protected Object next;
        protected boolean finish = false;

        public HistogramLeafCellOnStorageFile(PersistentFileReaderSourceWithSize source) {
            this.value = source;
            this.source = source;
        }

        public boolean hasNext() {
            return next != null || !finish;
        }

        @Override
        public boolean isReading() {
            return reader != null;
        }

        public void reset() {
            finish = false;
            reader = null;
            next = null;
        }

        @Override
        public Object readNext(HistogramTree tree) {
            if (finish) {
                return next;
            } else {
                if (reader == null) { //first
                    next = lookAhead(tree);
                }
                Object n = next;
                next = lookAhead(tree);
                return n;
            }
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
                if (v instanceof MailboxPersistable.PersistentFileEnd) {
                    reader.close();
                    reader = null;
                    finish = true;
                    return null;
                } else {
                    ((HistogramTreePersistable) tree).decrementPersistedSize();
                    source.remainingSize--;
                    if (v instanceof PersistentFileReaderSourceWithSize) {
                        PersistentFileReaderSourceWithSize rs = (PersistentFileReaderSourceWithSize) v;
                        rs.setManager(source.getManager());
                        current = new HistogramLeafCellOnStorageFile(rs);
                        return lookAhead(tree);
                    } else {
                        return v;
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + source + (reader == null ? "" : "," + reader) + ")";
        }
    }

    public static class PersistentFileReaderSourceWithSize implements Serializable {
        public PersistentFileReaderSource source;
        public long remainingSize;

        public PersistentFileReaderSourceWithSize(PersistentFileReaderSource source, long remainingSize) {
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

        @Override
        public String toString() {
            return String.format("%s(%s, remaining=%,d)",
                    getClass().getSimpleName(), source, remainingSize);
        }
    }

    public interface HistogramNodeOnStorage {
        boolean isPersisted();
        HistogramNode load(HistogramPutContext context);
        NodeTreeData toData();
        PersistentFileReaderSource getSource();
    }

    public static class HistogramNodeTreeOnStorage extends HistogramNodeTree implements HistogramNodeOnStorage {
        protected PersistentFileReaderSource source;

        public HistogramNodeTreeOnStorage(NodeTreeData data, PersistentFileReaderSource source) {
            super(data.height, Collections.emptyList());
            this.source = source;
            this.keyStart = data.keyStart;
            this.keyEnd = data.keyEnd;
            this.size = data.size;
        }

        @Override
        public PersistentFileReaderSource getSource() {
            return source;
        }

        @Override
        public NodeTreeData toData() {
            NodeTreeData d = new NodeTreeData();
            d.leaf = false;
            d.height = height;
            d.keyStart = keyStart;
            d.keyEnd = keyEnd;
            d.size = size;
            return d;
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
        public List<HistogramNode> getChildren() {
            load();
            return super.getChildren();
        }

        @Override
        public HistogramNode put(KeyComparator<?> comparator, Object key, HistogramPutContext context) {
            load(context);
            return super.put(comparator, key, context);
        }

        @Override
        protected HistogramNode split() {
            load();
            return super.split();
        }

        @Override
        public HistogramNode merge(int treeLimit, KeyComparator<?> comparator, HistogramNode lowerNode) {
            load();
            return super.merge(treeLimit, comparator, lowerNode);
        }

        @Override
        public boolean isPersisted() {
            return children.isEmpty();
        }

        @Override
        public HistogramNode load(HistogramPutContext context) {
            load();
            return this;
        }

        protected void load() {
            if (children.isEmpty()) {
                ArrayList<HistogramNode> cs = new ArrayList<>();
                try (MailboxPersistable.PersistentFileReader r = source.createReader()) {
                    long thisSibling = r.nextLong(); //long sibling
                    NodeTreeData thisData = (NodeTreeData) r.next(); //NodeTreData
                    int heightDiff = thisData.height - this.height;

                    while (true) {
                        long pos = r.position();
                        long sibling = r.nextLong(); //long sibling
                        Object childObj = r.next(); //PersistentFileEnd | PersistentFileReaderSource | NodeTreeData
                        if (childObj instanceof MailboxPersistable.PersistentFileEnd) {
                            break;
                        } else if (childObj instanceof PersistentFileReaderSource) {
                            PersistentFileReaderSource src = (PersistentFileReaderSource) childObj;
                            src.setManager(source.getManager());
                            NodeTreeData child = (NodeTreeData) r.next(); //NodeTreeData
                            if (child.leaf) {
                                cs.add(new HistogramNodeLeafOnStorage(child, src));
                            } else {
                                cs.add(new HistogramNodeTreeOnStorage(child, src));
                            }
                        } else {
                            NodeTreeData child = (NodeTreeData) childObj;
                            child.height += heightDiff;
                            if (child.leaf) {
                                cs.add(new HistogramNodeLeafOnStorage(child, source.newSource(pos)));
                            } else {
                                cs.add(new HistogramNodeTreeOnStorage(child, source.newSource(pos)));
                            }
                        }
                        if (sibling > 0) {
                            r.position(sibling);
                        } else {
                            break;
                        }
                    }
                    cs.trimToSize();
                    this.children = cs;
                    updateChildren();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        protected HistogramNode putChildAt(KeyComparator<?> comparator, Object key, HistogramPutContext context, int index) {
            HistogramNode child = children.get(index);
            if (child instanceof HistogramNodeOnStorage &&
                ((HistogramNodeOnStorage) child).isPersisted()) {
                HistogramNode newChild = ((HistogramNodeOnStorage) child).load(context);
                if (newChild != child) {
                    children.set(index, newChild);
                    newChild.setParent(this);
                }
            }
            return super.putChildAt(comparator, key, context, index);
        }

        @Override
        public String toString() {
            return String.format("%s(size=%,d, keys=%s..%s, source=%s, persisted=%s)",
                    getClass().getSimpleName(), size, keyStart, keyEnd, source, isPersisted());
        }
    }

    public static class HistogramNodeLeafOnStorage extends HistogramNodeLeaf implements HistogramNodeOnStorage {
        protected PersistentFileReaderSource source;
        public HistogramNodeLeafOnStorage(NodeTreeData data, PersistentFileReaderSource source) {
            super(data.keyStart, null, data.height);
            this.source = source;
            this.size = data.size;
        }

        @Override
        public PersistentFileReaderSource getSource() {
            return source;
        }

        @Override
        public NodeTreeData toData() {
            NodeTreeData d = new NodeTreeData();
            d.leaf = true;
            d.height = height;
            d.keyStart = key;
            d.keyEnd = key;
            d.size = size;
            return d;
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
                long thisSibling = r.nextLong(); //long sibling
                NodeTreeData thisData = (NodeTreeData) r.next(); //NodeTreeData
                Class<?> leafType = (Class<?>) r.next(); //Class nodeType
                HistogramNodeLeaf leaf = (HistogramNodeLeaf) leafType
                        .getConstructor(Object.class, HistogramPutContext.class, int.class)
                        .newInstance(key, context, height);
                leaf.setSize(size);

                long listCount = r.nextLong(); //long listCount
                long[] listPos = new long[(int) listCount];
                for (int i = 0; i < listCount; ++i) {
                    listPos[i] = r.nextLong(); //long listPointer[0..listCount-1]
                }
                long[] listSizes = new long[(int) listCount];
                for (int i = 0; i < listCount; ++i) {
                    listSizes[i] = r.nextLong(); //long listSizes[0..listCount-1]
                }
                List<HistogramLeafCellOnStorage> cells = new ArrayList<>((int) listCount);
                for (int i = 0; i < listCount; ++i) {
                    cells.add(new HistogramLeafCellOnStorageFile(new PersistentFileReaderSourceWithSize(source.newSource(listPos[i]),
                            listSizes[i])));
                }
                List<HistogramLeafList> structs = leaf.getStructList();
                for (int i = 0; i < listCount; ++i) {
                    HistogramLeafList list;
                    if (i < structs.size()) {
                        list = structs.get(i);
                    } else {
                        list = context.createEmptyList();
                        leaf.setStructList(i, list);
                    }
                    HistogramLeafCellOnStorage cell = cells.get(i);
                    if (list.tail != null) {
                        list.tail.next = cell;
                    }
                    list.tail = cell; //concatenate the persisted cells
                    if (list.head == null) {
                        list.head = list.tail;
                    }
                }
                leaf.setParent(getParent());
                return leaf;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public String toString() {
            return String.format("%s(size=%,d, keys=%s, source=%s)",
                    getClass().getSimpleName(), size, key, source);
        }
    }

    /**
     * <pre>
     * node:
     *   long sibling,
     *   NodeTreeData (false, keyStart, keyEnd, height, size)
     *   child0,
     *   ...
     *   PersistentFileEnd
     *
     * leaf:
     *   long sibling,
     *   NodeTreeData (true, keyStart, keyEnd, height, size),
     *   Class nodeType,
     *   long listCount,
     *   long listPointer[0..listCount-1] (&list0, &list1, ..., &list(listCount-1)),
     *   long listSizes[0..listCount-1],
     *   Object list0, ..., PersistentFileEnd,
     *   Object list1, ..., PersistentFileEnd,
     *   ...
     *   Object list(listSize-1), ..., PersistentFileEnd
     *
     *  persisted:
     *   long sibling,
     *   PersistentFileReaderSource(path, offset) //the order is intended for distinguishing persisted or not
     *   NodeTreeData
     *  </pre>
     */
    public static class NodeTreeData implements Serializable {
        public boolean leaf;
        public int height;
        public long size;
        public Object keyStart;
        public Object keyEnd;

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "leaf=" + leaf +
                    ", height=" + height +
                    ", size=" + size +
                    ", keyStart=" + keyStart +
                    ", keyEnd=" + keyEnd +
                    '}';
        }
    }
}
