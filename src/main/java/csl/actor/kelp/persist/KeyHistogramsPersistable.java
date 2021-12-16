package csl.actor.kelp.persist;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystem;
import csl.actor.kelp.ActorKelpFunctions.KeyComparator;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.behavior.HistogramTree;
import csl.actor.kelp.behavior.HistogramTreeNodeLeaf;
import csl.actor.kelp.behavior.HistogramTreeNodeTable;
import csl.actor.kelp.behavior.KeyHistograms;
import csl.actor.persist.PersistentFileManager;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <ul>
 *    <li>{@link KeyHistogramsPersistable} : the tree factory
 *       <ul>
 *         <li>{@link #create(KeyComparator, int, Class, Map)} : creates {@link HistogramTreePersistable}</li>
 *       </ul>
 *    </li>
 *    <li> {@link HistogramTreePersistableConfig} : the config interface.
 *            {@link HistogramTreePersistableConfigKelp} is an impl. with wrapping {@link ConfigKelp}
 *    </li>
 *    <li> {@link PersistentConditionHistogram} :
 *       the method {@link PersistentConditionHistogram#needToPersist(String, HistogramTreePersistable)}
 *          checks current status and returns {@link HistogramPersistentOperationType#LargeLeaves}
 *             or {@link HistogramPersistentOperationTree} (with a size limit) if it has too large tree.
 *            <ul>
 *                <li>{@link PersistentConditionHistogramSizeLimit} : simply checks whether the on-memory size exceeds a limit</li>
 *            </ul>
 *    </li>
 *
 *    <li> {@link HistogramTreePersistable}:  a {@link HistogramTree}
 *    </li>
 * </ul>
 */
public class KeyHistogramsPersistable extends KeyHistograms {
    protected PersistentConditionHistogram condition;
    protected HistogramTreePersistableConfig config;

    public static int logPersistColor = ActorSystem.systemPropertyColor("csl.actor.histogram.color", 106);

    public KeyHistogramsPersistable(HistogramTreePersistableConfig config,
                                    PersistentFileManager persistent) {
        this(config, persistent, new PersistentConditionHistogramSizeLimit(config, persistent.getLogger()));
    }

    public KeyHistogramsPersistable(HistogramTreePersistableConfig config,
                                    PersistentFileManager persistent,
                                    PersistentConditionHistogram condition) {
        super(persistent);
        this.config = config;
        this.condition = condition;
    }

    @Override
    public HistogramTreePersistable create(KeyComparator<?> comparator, int treeLimit, Class<?> keyType, Map<Object,Class<?>> valueTypesForPositions) {
        return new HistogramTreePersistable(comparator, treeLimit, config, persistent, condition)
                .withTypes(keyType, valueTypesForPositions);
    }

    public HistogramTreePersistableConfig getConfig() {
        return config;
    }

    public PersistentConditionHistogram getCondition() {
        return condition;
    }

    @Override
    public HistogramTree init(HistogramTree tree) {
        if (tree instanceof HistogramTreePersistable) {
            HistogramTreePersistable p = (HistogramTreePersistable) tree;
            p.init(persistent, condition);
            return p;
        } else {
            return new HistogramTreePersistable(tree, config, persistent, condition);
        }
    }

    public interface HistogramTreePersistableConfig {
        default int getHistogramPersistHistoryEntrySize() { return 10; }
        default int getHistogramPersistHistoryEntryLimit() { return 100; }
        default long getHistogramPersistSizeLimit() { return 1000; }
        default long getHistogramPersistOnMemorySize() { return 100; }
        default double getHistogramPersistSizeRatioThreshold() { return 0.00001; }
        default long getHistogramPersistRandomSeed() { return 0; }
        default int getHistogramMergerMax() { return 64; }
    }

    public static class HistogramTreePersistableConfigKelp implements HistogramTreePersistableConfig {
        protected ConfigKelp config;

        public HistogramTreePersistableConfigKelp(ConfigKelp config) {
            this.config = config;
        }

        public int getHistogramPersistHistoryEntrySize() { return config.histogramPersistHistoryEntrySize; }
        public int getHistogramPersistHistoryEntryLimit() { return config.histogramPersistHistoryEntryLimit; }
        public long getHistogramPersistSizeLimit() { return config.histogramPersistSizeLimit; }
        public long getHistogramPersistOnMemorySize() { return config.histogramPersistOnMemorySize; }
        public double getHistogramPersistSizeRatioThreshold() { return config.histogramPersistSizeRatioThreshold; }
        public long getHistogramPersistRandomSeed() { return config.histogramPersistRandomSeed; }

        public int getHistogramMergerMax() {
            return config.histogramMergerMax;
        }
    }

    public interface PersistentConditionHistogram {
        HistogramPersistentOperation needToPersist(String callerInfo, HistogramTreePersistable tree);
        ActorSystem.SystemLogger getLogger();
    }

    public interface HistogramPersistentOperation {
        default boolean isNone() {
            return false;
        }
    }

    public enum HistogramPersistentOperationType implements HistogramPersistentOperation {
        LargeLeaves,
        FullTree,
        None {
            @Override
            public boolean isNone() {
                return true;
            }
        }
    }

    public static class HistogramPersistentOperationTree implements HistogramPersistentOperation {
        protected long sizeLimit;

        public HistogramPersistentOperationTree(long sizeLimit) {
            this.sizeLimit = sizeLimit;
        }

        public long getSizeLimit() {
            return sizeLimit;
        }

        @Override
        public String toString() {
            return String.format("TreeVals(limit:%,d)", sizeLimit);
        }
    }

    public static class PersistentConditionHistogramSizeLimit implements PersistentConditionHistogram {
        protected long sizeLimit;
        protected double sizeRatioThreshold;
        protected ActorSystem.SystemLogger logger;

        public PersistentConditionHistogramSizeLimit(long sizeLimit, double sizeRatioThreshold, ActorSystem.SystemLogger logger) {
            this.sizeLimit = sizeLimit;
            this.sizeRatioThreshold = sizeRatioThreshold;
            this.logger = logger;
        }

        public PersistentConditionHistogramSizeLimit(HistogramTreePersistableConfig config, ActorSystem.SystemLogger logger) {
            this(config.getHistogramPersistSizeLimit(), config.getHistogramPersistSizeRatioThreshold(),
                    logger);
        }

        @Override
        public HistogramPersistentOperation needToPersist(String callerInfo, HistogramTreePersistable tree) {
            if (tree.getLeafSizeNonZero() > this.sizeLimit) {
                HistogramPersistentOperation result;
                double leafNZSizeRatio = tree.leafSizeNonZeroToSizeRatio();
                if (leafNZSizeRatio < sizeRatioThreshold) {
                    result = HistogramPersistentOperationType.LargeLeaves;
                } else {
                    if (Math.abs(tree.getTreeSizeOnMemory() - sizeLimit) / (double) sizeLimit < 0.1) { //the entire size is close to the limit
                        result = HistogramPersistentOperationType.FullTree;
                    } else {
                        result = new HistogramPersistentOperationTree(sizeLimit); //TODO the sizeLimit of the op means the size of values, not leaves
                    }
                }
                if (logger != null) {
                    logger.log(PersistentFileManager.logPersist, logPersistColor,
                            "Histogram(%h) %s: needToPersist -> %s : leafNZ=(%,d limit:%,d) leafNZValuesRatio=(%3.2f %s limit:%3.2f)",
                            System.identityHashCode(tree), callerInfo, result,
                            tree.getLeafSizeNonZero(), sizeLimit,
                            leafNZSizeRatio, leafNZSizeRatio < sizeRatioThreshold ? "<" : ">", sizeRatioThreshold);
                }
                return result;
            } else {
                return HistogramPersistentOperationType.None;
            }
        }

        @Override
        public ActorSystem.SystemLogger getLogger() {
            return logger;
        }
    }

    public static final byte TAG_END = 0;
    public static final byte TAG_NODE = 1;
    public static final byte TAG_SOURCE = 2;

    /**
     * <pre>
     * node:
     *   long sibling,
     *   byte tag = 1, //TAG_NODE
     *   NodeTreeData (false, keyStart, keyEnd, size)
     *   child0,
     *   ...
     *   0,
     *   PersistentFileEnd
     *
     * leaf:
     *   long sibling,
     *   byte tag = 1, //TAG_NODE
     *   NodeTreeData (true, keyStart, keyEnd, size),
     *   Class nodeType,
     *   var int listCount,
     *   LeafCellHeader listPointer[0..listCount-1] {(&amp;list0, size0, maxDepth0), ...}
     *   HistogramLeafCell list0: {segmentTag,...}, HistogramLeafCellSerializedEnd,
     *   HistogramLeafCell list1: {segmentTag,...}, HistogramLeafCellSerializedEnd,
     *   ...
     *   HistogramLeafCell list(listSize-1), ..., HistogramLeafCellSerializedEnd
     *
     *  persisted:
     *   long sibling,
     *   byte tag = 2, //TAG_SOURCE
     *   PersistentFileReaderSource(path, offset) //the order is intended for distinguishing persisted or not
     *   NodeTreeData
     *  </pre>
     */
    public static class NodeTreeData implements Serializable {
        public static final long serialVersionUID = 1L;
        public boolean leaf;
        public long size;
        public Object keyStart;
        public Object keyEnd;

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "leaf=" + leaf +
                    ", size=" + size +
                    ", keyStart=" + keyStart +
                    ", keyEnd=" + keyEnd +
                    '}';
        }

        public void write(PersistentFileManager.PersistentWriter writer, Serializer<?> serializer) throws IOException {
            writer.writeByte((byte) (leaf ? 1 : 0));
            writer.writeVarLong(size, true);
            if (serializer == null) {
                writer.write(keyStart);
                if (!leaf) {
                    writer.write(keyEnd);
                }
            } else {
                writer.write(keyStart, serializer);
                if (!leaf) {
                    writer.write(keyEnd, serializer);
                }
            }
        }

        public void read(PersistentFileManager.PersistentReader reader, Class<?> keyType, Serializer<?> serializer) {
            leaf = (reader.nextByte() != 0);
            size = reader.nextVarLong(true);
            if (serializer == null) {
                keyStart = reader.next();
                if (leaf) {
                    keyEnd = keyStart;
                } else {
                    keyEnd = reader.next();
                }
            } else {
                keyStart = reader.next(keyType, serializer);
                if (leaf) {
                    keyEnd = keyStart;
                } else {
                    keyEnd = reader.next(keyType, serializer);
                }
            }
        }
    }

    public static LeafCellHeader[] createCellHeaders(int len) {
        LeafCellHeader[] hs = new LeafCellHeader[len];
        for (int i = 0; i < len; ++i) {
            hs[i] = new LeafCellHeader();
        }
        return hs;
    }

    public static class LeafCellHeader implements TreeWritings.TreeWritingSeekData {
        public long pointer = -1L; //for tree writing
        public long listPointer;
        public long size;
        public int maxLinkDepth;

        public void writeBegin(TreeWritings.TreeWriting w) throws IOException {
            pointer = w.positionAndWriteLong(0);
            w.writeLong(size);
            w.writeInt(maxLinkDepth);
        }

        static final byte[] zeroData = new byte[8 + 8 + 4]; //listPtr+size+linkDep

        public void writeBeginZero(TreeWritings.TreeWriting w) throws IOException {
            pointer = w.position();
            w.writeBytes(zeroData);
//            pointer = w.positionAndWriteLong(0);
//            w.writeLong(0);
//            w.writeInt(0);
        }

        public void writeEnd(TreeWritings.TreeWriting w) throws IOException {
            w.writeSeekDataToPointer(pointer, this);
        }

        @Override
        public void write(TreeWritings.TreeWriting w) throws IOException {
            w.writeLong(listPointer);
            w.writeLong(size);
            w.writeInt(maxLinkDepth);
        }

        public void write(PersistentFileManager.PersistentWriter w) throws IOException {
            w.writeLong(listPointer);
            w.writeLong(size);
            w.writeInt(maxLinkDepth);
        }

        public void read(PersistentFileManager.PersistentFileReader r) {
            listPointer = r.nextLong();
            size = r.nextLong();
            maxLinkDepth = r.nextInt();
        }
        
        public void read(TreeWritings.TreeReader r) {
            listPointer = r.nextLong();
            size = r.nextLong();
            maxLinkDepth = r.nextInt();
        }
    }

    //////////

    /**
     * @param src the offset is saved by {@link HistogramTreePersistable#persistTree()} with {@link HistogramTreePersistable.PersistentTreeManager}
     * @return the loaded tree
     */
    public HistogramTree loadFullTree(PersistentFileManager.PersistentFileReaderSource src) {
        if (src.getManager() == null) {
            src.setManager(getPersistent());
        }
        try (PersistentFileManager.PersistentFileReader r = src.createReader()) {
            long prevPointer = r.nextLong();
            long nextPointer = r.nextLong();
            byte tag = r.nextByte();
            if (tag != HistogramTreePersistable.PersistentTreeManager.TREE_TAG_FULL_TREE) {
                r.getManager().getLogger().log("loadFullTree error tag:%d != %d",
                        tag, HistogramTreePersistable.PersistentTreeManager.TREE_TAG_FULL_TREE);
                return null;
            }

            HistogramTree tree = (HistogramTree) r.next();
            long size = r.nextLong();

            Class<?> kt = tree.finalKeyType();
            Serializer<?> keySer = r.serializer(kt);

            r.nextLong(); //sibling
            byte nodeTag = r.nextByte(); //tag
            if (nodeTag == KeyHistogramsPersistable.TAG_NODE) {
                NodeTreeData data = new NodeTreeData();
                data.read(r, kt, keySer);
                long rootPos = r.position();
                if (data.leaf) {
                    tree.setRoot(new HistogramTreeNodeLeafOnStorage(data, src.newSource(rootPos)));
                } else {
                    tree.setRoot(new HistogramTreeNodeTableOnStorage(data, src.newSource(rootPos)));
                }
            } else if (nodeTag == KeyHistogramsPersistable.TAG_SOURCE) {
                PersistentFileManager.PersistentFileReaderSource rootSrc = (PersistentFileManager.PersistentFileReaderSource) r.next();
                rootSrc.setManager(src.getManager());
                NodeTreeData data = new NodeTreeData();
                data.read(r, kt, keySer);
                if (data.leaf) {
                    tree.setRoot(new HistogramTreeNodeLeafOnStorage(data, rootSrc));
                } else {
                    tree.setRoot(new HistogramTreeNodeTableOnStorage(data, rootSrc));
                }
            }
            return tree;
        }
    }

    public static FullTreeLoader treeLoaderLastFullTree(PersistentFileManager.PersistentFileReaderSource source) throws IOException {
        FullTreeLoader l = new FullTreeLoader(source);
        l.jumpToLastFullTree();
        l.start();
        return l;
    }

    public interface KeyValueLoader {
        Object nextKey() throws IOException;
        Object nextValue() throws IOException;
        int currentListPosition() throws IOException;
        PersistentFileManager.PersistentFileReaderSource getSource();
    }

    public static class FullTreeLoader implements KeyValueLoader {
        protected FullTreeLoader parent;
        protected PersistentFileManager.PersistentFileReaderSource source;
        protected TreeWritings.TreeReader reader;

        protected long nextPointer;
        protected long prevPointer;
        protected HistogramTreePersistable current;
        protected HistogramPutContext currentContext;
        protected FullTreeLoaderData stack;

        protected boolean loaded = false;

        protected Class<?> keyType;
        protected Serializer<?> keySerializer;
        
        public FullTreeLoader(PersistentFileManager.PersistentFileReaderSource source) throws IOException {
            this.source = source;
        }
        @Override
        public PersistentFileManager.PersistentFileReaderSource getSource() {
            return source;
        }

        protected TreeWritings.TreeReader reader() throws IOException {
            if (reader == null) {
                reader = new TreeWritings.TreeReader(source.getManager(),
                        source.pathExpanded, source.offset);
            }
            return reader;
        }

        /**
         * reading tree-data saved {@link HistogramTreePersistable.PersistentTreeManager}
         * and searching a fullTree-persisted data.
         * @return true if found, and then the source is set to the top position of the found data
         * @throws IOException  error by reader
         */
        public boolean jumpToLastFullTree() throws IOException {
            TreeWritings.TreeReader reader = reader();
            long lastPointer = -1L;
            while (true) {
                long pos = reader.position();
                long prevPointer = reader.nextLong();
                long nextPointer = reader.nextLong();
                byte tag = reader.nextByte();
                if (tag == HistogramTreePersistable.PersistentTreeManager.TREE_TAG_FULL_TREE) {
                    lastPointer = pos;
                }
                if (nextPointer <= 0) {
                    break;
                } else {
                    reader.seek(nextPointer);
                }
            }
            if (lastPointer != -1L) {
                source = source.newSource(lastPointer);
                return true;
            } else {
                return false;
            }
        }

        public FullTreeLoader copyForPreviousOrNull() throws IOException {
            if (!loaded) {
                start();
            }
            if (current.getPreviousFullTreeSource() != null) {
                return new FullTreeLoader(current.getPreviousFullTreeSource());
            } else {
                return null;
            }
        }

        public HistogramTreePersistable tree() throws IOException {
            if (!loaded) {
                start();
            }
            return current;
        }

        /**
         * reading tree-data saved {@link HistogramTreePersistable.PersistentTreeManager}.
         * suppose the source.offset is set the top of a full-tree persist data jumped by {@link #jumpToLastFullTree()}
         * @throws IOException error by reader
         */
        public void start() throws IOException {
            TreeWritings.TreeReader reader = reader();
            loaded = true;
            reader.seek(source.offset);
            nextPointer = reader.nextLong();
            prevPointer = reader.nextLong();
            byte tag = reader.nextByte();
            long treeSize = reader.nextLong();
            current = (HistogramTreePersistable) reader.next();
            if (current != null && current.getPersistent() == null) {
                current.init(source.getManager());
            }
            if (current != null) {
                keyType = current.finalKeyType();
                keySerializer = reader.serializer(keyType);
            }
            currentContext = new HistogramPutContextMap(current);

            enterToLeafSegmentSibling(false);
        }

        protected boolean enterToLeafSegmentSibling(boolean topSibling) throws IOException {
            if (topSibling) {
                if (stack != null && stack.sibling() > 0) {
                    reader.seek(stack.sibling());
                    stack = stack.parent();
                } else {
                    return false;
                }
            }
            while (!enterToLeafSegment()) {
                if (stack != null && stack.sibling() > 0) {
                    reader.seek(stack.sibling());
                    stack = stack.parent();
                } else {
                    return false;
                }
            }
            return true;
        }

        protected boolean enterToLeafSegment() throws IOException {
            long sibling = reader.nextLong();
            byte tag = reader.nextByte();
            if (tag == KeyHistogramsPersistable.TAG_NODE) {
                NodeTreeData treeData = new NodeTreeData();
                treeData.read(reader, keyType, keySerializer);
                if (treeData.leaf) {
                    return enterToLeafSegmentLeaf(sibling, treeData);
                } else {
                    this.stack = new FullTreeLoaderDataTable(stack, sibling, treeData);
                    return enterToLeafSegment();
                }
            } else if (tag == KeyHistogramsPersistable.TAG_SOURCE) {
                PersistentFileManager.PersistentFileReaderSource src = (PersistentFileManager.PersistentFileReaderSource) reader.next();
                src.setManager(source.getManager());
                NodeTreeData treeData = new NodeTreeData();
                treeData.read(reader, keyType, keySerializer);
                if (treeData.leaf) {
                    stack = new FullTreeLoaderDataNodeLeaf(stack, sibling, new HistogramTreeNodeLeafOnStorage(treeData, src).load(currentContext));
                } else {
                    stack = new FullTreeLoaderDataNodeTable(stack, sibling, new HistogramTreeNodeTableOnStorage(treeData, src).load(currentContext));
                }
                return enterToLeafNodeLeaf();
            } else {
                return false;
            }
        }

        protected boolean enterToLeafSegmentLeaf(long sibling, NodeTreeData treeData) throws IOException {
            FullTreeLoaderDataLeaf stack = new FullTreeLoaderDataLeaf(this.stack, sibling, treeData);
            this.stack = stack;
            reader.next(); //Class nodeType
            int listCount = reader.nextVarInt(true); //var int listCount

            List<LeafCellHeader> headers = new ArrayList<>(listCount);
            for (int i = 0; i < listCount; ++i) {
                LeafCellHeader h = new LeafCellHeader();
                h.read(reader);
                headers.add(h);
            }
            stack.headers = headers;
            stack.headerIndex = 0;

            FullTreeLoaderDataLeafSegment stackSeg = stack.nextList(tree(), reader);
            if (stackSeg != null) {
                this.stack = stackSeg;
                return true;
            } else {
                return false;
            }
        }

        protected boolean enterToLeafNodeLeaf() {
            while (stack instanceof FullTreeLoaderDataNodeTable ||
                    stack instanceof FullTreeLoaderDataNodeLeaf) {
                while (stack instanceof FullTreeLoaderDataNodeTable) {
                    FullTreeLoaderData nextChild = ((FullTreeLoaderDataNodeTable) stack).nextChild(currentContext);
                    if (nextChild == null) {
                        if (stack.sibling() > 0) { //stop at top
                            return false;
                        } else {
                            stack = stack.parent();
                        }
                    } else {
                        stack = nextChild;
                    }
                }
                if (stack instanceof FullTreeLoaderDataNodeLeaf) {
                    FullTreeLoaderData nextChild = ((FullTreeLoaderDataNodeLeaf) stack).nextList(current);
                    if (nextChild == null) {
                        if (stack.sibling() > 0) { //stop at top
                            return false;
                        } else {
                            stack = stack.parent();
                        }
                    } else {
                        stack = nextChild;
                        return true;
                    }
                }
            }
            return false;
        }

        public Map.Entry<Object,Object> next() throws IOException {
            Object key = nextKey();
            while (key != null) {
                Object v = nextValue();
                if (v == null) {
                    key = nextKey();
                } else {
                    return Map.entry(key, v);
                }
            }
            return null;
        }

        public int currentListPosition() throws IOException {
            if (!loaded) {
                start();
            }
            return stack.currentListPosition();
        }

        @Override
        public Object nextKey() throws IOException {
            if (!loaded) {
                start();
            }
            while (true) {
                if (stack instanceof FullTreeLoaderDataLeafSegment) {
                    FullTreeLoaderDataLeafSegment stackSeg = (FullTreeLoaderDataLeafSegment) stack;
                    HistogramLeafCellOnStorage.CellSegment segment = stackSeg.segment;
                    if (segment.isNonEmpty()) {
                        return stackSeg.key;
                    } else if (segment instanceof HistogramLeafCellOnStorage.CellSegmentEnd) {
                        stack = stackSeg.parent();
                    } else {
                        stackSeg.nextSegment(); //load next segment
                    }
                } else if (stack instanceof FullTreeLoaderDataLeaf) {
                    FullTreeLoaderDataLeafSegment stackSeg = ((FullTreeLoaderDataLeaf) stack).nextList(tree(), reader);
                    if (stackSeg != null) {
                        stack = stackSeg;
                    } else {
                        enterToLeafSegmentSiblingOrParent();
                    }
                } else if (stack instanceof FullTreeLoaderDataTable) {
                    enterToLeafSegmentSiblingOrParent();
                } else if (stack instanceof FullTreeLoaderDataNodeTable ||
                            stack instanceof FullTreeLoaderDataNodeLeaf) {
                    if (!enterToLeafNodeLeaf()) {
                        enterToLeafSegmentSiblingOrParent();
                    }
                } else if (stack instanceof FullTreeLoaderDataNodeLeafList) {
                    FullTreeLoaderDataNodeLeafList stackList = (FullTreeLoaderDataNodeLeafList) stack;
                    if (stackList.hasNext()) {
                        return stackList.key();
                    } else {
                        stack = stack.parent();
                    }
                } else if (stack == null) {
                    if (reader != null) {
                        reader.close();
                        reader = null;
                    }
                    return null;
                } else {
                    throw new RuntimeException("invalid: " + stack);
                }
            }
        }

        private void enterToLeafSegmentSiblingOrParent() throws IOException {
            if (!enterToLeafSegmentSibling(true)) {
                if (stack != null) {
                    stack = stack.parent();
                }
            }
        }

        @Override
        public Object nextValue() throws IOException {
            if (stack instanceof FullTreeLoaderDataLeafSegment) {
                FullTreeLoaderDataLeafSegment stackSeg = (FullTreeLoaderDataLeafSegment) stack;
                HistogramLeafCellOnStorage.CellSegment segment = stackSeg.segment;
                if (segment.isNonEmpty()) {
                    return segment.pollWithReader(tree(), stackSeg.listPosition, stackSeg.reader, stackSeg,
                            stackSeg.valueType, stackSeg.valueSerializer);
                } else if (segment instanceof HistogramLeafCellOnStorage.CellSegmentEnd) {
                    return null;
                } else {
                    return null;
                }
            } else if (stack instanceof FullTreeLoaderDataNodeLeafList) {
                FullTreeLoaderDataNodeLeafList stackList = (FullTreeLoaderDataNodeLeafList) stack;
                if (stackList.hasNext()) {
                    return stackList.next();
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }
    }


    public static class FullTreeLoaderData {
        protected FullTreeLoaderData parent;

        public FullTreeLoaderData(FullTreeLoaderData parent) {
            this.parent = parent;
        }
        public long sibling() {
            return 0L;
        }
        public FullTreeLoaderData parent() {
            return parent;
        }
        public int currentListPosition() {
            return 0;
        }
    }

    public static class FullTreeLoaderDataLeaf extends FullTreeLoaderData {
        public KeyHistogramsPersistable.NodeTreeData treeData;
        public List<KeyHistogramsPersistable.LeafCellHeader> headers;
        public int headerIndex;
        public long sibling;

        public FullTreeLoaderDataLeaf(FullTreeLoaderData parent, long sibling, KeyHistogramsPersistable.NodeTreeData treeData) {
            super(parent);
            this.sibling = sibling;
            this.treeData = treeData;
        }

        public FullTreeLoaderDataLeafSegment nextList(HistogramTree tree, TreeWritings.TreeReader reader) throws IOException {
            int listPos = headerIndex;
            if (listPos < headers.size()) {
                KeyHistogramsPersistable.LeafCellHeader h = headers.get(listPos);
                reader.seek(h.listPointer);
                headerIndex++;

                Class<?> valueType = tree.finalValueTypeOrNull(listPos);
                Serializer<?> valueSer = reader.serializer(valueType);

                return new FullTreeLoaderDataLeafSegment(
                        treeData.keyStart, listPos, this, reader,
                        valueType, valueSer);

            } else {
                return null;
            }
        }

        @Override
        public int currentListPosition() {
            return headerIndex;
        }

        @Override
        public long sibling() {
            return sibling;
        }
    }

    public static class FullTreeLoaderDataLeafSegment extends FullTreeLoaderData
            implements Function<PersistentFileManager.PersistentFileReaderSource, PersistentFileManager.PersistentReader> {
        public Object key;
        public int listPosition;
        public TreeWritings.TreeReader reader;
        public HistogramLeafCellOnStorage.CellSegment segment;
        public Class<?> valueType;
        public Serializer<?> valueSerializer;
        public FullTreeLoaderDataLeafSegment(Object key, int listPosition, FullTreeLoaderDataLeaf parent,
                                             TreeWritings.TreeReader reader,
                                             Class<?> valueType, Serializer<?> valueSer) {
            super(parent);
            this.key = key;
            this.listPosition = listPosition;
            this.reader = reader;
            this.valueType = valueType;
            this.valueSerializer = valueSer;
            segment = HistogramLeafCellOnStorage.loadSegment(reader);
        }
        @Override
        public FullTreeLoaderDataLeaf parent() {
            return (FullTreeLoaderDataLeaf) parent;
        }

        public HistogramLeafCellOnStorage.CellSegment nextSegment() {
            segment = HistogramLeafCellOnStorage.loadSegment(reader);
            return segment;
        }

        @Override
        public int currentListPosition() {
            return listPosition;
        }

        @Override
        public PersistentFileManager.PersistentReader apply(PersistentFileManager.PersistentFileReaderSource source) {
            return reader.get(source);
        }
    }

    public static class FullTreeLoaderDataTable extends FullTreeLoaderData {
        public long sibling;
        public KeyHistogramsPersistable.NodeTreeData treeData;

        public FullTreeLoaderDataTable(FullTreeLoaderData parent, long sibling, KeyHistogramsPersistable.NodeTreeData treeData) {
            super(parent);
            this.sibling = sibling;
            this.treeData = treeData;
        }

        @Override
        public long sibling() {
            return sibling;
        }
    }

    public static class FullTreeLoaderDataNodeLeaf extends FullTreeLoaderData {
        public long sibling;
        public HistogramTreeNodeLeaf node;
        public int index;

        public FullTreeLoaderDataNodeLeaf(FullTreeLoaderData parent, long sibling, HistogramTreeNodeLeaf node) {
            super(parent);
            this.sibling = sibling;
            this.node = node;
        }

        public FullTreeLoaderData nextList(HistogramTree tree) {
            if (index < node.getStructList().size()) {
                FullTreeLoaderDataNodeLeafList l = new FullTreeLoaderDataNodeLeafList(this, tree, node,
                        index, node.getStructList().get(index));
                ++index;
                return l;
            } else {
                return null;
            }
        }

        @Override
        public long sibling() {
            return sibling;
        }
    }

    public static class FullTreeLoaderDataNodeLeafList extends FullTreeLoaderData {
        protected HistogramTree tree;
        protected HistogramTreeNodeLeaf leaf;
        protected HistogramLeafList list;
        protected int listIndex;
        protected long remaining;

        public FullTreeLoaderDataNodeLeafList(FullTreeLoaderData parent, HistogramTree tree, HistogramTreeNodeLeaf leaf,
                                              int listIndex, HistogramLeafList list) {
            super(parent);
            this.tree = tree;
            this.leaf = leaf;
            this.list = list;
            this.listIndex = listIndex;
            remaining = list.count();
        }

        public Object key() {
            return leaf.keyStart();
        }

        public boolean hasNext() {
            return remaining > 0;
        }

        @Override
        public int currentListPosition() {
            return listIndex;
        }

        public Object next() {
            Object v = list.poll(tree, listIndex, leaf);
            if (v != null) {
                --remaining;
            }
            return v;
        }
    }

    public static class FullTreeLoaderDataNodeTable extends FullTreeLoaderData {
        public long sibling;
        public HistogramTreeNodeTable node;
        public int index;

        public FullTreeLoaderDataNodeTable(FullTreeLoaderData parent, long sibling, HistogramTreeNodeTable node) {
            super(parent);
            this.sibling = sibling;
            this.node = node;
        }

        public FullTreeLoaderData nextChild(HistogramPutContext context) {
            while (index < node.getChildren(context.putTree).size()) {
                HistogramTreeNode t = node.getChildren(context.putTree).get(index);
                ++index;
                if (t instanceof HistogramTreeNodeTable) {
                    return new FullTreeLoaderDataNodeTable(this, -1L, (HistogramTreeNodeTable) t);
                } else if (t instanceof HistogramTreeNodeLeaf) {
                    return new FullTreeLoaderDataNodeLeaf(this, -1L, ((HistogramTreeNodeLeaf) t).load(context));
                }
            }
            return null;
        }

        @Override
        public long sibling() {
            return sibling;
        }
    }
}
