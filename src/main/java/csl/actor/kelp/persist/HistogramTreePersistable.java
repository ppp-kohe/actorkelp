package csl.actor.kelp.persist;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.kelp.ActorKelpFunctions;
import csl.actor.kelp.behavior.HistogramTree;
import csl.actor.kelp.behavior.HistogramTreeNodeLeaf;
import csl.actor.kelp.behavior.HistogramTreeNodeTable;
import csl.actor.kelp.behavior.KeyHistograms;
import csl.actor.kelp.persist.KeyHistogramsPersistable.HistogramPersistentOperationType;
import csl.actor.persist.PersistentFileManager;
import csl.actor.util.ConfigBase;
import csl.actor.util.SampleTiming;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class HistogramTreePersistable extends HistogramTree implements KeyHistogramsPersistable.HistogramTreePersistableConfig {
    public static final long serialVersionUID = 1L;
    protected PutIndexHistory history;
    protected int historyEntrySize;
    protected int historyEntryLimit;
    protected long sizeLimit;
    /**
     * required threshold
     */
    protected long onMemorySize;
    protected double sizeRatioThreshold;
    protected long randomSeed;
    protected Random random;

    protected long persistedSize;

    protected PersistentFileManager.PersistentFileReaderSource previousFullTreeSource;
    protected transient PersistentTreeManager persistentTree;

    protected transient KeyHistogramsPersistable.PersistentConditionHistogram condition;

    public HistogramTreePersistable() {
    }

    public HistogramTreePersistable(ActorKelpFunctions.KeyComparator<?> comparator, int treeLimit,
                                    KeyHistogramsPersistable.HistogramTreePersistableConfig config,
                                    PersistentFileManager persistent,
                                    KeyHistogramsPersistable.PersistentConditionHistogram condition) {
        super(null, comparator, treeLimit, persistent);
        initConfig(config);
        this.condition = condition;
        initHistory();
    }

    public HistogramTreePersistable(KeyHistograms.HistogramTreeNode root, ActorKelpFunctions.KeyComparator<?> comparator, int treeLimit,
                                    KeyHistogramsPersistable.HistogramTreePersistableConfig config,
                                    PersistentFileManager persistent,
                                    KeyHistogramsPersistable.PersistentConditionHistogram condition) {
        super(root, comparator, treeLimit, persistent);
        initConfig(config);
        this.condition = condition;
        initHistory();
    }


    public HistogramTreePersistable(HistogramTree tree,
                                    KeyHistogramsPersistable.HistogramTreePersistableConfig config,
                                    PersistentFileManager persistent,
                                    KeyHistogramsPersistable.PersistentConditionHistogram condition) {
        super(tree.getRoot(), tree.getComparator(), tree.getTreeLimit(), persistent);
        this.leafSize = tree.getLeafSize();
        this.leafSizeNonZero = tree.getLeafSizeNonZero();
        tree.getCompleted().forEach(this::complete);
        initConfig(config);
        this.condition = condition;
        initHistory();
    }

    @Override
    public HistogramTree init(PersistentFileManager persistent) {
        super.init(persistent);
        if (previousFullTreeSource != null && previousFullTreeSource.getManager() == null) {
            previousFullTreeSource.setManager(persistent);
        }
        return this;
    }

    public void init(PersistentFileManager persistent, KeyHistogramsPersistable.PersistentConditionHistogram condition) {
        init(persistent);
        if (this.condition == null) {
            this.condition = condition;
        }
    }

    protected void initConfig(KeyHistogramsPersistable.HistogramTreePersistableConfig config) {
        this.historyEntrySize = config.getHistogramPersistHistoryEntrySize();
        this.historyEntryLimit = config.getHistogramPersistHistoryEntryLimit();
        this.sizeLimit = config.getHistogramPersistSizeLimit();
        this.onMemorySize = config.getHistogramPersistOnMemorySize();
        this.sizeRatioThreshold = config.getHistogramPersistSizeRatioThreshold();
        this.randomSeed = config.getHistogramPersistRandomSeed();
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

    @Override
    public void read(Kryo kryo, Input input) {
        super.read(kryo, input);
        this.history = (PutIndexHistory) kryo.readClassAndObject(input);
        this.historyEntrySize = input.readVarInt(true);
        this.historyEntryLimit = input.readVarInt(true);
        this.sizeLimit = input.readVarLong(true);
        this.onMemorySize = input.readVarLong(true);
        this.sizeRatioThreshold = input.readDouble();
        this.randomSeed = input.readVarLong(true);
        this.random = (Random) kryo.readClassAndObject(input);
        this.persistedSize = input.readVarLong(true);
        this.previousFullTreeSource = (PersistentFileManager.PersistentFileReaderSource) kryo.readClassAndObject(input);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        super.write(kryo, output);
        kryo.writeClassAndObject(output, this.history);
        output.writeVarInt(this.historyEntrySize, true);
        output.writeVarInt(this.historyEntryLimit, true);
        output.writeVarLong(this.sizeLimit, true);
        output.writeVarLong(this.onMemorySize, true);
        output.writeDouble(this.sizeRatioThreshold);
        output.writeVarLong(this.randomSeed, true);
        kryo.writeClassAndObject(output, this.random);
        output.writeVarLong(this.persistedSize, true);
        kryo.writeClassAndObject(output, this.previousFullTreeSource);
    }

    @Override
    public HistogramTree copy() {
        HistogramTreePersistable p = (HistogramTreePersistable) super.copy();
        if (history != null) {
            p.history = history.copy();
        }
        if (random != null) {
            if (randomSeed == 0) {
                p.random = new Random();
            } else {
                p.random = new Random(randomSeed);
            }
        }
        return p;
    }

    ////// config

    @Override
    public int getHistogramPersistHistoryEntrySize() {
        return this.historyEntrySize;
    }

    @Override
    public int getHistogramPersistHistoryEntryLimit() {
        return this.historyEntryLimit;
    }

    @Override
    public long getHistogramPersistSizeLimit() {
        return this.sizeLimit;
    }

    @Override
    public long getHistogramPersistOnMemorySize() {
        return this.onMemorySize;
    }

    @Override
    public double getHistogramPersistSizeRatioThreshold() {
        return this.sizeRatioThreshold;
    }

    @Override
    public long getHistogramPersistRandomSeed() {
        return this.randomSeed;
    }

    @Override
    public HistogramTree createTree(KeyHistograms.HistogramTreeNode root) {
        return new HistogramTreePersistable(root, comparator, treeLimit, this, persistent, condition);
    }

    @Override
    public void put(Object key, KeyHistograms.HistogramPutContext context) {
        super.put(key, context);
        updateHistory(context);
        persistIfNeeded(context.allowFullTreePersist());
    }

    protected void updateHistory(KeyHistograms.HistogramPutContext context) {
        history.add(context.putIndexCurrentRangeStart + context.putIndexCurrentRangeLength / 2f);
        if (history.count > historyEntryLimit) {
            history = history.next;
            history.clear();
        }
    }

    @Override
    public long getTreeSizeForReduceCheck() {
        return getTreeSizeOnMemory();
    }

    @Override
    public long getTreeSizeOnMemory() {
        return getTreeSize() - persistedSize;
    }

    public KeyHistogramsPersistable.HistogramPersistentOperation checkPersist(String callerInfo) {
        KeyHistogramsPersistable.HistogramPersistentOperation op = checkPersistWithoutRestructure(callerInfo);
        if (!op.isNone()) {
            long prevLeaf = getLeafSize();
            long minTables = minTables(prevLeaf);
            long prevNodes = getNodeSizeOnMemory();
            double tableRatio = (prevNodes / (double) minTables);
            if (tableRatio > 5) {
                restructure(false);
                op = checkPersistWithoutRestructure(callerInfo + "+restruct");
                PersistentFileManager m = getPersistent();
                if (m != null) {
                    logger(m).log(PersistentFileManager.logPersist, KeyHistogramsPersistable.logPersistColor,
                            "Histogram(%h) restructure persisted=%,d prevNodes=%,d leaves=%,d minTables=%,d ratio=%1.2f -> nodes=%,d leaves=%,d %s",
                            System.identityHashCode(this),
                            getPersistedSize(),
                            prevNodes, prevLeaf, minTables, tableRatio,
                            getNodeSizeOnMemory(), getLeafSize(), op);
                }
            }
        }
        return op;
    }

    public KeyHistogramsPersistable.HistogramPersistentOperation checkPersistWithoutRestructure(String callerInfo) {
        if (condition == null) {
            return new KeyHistogramsPersistable.PersistentConditionHistogramSizeLimit(sizeLimit, leafSizeNonZeroToSizeRatio(), new ActorSystemDefault.SystemLoggerErr())
                    .needToPersist(callerInfo, this);
        } else {
            return condition.needToPersist(callerInfo, this);
        }
    }

    private long minTables(long leaves) {
        long n = leaves;
        long nodes = 0;
        int treeLimit = this.treeLimit;
        while (true) {
            long rem = n % treeLimit;
            if (rem == 0) {
                n = n / treeLimit;
            } else {
                n = (n + treeLimit - rem) / treeLimit;
            }
            nodes += n;
            if (n <= 1) {
                break;
            }
        }
        return Math.max(1, nodes);
    }

    protected void persistIfNeeded(boolean allowFullTreePersist) {
        KeyHistogramsPersistable.HistogramPersistentOperation op = checkPersist("afterPut");
        if (op.equals(HistogramPersistentOperationType.FullTree) &&
            !allowFullTreePersist) {
            op = new KeyHistogramsPersistable.HistogramPersistentOperationTree(getTreeSizeOnMemory());
        }
        persist(op);
    }

    @Override
    public boolean needToReduce() {
        KeyHistogramsPersistable.HistogramPersistentOperation op = checkPersist("needToReduce");
        return !op.isNone();
    }

    public void persist(KeyHistogramsPersistable.HistogramPersistentOperation op) {
        if (op.equals(HistogramPersistentOperationType.LargeLeaves)) {
            persistLargeLeaves();
        } else if (op.equals(HistogramPersistentOperationType.FullTree)) {
            persistTree();
        } else if (op instanceof KeyHistogramsPersistable.HistogramPersistentOperationTree) {
            persistTree(((KeyHistogramsPersistable.HistogramPersistentOperationTree) op).getSizeLimit());
        }
    }

    /**
     * @return leafSizeNonZero / treeSize.
     * a small value means few leaves accumulates many values.
     * a large value means many leaves accumulates few values.
     * 1.0 means each leaf contains only one value.
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
            writeTree(PersistentTreeManager.TREE_TAG_LEAVES, s -> {
                long prevPersist = this.persistedSize;
                long prevNodeSize = getNodeSizeOnMemory();
                long prevLeafSize = getLeafSizeOnMemory();
                long prevTreeSize = getTreeSizeOnMemory();
                s.logBefore(() -> String.format("keep=%,d prevPersisted=%,d nodes=(tblMem:%,d leafMem:%,d) valsMem=%,d",
                        keepSize, prevPersist,
                        prevNodeSize, prevLeafSize, prevTreeSize));

                persistLargeLeaves(root, s.writer, keepSize);

                s.logAfter(() -> String.format("keep=%,d persisted=(%,d %+,d) nodes=(tblMem:%,d leafMem:%,d) valsMem=%,d",
                        keepSize, persistedSize, (persistedSize - prevPersist),
                        getNodeSizeOnMemory(), getLeafSizeOnMemory(), getTreeSizeOnMemory()));
            });
        }
    }

    protected PersistentFileManager.PersistentFileReaderSource writeTree(byte tag, Consumer<PersistentTreeSession> task) {
        getTreePersistent();
        try {
            return persistentTree.write(tag, task);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected PersistentTreeManager getTreePersistent() {
        if (persistentTree == null) {
            persistentTree = new PersistentTreeManager(getPersistent(), logger(getPersistent()));
        }
        return persistentTree;
    }


    @Override
    public void close() {
        super.close();
        PersistentTreeManager tm = this.persistentTree;
        if (tm != null) {
            tm.close();
            persistentTree = null;
        }
    }

    public void persistLargeLeaves(KeyHistograms.HistogramTreeNode node, PersistentFileManager.PersistentWriter w, long keepSize) {
        if (node.size() > keepSize) {
            if (node instanceof HistogramTreeNodeTable) {
                for (KeyHistograms.HistogramTreeNode child : ((HistogramTreeNodeTable) node).getChildrenOnMemory()) {
                    persistLargeLeaves(child, w, keepSize);
                }
            } else if (node instanceof HistogramTreeNodeLeaf) {
                persistList((HistogramTreeNodeLeaf) node, w, keepSize);
            }
        }
    }

    public void persistList(HistogramTreeNodeLeaf leaf, PersistentFileManager.PersistentWriter w, long keepSize) {
        List<KeyHistograms.HistogramLeafList> valuesList = leaf.getStructList();
        int lists = valuesList.size();
        KeyHistograms.HistogramLeafCell[] persistedTop = new KeyHistograms.HistogramLeafCell[lists];
        int li = 0;
        int listsRemaining = 0;
        for (KeyHistograms.HistogramLeafList l : valuesList) {
            persistedTop[li] = l.head;
            if (persistedTop[li] != null) {
                listsRemaining++;
            }
            ++li;
        }
        long onMemoryRemaining = keepSize;
        while (listsRemaining > 0 && onMemoryRemaining > 0) {
            for (int i = 0; i < lists; ++i) {
                KeyHistograms.HistogramLeafCell c = persistedTop[i];
                if (c != null) {
                    onMemoryRemaining -= c.sizeOnMemory();
                    if (onMemoryRemaining <= 0) { //persist c, c.next, c.next.next...
                        break;
                    }
                    persistedTop[i] = c.next;
                    if (persistedTop[i] == null) {
                        listsRemaining--;
                    }
                }
            }
        }
        //persist remaining items
        li = 0;
        for (KeyHistograms.HistogramLeafCell top : persistedTop) {
            if (top != null) {
                HistogramLeafCellOnStorage.HistogramLeafCellOnStorageWriting writing = HistogramLeafCellOnStorage.HistogramLeafCellOnStorageWriting
                        .writeCell(this, leaf, top, w);
                this.persistedSize += writing.persistedSize;
                valuesList.get(li).replaceRest(top, writing.cell);
                leaf.sizePersisted += writing.persistedSize;
            }
            ++li;
        }
    }

    public interface ObjectWriter {
        void write(Object o) throws IOException;
    }

    public long getPersistedSize() {
        return persistedSize;
    }

    public void decrementPersistedSize() {
        persistedSize--;
    }

    public void persistTree(long sizeLimit) {
        float[] ms = toPersistTreeDist(this.history.totalMean());
        persistTree(Math.min(getTreeSizeOnMemory(), sizeLimit), ms);
    }

    private float[] toPersistTreeDist(float[] ms) {
        for (int i = 0, l = ms.length; i < l; ++i) {
            ms[i] = 1f - ms[i];
        }
        return ms;
    }

    public void persistTree(long persistingLimit, float... distribution) {
        //it does not check persistingLimit>0, persistTreeNode might save some sub-trees even under the condition
        writeTree(PersistentTreeManager.TREE_TAG_NODES, session -> {
            long prevPersisted = persistedSize;
            long prevNodeSize = getNodeSizeOnMemory();
            long prevLeafSize = getLeafSizeOnMemory();
            long prevTreeSize = getTreeSizeOnMemory();
            session.logBefore(() -> String.format("limit=%,d prevPersisted=%,d nodes=(tblMem:%,d leafMem:%,d) valsMem=%,d",
                    persistingLimit, prevPersisted,
                    prevNodeSize, prevLeafSize, prevTreeSize));

            root = persistTreeNode( root, 0, 1f, distribution, persistingLimit, session.writer, new PersistTreeContext(false));

            session.logAfter(() -> String.format("limit=%,d persisted=(%,d %+,d) nodes=(tblMem:%,d leafMem:%,d) valsMem=%,d dist=%s", persistingLimit,
                    persistedSize, (persistedSize - prevPersisted),
                    getNodeSizeOnMemory(), getLeafSizeOnMemory(), getTreeSizeOnMemory(),
                    formatDist(distribution)));
        });
    }


    public double getPersistedRate() {
        return getPersistedSize() / (double) getTreeSize();
    }

    private Object formatDist(float[] dist) {
        return ConfigBase.lazyToString(() -> {
            StringBuilder buf = new StringBuilder();
            buf.append("dist=[");
            for (int i = 0, len = dist.length; i < len; ++i) {
                if (i != 0) {
                    buf.append(" ");
                }
                buf.append(String.format("%1.1f:", ((float) i) / (float) len))
                        .append(String.format("%1.2f", dist[i]));
            }
            buf.append("]");
            return buf.toString();
        });
    }

    public ActorSystem.SystemLogger logger(PersistentFileManager m) {
        return condition == null ? m.getLogger() : condition.getLogger();
    }

    /**
     * @param node         the persisted node
     * @param rangeStart   the range start of the entire tree
     * @param rangeLength  the range length of the sub-tree against the entire tree
     * @param distribution the distribution of recent insertion values to the entire tree (0..1.0)
     * @param remaining    the required value size (the number of cell values) for the persisting
     * @param pw           the writer
     * @return the persisted node
     * @see KeyHistogramsPersistable.NodeTreeData
     */
    protected KeyHistograms.HistogramTreeNode persistTreeNode(KeyHistograms.HistogramTreeNode node, float rangeStart, float rangeLength, float[] distribution, long remaining,
                                                           TreeWritings.TreeWriting pw, PersistTreeContext context) {
        if (node.isPersisted()) {
            return node;
        } else if (node instanceof HistogramTreeNodeTable) {
            List<KeyHistograms.HistogramTreeNode> cs = ((HistogramTreeNodeTable) node).getChildren(this);
            int csSize = cs.size();

            boolean[] persisted = new boolean[csSize];
            long persistedSizeOfNode = 0;

            long nSize = node.size();

            try (TreeWritings.TreeWriting w = pw.subWriting()) {
                for (int i = 0; i < csSize; ++i) {
                    float p = rangeStart + (i / (float) csSize) * rangeLength;
                    float d = distribution[(int) Math.min(distribution.length - 1, Math.max(0, distribution.length * p))];

                    if (random.nextDouble() <= d) {
                        //persist
                        persisted[i] = true;

                        long prevPersisted = this.persistedSize;
                        cs.set(i, persistTreeNodeReplace(cs.get(i), w, context));
                        persistedSizeOfNode += this.persistedSize - prevPersisted;
                    }
                }

                if (persistedSizeOfNode < remaining) { //it needs further persisting
                    remaining -= persistedSizeOfNode;
                    float r = (1 / (float) csSize) * rangeLength;
                    for (int i = 0; i < csSize; ++i) {
                        if (!persisted[i]) {

                            long cSize = cs.get(i).size();
                            double sizeP = cSize / (double) nSize;

                            float p = rangeStart + (i / (float) csSize) * rangeLength;
                            cs.set(i, persistTreeNode(cs.get(i), p, r, distribution, (long) (remaining * sizeP), w, context));
                        }
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return node;

        } else if (node instanceof HistogramTreeNodeLeaf) {
            if (remaining > 0) {
                return persistTreeNodeReplace(node, pw, context);
            } else {
                return node;
            }
        } else {
            return node;
        }
    }

    public static class PersistTreeContext {
        private final KeyHistogramsPersistable.NodeTreeData data = new KeyHistogramsPersistable.NodeTreeData();
        private KeyHistogramsPersistable.LeafCellHeader[] headers;
        private boolean fullTree;

        public PersistTreeContext(boolean fullTree) {
            this.fullTree = fullTree;
        }

        public KeyHistogramsPersistable.NodeTreeData treeData() { //shared: the Kryo references are reset for each write: finally reset() in writeClassAndObject()
            return data;
        }

        public KeyHistogramsPersistable.LeafCellHeader[] headers(int n) {
            if (headers == null || headers.length != n) {
                headers = KeyHistogramsPersistable.createCellHeaders(n);
            }
            return headers;
        }

        public boolean isFullTree() {
            return fullTree;
        }
    }

    /**
     * @param node the persisted node
     * @param w    the writer
     * @return the persisted result node
     * @see KeyHistogramsPersistable.NodeTreeData
     */
    public KeyHistograms.HistogramTreeNode persistTreeNodeReplace(KeyHistograms.HistogramTreeNode node, TreeWritings.TreeWriting w,
                                                                  PersistTreeContext context) {
        try {
            if (node.isPersisted()) { //HistogramNodeOnStorage
                w.writeBegin();
                HistogramNodeOnStorage nOnS = (HistogramNodeOnStorage) node;
                w.write(nOnS.getSource()); //PersistentFileReaderSource
                w.write(nOnS.toData()); //NodeTreeData
                w.writeEnd();
                return node;
//            } else if (node instanceof HistogramTreeNodeLeafOnStorage) { //loaded LeafOnStorage will be replaced. so here persisted, but the above branch covers it
//                return persistTreeNodeReplaceMoveLeaf((HistogramTreeNodeLeafOnStorage) node, w, context);
            } else {
                boolean leaf = (node instanceof HistogramTreeNodeLeaf);
                PersistentFileManager.PersistentFileReaderSource src = w.writeBegin(); //long sibling=0
                KeyHistogramsPersistable.NodeTreeData data = context.treeData();
                data.leaf = leaf;
                data.keyStart = node.keyStart();
                data.keyEnd = node.keyEnd();
                data.size = node.size(); //suppose no on-memory cells
                w.write(data); //NodeTreeData

                if (leaf) {
                    HistogramTreeNodeLeaf leafNode = (HistogramTreeNodeLeaf) node;
                    w.write(node.getClass()); //Class nodeType
                    List<KeyHistograms.HistogramLeafList> lists = leafNode.getStructList();
                    int len = lists.size();
                    KeyHistogramsPersistable.LeafCellHeader[] headers = context.headers(len);
                    w.writeVarInt(len, true); //var int listCount
                    for (KeyHistogramsPersistable.LeafCellHeader h : headers) {
                        h.writeBeginZero(w);
                    }

                    for (int i = 0; i < len; ++i) {
                        KeyHistogramsPersistable.LeafCellHeader header = headers[i];
                        header.listPointer = w.position();
                        HistogramLeafCellOnStorage.HistogramLeafCellOnStorageWriting writing = HistogramLeafCellOnStorage.HistogramLeafCellOnStorageWriting
                                .write(this, leafNode, lists.get(i).head, w);
                        header.size = writing.totalSize;
                        header.maxLinkDepth = writing.maxLinkDepth;
                        this.persistedSize += writing.persistedSize;
                    }

                    for (KeyHistogramsPersistable.LeafCellHeader h : headers) {
                        h.writeEnd(w);
                    }

                    if (PersistentFileManager.logDebugPersist) {
                        long n = Arrays.stream(headers).mapToLong(l -> l.size).sum();
                        if (data.size != n) {
                            logger(w.getManager()).log(KeyHistogramsPersistable.logPersistColor,
                                    "KeyHistogramPersistable.persistTreeNodeReplace error: inconsistent leaf.size=%,d vs sum(list)=%,d : %s",
                                    data.size, n, w);
                        }
                    }

                    return replaceEnd(node, data, src, w, context.isFullTree());

                } else {
                    try (TreeWritings.TreeWriting sw = w.subWriting()) {
                        for (KeyHistograms.HistogramTreeNode child : ((HistogramTreeNodeTable) node).getChildren(this)) {
                            persistTreeNodeReplace(child, sw, context); //it might create new OnStorage node, but the parent entire will be persisted
                        }
                    }
                    w.writeLong(0L); //long sibling; invalid
                    w.write(PersistentFileManager.FILE_END); //PersistentFileEnd
                    return replaceEnd(node, data, src, w, context.isFullTree());
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException("persistTreeNodeReplace " + w, ex); //TODO error ?
        }
    }

    protected KeyHistograms.HistogramTreeNode replaceEnd(KeyHistograms.HistogramTreeNode node,
                                                         KeyHistogramsPersistable.NodeTreeData data,
                                                         PersistentFileManager.PersistentFileReaderSource src,
                                                         TreeWritings.TreeWriting w, boolean fullTree) throws IOException {
        w.writeEnd();
        if (fullTree) {
            return node;
        } else {
            KeyHistograms.HistogramTreeNode newNode = null;
            if (node instanceof HistogramTreeNodeTable) {
                addNodeSizeOnMemory(-1L);
                newNode = new HistogramTreeNodeTableOnStorage(data, src);
            } else { //if (node instanceof HistogramTreeNodeLeaf) { //leaf
                addLeafSizeOnMemory(-1L);
                newNode = new HistogramTreeNodeLeafOnStorage(data, src);
            }
            newNode.setParent(node.getParent());
            return newNode;
        }
    }

    /**
     * @param leaf the replaced leaf node
     * @param w the writing
     * @return saved leaf
     * @throws IOException the error by reading or writing
     * @see KeyHistogramsPersistable.NodeTreeData
     */
    //currently disabled
    public KeyHistograms.HistogramTreeNode persistTreeNodeReplaceMoveLeaf(HistogramTreeNodeLeafOnStorage leaf, TreeWritings.TreeWriting w,
                                                                          PersistTreeContext context) throws IOException {
        leaf.getFileManager(); //setting the manager
        try (PersistentFileManager.PersistentFileReader r = leaf.getSource().createReader()) {
            PersistentFileManager.PersistentFileReaderSource src = w.writeBegin(); //write sibling
            long thisSibling = r.nextLong();
            KeyHistogramsPersistable.NodeTreeData thisData = (KeyHistogramsPersistable.NodeTreeData) r.next();
            w.write(thisData);

            Class<?> leafType = (Class<?>) r.next();
            w.write(leafType);

            int len = r.nextVarInt(true); //var int listCount;
            KeyHistogramsPersistable.LeafCellHeader[] headers = context.headers(len);
            w.writeVarInt(len, true); //var int listCount

            for (KeyHistogramsPersistable.LeafCellHeader h : headers) {
                h.read(r);
                h.writeBegin(w);
            }

            for (int i = 0; i < len; ++i) {
                KeyHistogramsPersistable.LeafCellHeader header = headers[i];
                header.listPointer = w.position();

                HistogramLeafCellOnStorage.HistogramLeafCellOnStorageWriting writing =
                        new HistogramLeafCellOnStorage.HistogramLeafCellOnStorageWriting(r, w);

                header.size = writing.totalSize;
                header.maxLinkDepth = writing.maxLinkDepth;
                //NO persistedSize changed: this.persistedSize += writing.persistedSize;
            }

            for (KeyHistogramsPersistable.LeafCellHeader h : headers) {
                h.writeEnd(w);
            }

            return replaceEnd(leaf, thisData, src, w, context.isFullTree()); //TODO -1 leafOnMem OK?
        }
    }

        /**
         * @return implementation field getter
         */
    public PutIndexHistory getHistory() {
        return history;
    }

    public interface HistogramNodeOnStorage {
        boolean isPersisted();
        KeyHistogramsPersistable.NodeTreeData toData();
        PersistentFileManager.PersistentFileReaderSource getSource();
        PersistentFileManager getFileManager();
    }

    public static class PutIndexHistory implements Serializable {
        public static final long serialVersionUID = 1L;
        public int count;
        public int[] indexHistogram = new int[10];
        public PutIndexHistory next;

        public PutIndexHistory copy() {
            PutIndexHistory h = this;
            PutIndexHistory copy = null;
            PutIndexHistory copyTop = null;
            while (true) {
                PutIndexHistory nextCopy = new PutIndexHistory();
                PutIndexHistory src = h;
                Arrays.setAll(nextCopy.indexHistogram, i -> src.indexHistogram[i]);
                nextCopy.count = src.count;
                if (copy != null) {
                    copy.next = nextCopy;
                } else {
                    copyTop = nextCopy;
                }
                copy = nextCopy;

                h = h.next;
                if (h == this) {
                    break;
                }
            }
            copy.next = copyTop;
            return copyTop;
        }

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
            if (n != 0) {
                for (int i = 0, len = h.indexHistogram.length; i < len; ++i) {
                    ms[i] /= (float) n;
                }
            }
            return ms;
        }
    }

    public void setPreviousFullTreeSource(PersistentFileManager.PersistentFileReaderSource previousFullTreeSource) {
        this.previousFullTreeSource = previousFullTreeSource;
    }

    public PersistentFileManager.PersistentFileReaderSource getPreviousFullTreeSource() {
        return previousFullTreeSource;
    }

    /**
     * <pre>
     *     long prevPointer, //-1, ...
     *     long nextPointer,
     *     byte tag, //TREE_TAG_LEAVES=1 | TREE_TAG_NODES=2 | TREE_TAG_FULL_TREE=3
     *     ...
     * </pre>
     */
    public static class PersistentTreeManager {
        protected transient PersistentFileManager manager;
        public PersistentFileManager.PersistentFileReaderSource previousSource;
        public long previousLength;
        protected transient TreeWritings.TreeWriting writer;
        protected transient Instant lastTime = Instant.EPOCH;

        public static byte TREE_TAG_LEAVES = 1;
        public static byte TREE_TAG_NODES = 2;
        public static byte TREE_TAG_FULL_TREE = 3;

        protected SampleTiming logTiming = new SampleTiming(16, 1 << 16);
        protected Duration persistTime = Duration.ZERO;
        protected ActorSystem.SystemLogger logger;

        public PersistentTreeManager(PersistentFileManager manager, ActorSystem.SystemLogger logger) {
            this.manager = manager;
            this.logger = logger;
        }

        public ActorSystem.SystemLogger getLogger() {
            return logger;
        }

        public PersistentFileManager.PersistentFileReaderSource write(byte tag, Consumer<PersistentTreeSession> task) throws IOException {
            long prevPos = -1L;
            if (previousSource != null) {
                prevPos = previousSource.offset;
            }
            try {
                TreeWritings.lockWriter();
                try (TreeWritings.TreeWriting w = writer(manager).subWriting()) {
                    long offset = w.position();
                    w.writeLong(prevPos);
                    long pos = w.positionAndWriteLong(-1);
                    w.writeByte(tag);
                    PersistentTreeSession session = new PersistentTreeSession(this, w, offset, logTiming.next(), logTiming.getLast(), tag);
                    task.accept(session);
                    session.length = w.position() - offset;
                    w.writePositionToPointer(pos);
                    session.runLogAfter();
                }
                writeEnd();
            } finally {
                TreeWritings.unlockWriter();
            }
            return previousSource;
        }

        public TreeWritings.TreeWriting writer(PersistentFileManager m) throws IOException {
            Instant now = Instant.now();
            boolean sizeOver = previousLength > 1_800_000_000L;
            boolean timeOver = (lastTime == null || Duration.between(lastTime, now).compareTo(Duration.ofMinutes(1)) > 0);
            PersistentFileManager.PersistentFileReaderSource source = createNextSource(m, sizeOver); //update source
            if (writer == null || sizeOver || timeOver || writer.position() != source.offset) {
                if (writer != null) {
                    writer.close();
                }
                writer = createTreeWriting(m, source.pathExpanded, source.offset);
            }
            return writer;
        }

        protected PersistentFileManager.PersistentFileReaderSource createNextSource(PersistentFileManager m, boolean createNext) {
            if (previousSource == null || createNext) {
                previousSource = m.createReaderSourceForPathExpanded(m.createExpandedPathForHead("histtree"));
            } else {
                previousSource = previousSource.newSource(previousLength);
            }
            return previousSource;
        }

        public void writeEnd() {
            lastTime = Instant.now();
            previousLength = writer.position();
        }


        public TreeWritings.TreeWriting createTreeWriting(PersistentFileManager m, String pathExpanded, long offset) throws IOException {
            return TreeWritings.createWriter(m, pathExpanded, offset);
        }

        public void close() {
            try {
                if (writer != null) {
                    writer.close();
                    writer = null;
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public Duration addPersistTime(Duration time) {
            this.persistTime = persistTime.plus(time);
            return persistTime;
        }
    }

    public static class PersistentTreeSession {
        public TreeWritings.TreeWriting writer;
        public long offset;
        public long length;
        public boolean log;
        public Instant start;
        public PersistentTreeManager manager;
        public byte tag;
        public int logTiming;
        public Supplier<String> logAfter = () -> "";

        public PersistentTreeSession(PersistentTreeManager manager, TreeWritings.TreeWriting writer, long offset, boolean log, int logTiming, byte tag) {
            this.writer = writer;
            this.offset = offset;
            this.manager = manager;

            start = Instant.now();
            this.log = log;
            this.logTiming = logTiming;
            this.tag = tag;
        }

        public void logBefore(Supplier<String> logStr) {
            if (log && manager != null) {
                manager.getLogger().log(PersistentFileManager.logPersist, PersistentFileManager.logColorPersist,
                        "Histogram(%h) persist(%s) start : timing=%,d %s",
                        System.identityHashCode(this),
                        persistTreeOpName(tag),
                        logTiming,
                        logStr.get());
            }
        }

        public void logAfter(Supplier<String> logAfter) {
            this.logAfter = logAfter;
        }

        public void runLogAfter() {
            Duration time = Duration.between(start, Instant.now());
            Duration total = manager.addPersistTime(time);

            if (log && manager != null) {
                manager.getLogger().log(PersistentFileManager.logPersist, PersistentFileManager.logColorPersist,
                        "Histogram(%h) persist(%s) FINISH time=(+%s total:%s) %s: timing=%,d %s",
                        System.identityHashCode(this),
                        persistTreeOpName(tag),
                        time, total,
                        PersistentConditionActor.bytesString(length),
                        logTiming,
                        logAfter.get());
            }
        }

        private String persistTreeOpName(byte tag) {
            if (tag == PersistentTreeManager.TREE_TAG_LEAVES) {
                return "LargeLeaves";
            } else if (tag == PersistentTreeManager.TREE_TAG_FULL_TREE) {
                return "FullTree";
            } else if (tag == PersistentTreeManager.TREE_TAG_NODES) {
                return "Nodes";
            } else {
                return "?(" + tag + ")";
            }
        }
    }

    /**
     * <pre>
     * long treeSize, //the tree (this) is not contains the size. so write here
     * HistogramTreePersistable this, //root is null
     * long nextPointer0, //the flatten sorted entries
     * ... //entire-leaves (NodeTreeData) or persisted-nodes
     * long nextPointer1,
     * ...
     * long nextPointer2,
     * ...
     * long 0
     * PersistentFileEnd
     * </pre>
     * @return the saved source, set to {@link #getPreviousFullTreeSource()}
     * @see KeyHistogramsPersistable.NodeTreeData
     */
    public PersistentFileManager.PersistentFileReaderSource persistTree() {
        previousFullTreeSource = writeTree(PersistentTreeManager.TREE_TAG_FULL_TREE, session -> {
            try {
                long prevNodesOnMem = getNodeSizeOnMemory();
                long prevLeafOnMom = getLeafSizeOnMemory();
                long prevSizeMem = getTreeSizeOnMemory();
                long prevPersisted = this.persistedSize;
                session.logBefore(() -> String.format("prevPersisted=%,d nodes=(tblMem:%,d leafMem:%,d) valsMem=%,d",
                        prevPersisted,
                        prevNodesOnMem, prevLeafOnMom, prevSizeMem));

                TreeWritings.TreeWriting w = session.writer;
                //write all info of the tree except for nodes
                KeyHistograms.HistogramTreeNode currentRoot = this.root;
                this.root = null;
                w.writeLong(getTreeSize()); //the size is from root, so it is not included in write(this)
                w.write(this);

                persistTreeTraverse(currentRoot, w, new PersistTreeContext(true));

                w.writeLong(0L); //long sibling; invalid
                w.write(PersistentFileManager.FILE_END); //PersistentFileEnd

                clearTree(true);

                session.logAfter(() -> String.format("nodes=(tblMem:%,d->0 leafMem:%,d->0) valsMem=%,d->0",
                        prevNodesOnMem, prevLeafOnMom, prevSizeMem));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        return previousFullTreeSource;
    }

    protected void persistTreeTraverse(KeyHistograms.HistogramTreeNode node, TreeWritings.TreeWriting w,
                                       PersistTreeContext context) {
        if (node.isPersisted()) {
            persistTreeNodeReplace(node, w, context);
        } else if (node instanceof HistogramTreeNodeTable) {
            ((HistogramTreeNodeTable) node).getChildren(this)
                    .forEach(n -> persistTreeTraverse(n, w, context));
        } else if (node instanceof HistogramTreeNodeLeaf) {
            persistTreeNodeReplace(node, w, context);
        }
    }

    @Override
    protected void clearTree(boolean clearCompleted) {
        super.clearTree(clearCompleted);
        initHistory();
        persistedSize = 0;
    }
}
