package csl.actor.kelp;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystem;
import csl.actor.cluster.ConfigBase;
import csl.actor.cluster.MailboxPersistable;
import csl.actor.cluster.MailboxPersistable.PersistentFileReaderSource;
import csl.actor.remote.KryoBuilder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.*;

public class KeyHistogramsPersistable extends KeyHistograms {
    protected PersistentConditionHistogram condition;
    protected HistogramTreePersistableConfig config;


    public static int logPersistColor = ActorSystem.systemPropertyColor("csl.actor.histogram.color", 106);

    public KeyHistogramsPersistable(HistogramTreePersistableConfig config,
                                    MailboxPersistable.PersistentFileManager persistent) {
        this(config, persistent, new PersistentConditionHistogramSizeLimit(config));
    }

    public KeyHistogramsPersistable(HistogramTreePersistableConfig config,
                                    MailboxPersistable.PersistentFileManager persistent,
                                    PersistentConditionHistogram condition) {
        super(persistent);
        this.config = config;
        this.condition = condition;
    }

    @Override
    public HistogramTreePersistable create(KeyComparator<?> comparator, int treeLimit) {
        return new HistogramTreePersistable(comparator, treeLimit, config, persistent, condition);
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
        default int histogramPersistHistoryEntrySize() { return 10; }
        default int histogramPersistHistoryEntryLimit() { return 100; }
        default long histogramPersistSizeLimit() { return 1000; }
        default long histogramPersistOnMemorySize() { return 100; }
        default double histogramPersistSizeRatioThreshold() { return 0.00001; }
        default long histogramPersistRandomSeed() { return 0; }
    }

    public interface PersistentConditionHistogram {
        HistogramPersistentOperation needToPersist(String callerInfo, HistogramTreePersistable tree,
                                                   long onMemoryValues, double leafSizeNonZeroToSizeRatio);
    }

    public interface HistogramPersistentOperation { }

    public enum HistogramPersistentOperationType implements HistogramPersistentOperation {
        LargeLeaves,
        None
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
            return "Tree(" + sizeLimit + ")";
        }
    }

    public static class PersistentConditionHistogramSizeLimit implements PersistentConditionHistogram {
        protected long sizeLimit;
        protected double sizeRatioThreshold;

        public PersistentConditionHistogramSizeLimit(long sizeLimit, double sizeRatioThreshold) {
            this.sizeLimit = sizeLimit;
            this.sizeRatioThreshold = sizeRatioThreshold;
        }

        public PersistentConditionHistogramSizeLimit(HistogramTreePersistableConfig config) {
            this(config.histogramPersistSizeLimit(), config.histogramPersistSizeRatioThreshold());
        }

        @Override
        public HistogramPersistentOperation needToPersist(String callerInfo, HistogramTreePersistable tree,
                                                          long onMemoryValues, double leafSizeNonZeroToSizeRatio) {
            if (onMemoryValues > this.sizeLimit) {
                if (leafSizeNonZeroToSizeRatio < sizeRatioThreshold) {
                    return HistogramPersistentOperationType.LargeLeaves;
                } else {
                    return new HistogramPersistentOperationTree(sizeLimit);
                }
            } else {
                return HistogramPersistentOperationType.None;
            }
        }
    }

    public static class PersistentConditionHistogramSampling implements PersistentConditionHistogram {
        protected MailboxPersistable mailbox;
        protected MailboxPersistable.PersistentConditionMailboxSampling mailboxSampling;
        protected long sizeLimit;
        protected double sizeRatioThreshold;
        protected MailboxPersistable.SampleTiming logTimingNeedToPersist = new MailboxPersistable.SampleTiming();

        public PersistentConditionHistogramSampling(long sizeLimit, double sizeRationThreshold, ActorSystem.SystemLogger logger) {
            this(sizeLimit, sizeRationThreshold, null, logger);
        }

        public PersistentConditionHistogramSampling(long sizeLimit, double sizeRationThreshold, MailboxPersistable mailbox,
                                                    ActorSystem.SystemLogger logger) {
            this.sizeLimit = sizeLimit;
            this.sizeRatioThreshold = sizeRationThreshold;
            this.mailbox = mailbox;
            if (mailbox != null &&
                    mailbox.getPersistent() instanceof MailboxPersistable.PersistentConditionMailboxSampling) {
                mailboxSampling = (MailboxPersistable.PersistentConditionMailboxSampling) mailbox.getPersistent();
            } else {
                mailboxSampling = new MailboxPersistable.PersistentConditionMailboxSampling(sizeLimit, logger);
            }
        }

        @Override
        public HistogramPersistentOperation needToPersist(String callerInfo, HistogramTreePersistable tree,
                                                          long onMemoryValues, double leafSizeNonZeroToSizeRatio) {
            if (onMemoryValues > sizeLimit) {
                boolean log = logTimingNeedToPersist.next();
                long sample = mailboxSampling.currentSampleWithUpdating(mailbox);
                long available = mailboxSampling.runtimeAvailableBytes();
                long leafSize = tree.getLeafSizeNonZero() * 96L;
                long estimated = (onMemoryValues + sizeLimit) * sample + leafSize;
                boolean res = estimated > available;
                long limit = 0;
                if (res) {
                    limit = Math.min(Math.max(sizeLimit, (available - leafSize) / sample), onMemoryValues);
                }
                if (log || res) {
                    mailboxSampling.getLogger().log(MailboxPersistable.logPersist, logPersistColor,
                            "Histogram(%h) %s: needToPersist: size=%,d sizeLimit=%,d leafNZ=%,d(%3.2f) sample=%,d estimated=%,d free=%,d (%3.1f%%) -> %s%s",
                            System.identityHashCode(tree), callerInfo,
                            onMemoryValues, sizeLimit, tree.getLeafSizeNonZero(), tree.getLeafSizeNonZeroRate(), sample,
                            estimated, available, available == 0 ? Double.POSITIVE_INFINITY : ((estimated / (double) available) * 100.0), res,
                            (res ? String.format(" limit:%,d", limit) : ""));
                }
                if (res) {
                    return new HistogramPersistentOperationTree(limit);
                } else {
                    return HistogramPersistentOperationType.None;
                }
            } else {
                return HistogramPersistentOperationType.None;
            }
        }
    }

    public static class HistogramTreePersistable extends KeyHistograms.HistogramTree implements HistogramTreePersistableConfig {
        public static final long serialVersionUID = 1L;
        protected PutIndexHistory history;
        protected int historyEntrySize;
        protected int historyEntryLimit;
        protected long sizeLimit;
        protected long onMemorySize;
        protected double sizeRatioThreshold;
        protected long randomSeed;
        protected Random random;

        protected long persistedSize;

        protected transient PersistentConditionHistogram condition;

        public HistogramTreePersistable(KeyHistograms.KeyComparator<?> comparator, int treeLimit,
                                        HistogramTreePersistableConfig config,
                                        MailboxPersistable.PersistentFileManager persistent,
                                        PersistentConditionHistogram condition) {
            super(null, comparator, treeLimit, persistent);
            initConfig(config);
            this.condition = condition;
            initHistory();
        }

        public HistogramTreePersistable(KeyHistograms.HistogramNode root, KeyHistograms.KeyComparator<?> comparator, int treeLimit,
                                        HistogramTreePersistableConfig config,
                                        MailboxPersistable.PersistentFileManager persistent,
                                        PersistentConditionHistogram condition) {
            super(root, comparator, treeLimit, persistent);
            initConfig(config);
            this.condition = condition;
            initHistory();
        }


        public HistogramTreePersistable(HistogramTree tree,
                                        HistogramTreePersistableConfig config,
                                        MailboxPersistable.PersistentFileManager persistent,
                                        PersistentConditionHistogram condition) {
            super(tree.getRoot(), tree.getComparator(), tree.getTreeLimit(), persistent);
            this.leafSize = tree.leafSize;
            this.leafSizeNonZero = tree.leafSizeNonZero;
            this.completed.addAll(tree.getCompleted());
            initConfig(config);
            this.condition = condition;
            initHistory();
        }

        public void init(MailboxPersistable.PersistentFileManager persistent, PersistentConditionHistogram condition) {
            init(persistent);
            if (this.condition == null) {
                this.condition = condition;
            }
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

        @Override
        public void read(Kryo kryo, Input input) {
            super.read(kryo, input);
            this.history = (PutIndexHistory) kryo.readClassAndObject(input);
            this.historyEntrySize = input.readInt();
            this.historyEntryLimit = input.readInt();
            this.sizeLimit = input.readLong();
            this.onMemorySize = input.readLong();
            this.sizeRatioThreshold = input.readDouble();
            this.randomSeed = input.readLong();
            this.random = (Random) kryo.readClassAndObject(input);
            this.persistedSize = input.readLong();
        }

        @Override
        public void write(Kryo kryo, Output output) {
            super.write(kryo, output);
            kryo.writeClassAndObject(output, this.history);
            output.writeInt(this.historyEntrySize);
            output.writeInt(this.historyEntryLimit);
            output.writeLong(this.sizeLimit);
            output.writeLong(this.onMemorySize);
            output.writeDouble(this.sizeRatioThreshold);
            output.writeLong(this.randomSeed);
            kryo.writeClassAndObject(output, this.random);
            output.writeLong(this.persistedSize);
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
            return new HistogramTreePersistable(root, comparator, treeLimit, this, persistent, condition);
        }

        @Override
        public HistogramLeafList createEmptyList() {
            return new HistogramLeafListPersistable();
        }

        @Override
        public void put(Object key, KeyHistograms.HistogramPutContext context) {
            super.put(key, context);
            updateHistory(context);
            persistIfNeeded();
        }

        protected void updateHistory(HistogramPutContext context) {
            history.add(context.putIndexCurrentRangeStart + context.putIndexCurrentRangeLength / 2f);
            if (history.count > historyEntryLimit) {
                history = history.next;
                history.clear();
            }
        }

        public HistogramPersistentOperation checkPersist(String callerInfo) {
            return checkPersist(callerInfo, getTreeSizeOnMemory());
        }

        @Override
        public long getTreeSizeForReduceCheck() {
            return getTreeSizeOnMemory();
        }

        public long getTreeSizeOnMemory() {
            return getTreeSize() - persistedSize;
        }

        public HistogramPersistentOperation checkPersist(String callerInfo, long onMemoryValues) {
            if (condition == null) {
                return new PersistentConditionHistogramSizeLimit(sizeLimit, leafSizeNonZeroToSizeRatio())
                        .needToPersist(callerInfo, this, onMemoryValues, leafSizeNonZeroToSizeRatio());
            } else {
                return condition.needToPersist(callerInfo, this, onMemoryValues, leafSizeNonZeroToSizeRatio());
            }
        }

        protected void persistIfNeeded() {
            persist(checkPersist("afterPut"));
        }

        @Override
        public boolean needToReduce() {
            KeyHistogramsPersistable.HistogramPersistentOperation op = checkPersist("needToReduce", getTreeSizeOnMemory() + 10L);
            return (!op.equals(KeyHistogramsPersistable.HistogramPersistentOperationType.None));
        }

        public void persist(HistogramPersistentOperation op) {
            if (op.equals(HistogramPersistentOperationType.LargeLeaves)) {
                persistLargeLeaves();
            } else if (op instanceof HistogramPersistentOperationTree) {
                persistTree(((HistogramPersistentOperationTree) op).getSizeLimit());
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
                MailboxPersistable.PersistentFileManager m = getPersistent();
                if (m != null) {
                    try (MailboxPersistable.PersistentFileWriter w = m.createWriter("histleaf")) {
                        long prevPersist = this.persistedSize;
                        persistLargeLeaves(root, w, keepSize);
                        logPersisted(String.format("Histogram(%h) persistLargeLeaves(keep=%,d)",
                                System.identityHashCode(this), keepSize), prevPersist, w, "");
                    }
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
            int lists = valuesList.size();
            HistogramLeafCell[] onMemoryTops = new HistogramLeafCell[lists];
            int li = 0;
            int listsRemaining = 0;
            for (HistogramLeafList l : valuesList) {
                onMemoryTops[li] = l.head; //the head is skipped as it may be an intermediate OnStorage item
                if (onMemoryTops[li] != null) {
                    listsRemaining++;
                }
                ++li;
            }
            long onMemoryRemaining = keepSize;
            while (listsRemaining > 0 && onMemoryRemaining > 0) {
                for (int i = 0; i < lists; ++i) {
                    HistogramLeafCell c = onMemoryTops[i];
                    if (c != null) {
                        HistogramLeafCell next = c.next;
                        onMemoryTops[i] = next;
                        if (onMemoryTops[i] == null) {
                            listsRemaining--;
                        }
                        onMemoryRemaining--;
                        if (onMemoryRemaining <= 0) {
                            break;
                        }
                    }
                }
            }
            //persist remaining items
            for (HistogramLeafCell last : onMemoryTops) {
                if (last != null) {
                    HistogramLeafCell c = last.next;
                    if (c != null) {
                        PersistentFileReaderSource src = w.createReaderSourceFromCurrentPosition();
                        long persistedSize;
                        try {
                            persistedSize = persistList(c, w::write);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        last.next = new HistogramLeafCellOnStorageFile(new PersistentFileReaderSourceWithSize(src, persistedSize));
                    }
                }
            }
        }

        interface ObjectWriter {
            void write(Object o) throws IOException;
        }

        public long persistList(HistogramLeafCell cell, ObjectWriter writer) throws IOException {
            long persistedSize = 0;
            if (cell instanceof HistogramLeafCellOnStorage &&
                    ((HistogramLeafCellOnStorage) cell).isReading()) {
                HistogramLeafCellOnStorage head = (HistogramLeafCellOnStorage) cell;
                while (head.hasNext()) {
                    Object v = head.readNext(this);
                    if (v != null) {
                        HistogramLeafCell savedCell = new HistogramLeafCell(v);
                        writer.write(savedCell); //save as cell
                        persistedSize += savedCell.valueCount();
                        //no change for this.persistedSize: because of read:-1 and write:+1
                    } else {
                        break;
                    }
                }
                cell = cell.next;
            }

            while (cell != null) {
                writer.write(cell);
                persistedSize += cell.valueCount();
                if (!(cell instanceof HistogramLeafCellOnStorage)) { //OnStorage is already included in the persistedSize
                    this.persistedSize += cell.valueCount();
                }
                cell = cell.next;
            }
            writer.write(new HistogramLeafCellSerializedEnd());
            return persistedSize;
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
            MailboxPersistable.PersistentFileManager m = getPersistent();
            if (m != null) { //it does not checks persistingLimit>0, persistTreeNode might save some sub-trees even under the condition
                try (TreeWriting w = new TreeWriting(m, m.createPath("histtree"))) {
                    logPersistedTreeBefore(persistingLimit, w);
                    long prevPersisted = persistedSize;
                    persistTreeNode(root, 0, 1f, distribution, persistingLimit, w);
                    logPersisted(String.format("Histogram(%h) persistTree(limit=%,d) finish",
                            System.identityHashCode(this), persistingLimit), prevPersisted, w,
                            formatDist(distribution));
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
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

        protected void logPersistedTreeBefore(long persistingLimit, Object obj) {
            MailboxPersistable.PersistentFileManager m = getPersistent();
            if (m != null) {
                m.getLogger().log(MailboxPersistable.logPersist, logPersistColor,
                        "Histogram(%h) persistTree(limit=%,d) persistedSize=%,d size=%,d onMemory=%,d : %s",
                        System.identityHashCode(this), persistingLimit, persistedSize, getTreeSize(), getTreeSizeOnMemory(), obj);
            }
        }

        protected void logPersisted(String head, long prevPersist, Object obj1, Object obj2) {
            MailboxPersistable.PersistentFileManager m = getPersistent();
            if (m != null && MailboxPersistable.logPersist) {
                m.getLogger().log(true, logPersistColor,
                        "%s: persistedSize=(%,d -> %,d (%+,d)) size=%,d onMemory=%,d leafNZ=%,d(%3.2f) leafNStoSize=%4.3f : %s %s",
                        head, prevPersist, persistedSize, (persistedSize - prevPersist), getTreeSize(), getTreeSizeOnMemory(),
                        getLeafSizeNonZero(), getLeafSizeNonZeroRate(), leafSizeNonZeroToSizeRatio(), obj1, obj2);
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

                        if (random.nextDouble() <= d) {
                            //persist
                            persisted[i] = true;

                            long prevPersisted = this.persistedSize;
                            cs.set(i, persistTreeNodeReplace(cs.get(i), w));
                            persistedSize += prevPersisted - this.persistedSize;
                        }
                    }

                    if (persistedSize < remaining) { //it needs further persisting
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
                    data.size = node.size(); //suppose no on-memory cells
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
                            persistedSizes[i] = persistList(lists.get(i).head, w::write);
                        }

                        w.writeLongsToPointer(persistedSizes, persistedSizePointers[0]); //listSizes[0,...]= size0,size1,...

                        if (MailboxPersistable.logDebugPersist) {
                            long n = Arrays.stream(persistedSizes).sum();
                            if (data.size != n) {
                                w.getManager().getLogger().log(true, logPersistColor,
                                        "KeyHistogramPersistable.persistTreeNodeReplace error: inconsistent leaf.size=%,d vs sum(list)=%,d : %s",
                                        data.size, n, w);
                            }
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
                throw new RuntimeException("persistTreeNodeReplace " + w, ex); //TODO error ?
            }
        }

        protected HistogramNode replaceEnd(HistogramNode node, HistogramNode newNode, TreeWriting w) throws IOException {
            w.writeEnd();
            newNode.setParent(node.getParent());
            return newNode;
        }

        /** @return implementation field getter */
        public PutIndexHistory getHistory() {
            return history;
        }
    }


    public static class TreeWriting implements Cloneable, AutoCloseable {
        protected TreeWriting parent;
        protected long position;
        protected long pointerTarget = -1L;
        protected long positionStart;
        protected MailboxPersistable.PersistentFileManager manager;
        protected String pathExpanded;
        protected RandomAccessFile dataStore;
        protected KryoBuilder.SerializerFunction serializer;
        protected Output out;
        protected Path filePath;

        public TreeWriting(MailboxPersistable.PersistentFileManager manager, String pathExpanded) throws IOException {
            this.manager = manager;
            this.serializer = manager.getSerializer();
            this.pathExpanded = pathExpanded;
            Path p = manager.getPath(pathExpanded);
            filePath = p;
            manager.openForWrite(p);
            dataStore = new RandomAccessFile(manager.getPath(pathExpanded).toFile(), "rw");
            out = new Output(4096, Integer.MAX_VALUE);
        }

        public String getPathExpanded() {
            return pathExpanded;
        }

        public MailboxPersistable.PersistentFileManager getManager() {
            return manager;
        }

        @Override
        public void close() throws IOException {
            if (parent == null) {
                dataStore.close();
                manager.close(filePath);
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
            return new PersistentFileReaderSource(pathExpanded, positionStart, manager);
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

        @Override
        public String toString() {
            return String.format("tree-writer(path=%s, pos=%,d", pathExpanded, position);
        }
    }

    public static class PutIndexHistory implements Serializable {
        public static final long serialVersionUID = 1L;
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
            if (n != 0) {
                for (int i = 0, len = h.indexHistogram.length; i < len; ++i) {
                    ms[i] /= (float) n;
                }
            }
            return ms;
        }
    }

    public static class HistogramLeafListPersistable extends HistogramLeafList { //does not override iterator()
        public static final long serialVersionUID = 1L;

        @Override
        public Object poll(HistogramTree tree) {
            if (head instanceof HistogramLeafCellOnStorage) {
                Object n = ((HistogramLeafCellOnStorage) head).readNext(tree);
                if (n == null) {
                    super.poll(tree); //remove the storage
                    if (head == null) {
                        return null;
                    }
                    return poll(tree);
                } else {
                    if (tree instanceof HistogramTreePersistable) {
                        ((HistogramTreePersistable) tree).decrementPersistedSize();
                    }
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

        @Override
        public void write(Kryo kryo, Output output) {
            super.write(kryo, output);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            super.read(kryo, input);
        }
    }

    public static abstract class HistogramLeafCellOnStorage extends HistogramLeafCell {
        public static final long serialVersionUID = 1L;
        public HistogramLeafCellOnStorage() {
            super(null);
        }
        public abstract boolean hasNext();
        public abstract Object readNext(HistogramTree tree);
        public abstract boolean isReading();
    }

    public static class HistogramLeafCellOnStorageFile extends HistogramLeafCellOnStorage {
        public static final long serialVersionUID = 1L;
        protected PersistentFileReaderSourceWithSize source; //offset: listPointer
        protected transient MailboxPersistable.PersistentFileReader reader;
        protected HistogramLeafCellOnStorageFile current;
        protected Object nextValue;
        protected boolean finish = false;
        protected long skipCount = 0;
        protected long remainingCount;

        public HistogramLeafCellOnStorageFile(PersistentFileReaderSourceWithSize source) {
            this.value = source;
            this.source = source;
            this.remainingCount = source.remainingSize;
        }

        public PersistentFileReaderSourceWithSize getSource() {
            return source;
        }

        @Override
        public long valueCount() {
            return remainingCount;
        }

        public boolean hasNext() {
            return nextValue != null || !finish;
        }

        @Override
        public boolean isReading() {
            return reader != null;
        }

        @Override
        public Object readNext(HistogramTree tree) {
            while (skipCount > 0 && !finish) {
                readNextBody(tree);
                --skipCount;
            }
            if (finish) {
                Object v = nextValue;
                nextValue = null;
                return v;
            } else {
                return readNextBody(tree);
            }
        }

        protected Object readNextBody(HistogramTree tree) {
            if (reader == null) { //first
                nextValue = lookAhead(tree);
            }
            Object n = nextValue;
            nextValue = lookAhead(tree);
            --remainingCount;
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
                    if (source.getManager() == null) {
                        source.setManager(tree.getPersistent());
                    }
                    reader = source.createReader();
                }
                Object v = reader.next();
                if (v instanceof HistogramLeafCellSerializedEnd) {
                    reader.getManager().getLogger().log(MailboxPersistable.logDebugPersist,
                            logPersistColor, "lookAhead finish: %s", reader);
                    reader.close();
                    reader = null;
                    finish = true;
                    return null;
                } else {
                    if (v instanceof HistogramLeafCellOnStorageFile) {
                        HistogramLeafCellOnStorageFile cos = (HistogramLeafCellOnStorageFile) v;
                        cos.getSource().setManager(source.getManager());
                        current = cos;
                        return lookAhead(tree);
                    } else {
                        return ((HistogramLeafCell) v).value;
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

        @Override
        public void write(Kryo kryo, Output output) {
            super.write(kryo, output);
            output.writeBoolean(this.finish);
            output.writeLong(source.remainingSize - remainingCount);
            kryo.writeClassAndObject(output, source);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            super.read(kryo, input);
            this.finish = input.readBoolean();
            this.skipCount = input.readLong();
            this.source = (PersistentFileReaderSourceWithSize) kryo.readClassAndObject(input);
            this.remainingCount = source.remainingSize;
            this.nextValue = null;
            this.current = null;
        }
    }

    public static class PersistentFileReaderSourceWithSize implements Serializable {
        public static final long serialVersionUID = 1L;
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
        NodeTreeData toData();
        PersistentFileReaderSource getSource();
        MailboxPersistable.PersistentFileManager getFileManager();
    }

    public static class HistogramNodeTreeOnStorage extends HistogramNodeTree implements HistogramNodeOnStorage {
        public static final long serialVersionUID = 1L;
        protected PersistentFileReaderSource source;
        protected boolean loaded;

        public HistogramNodeTreeOnStorage(NodeTreeData data, PersistentFileReaderSource source) {
            super(data.height, Collections.emptyList());
            this.source = source;
            this.keyStart = data.keyStart;
            this.keyEnd = data.keyEnd;
            this.size = data.size;
            loaded = false;
        }

        @Override
        public void initPersistent(MailboxPersistable.PersistentFileManager persistent) {
            if (source.getManager() == null) {
                source.setManager(persistent);
            }
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
            if (loaded) {
                super.updateChildren();
            }
        }

        @Override
        protected void updateKeys() {
            if (loaded) {
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
            return super.split(); //loaded children already have persistent
        }

        @Override
        public HistogramNode merge(int treeLimit, KeyComparator<?> comparator, HistogramNode lowerNode) {
            load();
            return super.merge(treeLimit, comparator, lowerNode);
        }

        @Override
        public HistogramNode split(long halfSize, long currentLeft) {
            load();
            return super.split(halfSize, currentLeft);
        }

        @Override
        public boolean isPersisted() {
            return !loaded;
        }

        @Override
        public HistogramNode load(HistogramPutContext context) {
            load();
            return this;
        }

        @Override
        public MailboxPersistable.PersistentFileManager getFileManager() {
            MailboxPersistable.PersistentFileManager m = source.getManager();
            if (m == null && parent instanceof HistogramNodeOnStorage) {
                m = ((HistogramNodeOnStorage) parent).getFileManager();
                source.setManager(m);
            }
            return m;
        }

        protected void load() {
            if (!loaded) {
                loaded = true;
                ArrayList<HistogramNode> cs = new ArrayList<>();
                getFileManager(); //setup manager for reading
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
                    r.getManager().getLogger().log(MailboxPersistable.logDebugPersist, logPersistColor, "close: %s", r);
                } catch (Exception ex) {
                    throw new RuntimeException("load: " + source, ex);
                }
            }
        }

        @Override
        public String toString() {
            return String.format("%s(size=%,d, keys=%s..%s, source=%s, persisted=%s)",
                    getClass().getSimpleName(), size, keyStart, keyEnd, source, isPersisted());
        }
    }

    public static class HistogramNodeLeafOnStorage extends HistogramNodeLeaf implements HistogramNodeOnStorage {
        public static final long serialVersionUID = 1L;
        protected PersistentFileReaderSource source;
        public HistogramNodeLeafOnStorage(NodeTreeData data, PersistentFileReaderSource source) {
            super(data.keyStart, null, data.height);
            this.source = source;
            this.size = data.size;
        }

        @Override
        public void initPersistent(MailboxPersistable.PersistentFileManager persistent) {
            if (source.getManager() == null) {
                source.setManager(persistent);
            }
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
        protected void putValueStruct(HistogramPutContext context) {
            if (MailboxPersistable.logPersist) { //use regular logPersist
                MailboxPersistable.PersistentFileManager m = getFileManager();
                if (m != null) {
                    m.getLogger().log(true, logPersistColor,
                            "HistogramNodeLeafOnStorage.putValueStruct: illegal operation %s",
                            this);
                }
            }
        }

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
        public MailboxPersistable.PersistentFileManager getFileManager() {
            MailboxPersistable.PersistentFileManager m = source.getManager();
            if (m == null && parent instanceof HistogramNodeOnStorage) {
                m = ((HistogramNodeOnStorage) parent).getFileManager();
                source.setManager(m);
            }
            return m;
        }

        @Override
        public HistogramNodeLeaf load(HistogramPutContext context) {
            getFileManager();
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
                List<HistogramNode> ns = getParent().getChildren();
                int idx = ns.indexOf(this);
                if (idx >= 0) {
                    ns.set(idx, leaf);
                }
                r.getManager().getLogger().log(MailboxPersistable.logDebugPersist, logPersistColor, "close: %s", r);
                return leaf;
            } catch (Exception ex) {
                throw new RuntimeException("load: " + source, ex);
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
     *   0,
     *   PersistentFileEnd
     *
     * leaf:
     *   long sibling,
     *   NodeTreeData (true, keyStart, keyEnd, height, size),
     *   Class nodeType,
     *   long listCount,
     *   long listPointer[0..listCount-1] (&list0, &list1, ..., &list(listCount-1)),
     *   long listSizes[0..listCount-1],
     *   HistogramLeafCell list0, ..., HistogramLeafCellSerializedEnd,
     *   HistogramLeafCell list1, ..., HistogramLeafCellSerializedEnd,
     *   ...
     *   HistogramLeafCell list(listSize-1), ..., HistogramLeafCellSerializedEnd
     *
     *  persisted:
     *   long sibling,
     *   PersistentFileReaderSource(path, offset) //the order is intended for distinguishing persisted or not
     *   NodeTreeData
     *  </pre>
     */
    public static class NodeTreeData implements Serializable {
        public static final long serialVersionUID = 1L;
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
