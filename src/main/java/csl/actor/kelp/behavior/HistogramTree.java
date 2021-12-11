package csl.actor.kelp.behavior;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.kelp.ActorKelpFunctions;
import csl.actor.persist.PersistentFileManager;

import java.io.Serializable;
import java.util.*;

public class HistogramTree implements Serializable, KryoSerializable, Cloneable {
    public static final long serialVersionUID = 1L;
    protected KeyHistograms.HistogramTreeNode root;
    protected ActorKelpFunctions.KeyComparator<?> comparator;
    protected int treeLimit;
    protected HistogramTreeNodeLeaf completed0;
    protected LinkedList<HistogramTreeNodeLeaf> completed = new LinkedList<>();
    protected long leafSize;
    protected long leafSizeNonZero;
    protected long nodeSizeOnMemory;
    protected long leafSizeOnMemory;

    protected PersistentFileManager persistent;

    public HistogramTree(ActorKelpFunctions.KeyComparator<?> comparator) {
        this(comparator, 32);
    }

    public HistogramTree(ActorKelpFunctions.KeyComparator<?> comparator, int treeLimit) {
        this(comparator, treeLimit, PersistentFileManager.getPersistentFile(null, ""));
    }

    public HistogramTree(ActorKelpFunctions.KeyComparator<?> comparator, int treeLimit, PersistentFileManager persistent) {
        this(null, comparator, treeLimit, persistent);
    }

    public HistogramTree(KeyHistograms.HistogramTreeNode root, ActorKelpFunctions.KeyComparator<?> comparator, int treeLimit,
                         PersistentFileManager persistent) {
        this.root = root;
        this.comparator = comparator;
        this.treeLimit = treeLimit;
        init(persistent);
        updateNodeSize(root);
    }

    public HistogramTree() {
    }

    public boolean hasSufficientPoints() {
        if (root == null) {
            return false;
        } else {
            return leafSize >= 2;
        }
    }

    public void put(Object key, KeyHistograms.HistogramPutContext context) {
        context.putTree = this;
        context.putTreeLimit = treeLimit;

        context.putIndexCurrentRangeStart = 0;
        context.putIndexCurrentRangeLength = 1;
        if (root == null) {
            root = context.createLeafWithCountUp(key, 0);
        } else {
            KeyHistograms.HistogramTreeNode l = root.put(comparator, key, context);
            if (l != null) {
                root = createNodeTree(root.height() + 1, treeLimit, KeyHistograms.sort(comparator, l, root));
                addNodeSizeOnMemory(1);
            }
        }
    }

    protected HistogramTreeNodeTable createNodeTree(int height, int treeLimit, KeyHistograms.HistogramTreeNode... children) {
        return new HistogramTreeNodeTable(height, treeLimit, children);
    }

    /**
     * @return a new split of left hand side
     */
    public HistogramTree split() {
        if (root == null) {
            return createTree(null);
        } else {
            HistogramTree left = createTree(root.split(root.size() / 2, 0));
            leafSize = 0;
            leafSizeNonZero = 0;
            prune(true);
            left.prune(true);

            splitCompleted(left);
            return left;
        }
    }

    public void fixNodeSIze() {
        updateNodeSize(root);
    }

    protected void updateNodeSize(KeyHistograms.HistogramTreeNode node) {
        if (node == root) {
            nodeSizeOnMemory = 0;
            leafSizeOnMemory = 0;
        }
        if (node instanceof HistogramTreeNodeTable) {
            if (!node.isPersisted()) {
                addNodeSizeOnMemory(1L);
            }
            ((HistogramTreeNodeTable) node).getChildrenOnMemory()
                    .forEach(this::updateNodeSize);
        } else if (node instanceof HistogramTreeNodeLeaf) {
            if (!node.isPersisted()) {
                addLeafSizeOnMemory(1L);
            }
        }
    }

    public HistogramTree createTree(KeyHistograms.HistogramTreeNode root) {
        return new HistogramTree(root, comparator, treeLimit, persistent);
    }

    @SuppressWarnings("unchecked")
    protected void splitCompleted(HistogramTree left) {
        updateNodeSize(root);
        left.updateNodeSize(left.getRoot());
        Object rightKey = splitPointAsRightHandSide(left);
        for (Iterator<HistogramTreeNodeLeaf> iter = completed.iterator(); iter.hasNext(); ) {
            HistogramTreeNodeLeaf l = iter.next();
            if (((ActorKelpFunctions.KeyComparator<Object>) comparator).compare(l.getKey(), rightKey) < 0) {
                //move to left
                left.complete(l);
                iter.remove();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public Object splitPointAsRightHandSide(HistogramTree splitLeft) {
        //this is the right hand side and the right has least one root node.
        // so the returned point is inclusive for the right split:
        //   subsequent split may be done with empty root, then it has both no point
        if (splitLeft.getLeafSize() == 0) {
            if (root == null) {
                return null;
            } else {
                return root.keyStart();
            }
        } else {
            return ((ActorKelpFunctions.KeyComparator<Object>) comparator).centerPoint(splitLeft.getRoot().keyEnd(), root.keyStart());
        }
    }

    public void merge(HistogramTree tree) {
        KeyHistograms.HistogramTreeNode rootAnother = tree.getRoot();
        if (root == null && rootAnother != null) {
            root = rootAnother;
        } else if (root != null && rootAnother == null) {
            //nothing
        } else if (root == null /*&& rootAnother == null*/) { //both
            //nothing
        } else {
            if (root.height() < rootAnother.height()) {
                root = merge(treeLimit, comparator, rootAnother, root);
            } else {
                root = merge(treeLimit, comparator, root, rootAnother);
            }
        }
        tree.getCompleted().forEach(this::complete);
        leafSize = 0;
        leafSizeNonZero = 0;
        prune(true);
        updateNodeSize(root);
    }

    protected KeyHistograms.HistogramTreeNode merge(int treeLimit, ActorKelpFunctions.KeyComparator<?> comparator, KeyHistograms.HistogramTreeNode target, KeyHistograms.HistogramTreeNode merged) {
        int height = target.height();

        HistogramTreeNodeTable targetNode;
        if (target instanceof HistogramTreeNodeLeaf) {
            targetNode = createNodeTree(
                    (height > 0 ? height : 1),
                    treeLimit,
                    (height > 0 ? target.increaseHeight(-1) : target));
        } else {
            targetNode = (HistogramTreeNodeTable) target;
        }

        KeyHistograms.HistogramTreeNode newNode = targetNode.merge(this, treeLimit, comparator, merged);
        if (newNode != null) {
            return createNodeTree(height + 1, treeLimit, KeyHistograms.sort(comparator, targetNode, newNode));
        } else {
            return targetNode;
        }
    }

    public void complete(HistogramTreeNodeLeaf n) {
        if (completed0 == null) {
            completed0 = n;
        } else {
            completed.add(n);
        }
    }

    public HistogramTreeNodeLeaf takeCompleted() {
        if (completed0 != null) {
            HistogramTreeNodeLeaf r = completed0;
            if (completed.isEmpty()) {
                completed0 = null;
            } else {
                completed0 = completed.removeFirst();
            }
            return r;
        } else {
            return null;
        }
    }

    public KeyHistograms.HistogramTreeNode getRoot() {
        return root;
    }

    public long getTreeSize() {
        KeyHistograms.HistogramTreeNode root = this.root;
        return root == null ? 0 : root.size();
    }

    public long getTreeSizeForReduceCheck() {
        return getTreeSize();
    }

    public int getTreeHeight() {
        return root == null ? 0 : root.height();
    }

    /**
     * @return implementation field getter
     */
    public ActorKelpFunctions.KeyComparator<?> getComparator() {
        return comparator;
    }

    public boolean hasCompleted() {
        return completed0 != null || !completed.isEmpty();
    }

    public List<HistogramTreeNodeLeaf> getCompleted() {
        if (completed0 != null) {
            ArrayList<HistogramTreeNodeLeaf> l = new ArrayList<>();
            l.add(completed0);
            l.addAll(completed);
            return l;
        } else {
            return completed;
        }
    }

    public void prune() {
        prune(false);
    }

    protected void prune(boolean countUpLeafSize) {
        if (root != null) {
            if (root.prune(this, countUpLeafSize)) {
                root = null;
            }
        }
    }

    public int getTreeLimit() {
        return treeLimit;
    }

    public void incrementLeafSize(int n) {
        leafSize += n;
        leafSizeOnMemory += n;
    }

    public void incrementLeafSizeNonZero(int n) {
        leafSizeNonZero += n;
    }

    public long getLeafSize() {
        return leafSize;
    }

    public long getLeafSizeNonZero() {
        return leafSizeNonZero;
    }

    public double getLeafSizeNonZeroRate() {
        if (leafSize == 0) {
            return 1.0;
        }
        return ((double) leafSizeNonZero) / (double) leafSize;
    }

    public long getTreeSizeOnMemory() {
        return getTreeSize();
    }

    /**
     * @return the on-memory table-nodes on the tree. it means {@link KeyHistograms.HistogramTreeNode#isPersisted()}=false nodes
     */
    public long getNodeSizeOnMemory() { //nonLeaf
        return nodeSizeOnMemory;
    }

    /**
     * @return the on-memory leaves on the tree. it means {@link KeyHistograms.HistogramTreeNode#isPersisted()}=false nodes
     */
    public long getLeafSizeOnMemory() {
        return leafSizeOnMemory;
    }

    public void addNodeSizeOnMemory(long n) {
        this.nodeSizeOnMemory += n;
    }

    public void addLeafSizeOnMemory(long n) {
        this.leafSizeOnMemory += n;
    }

    public KeyHistograms.HistogramLeafList createEmptyList() {
        return new KeyHistograms.HistogramLeafList();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void read(Kryo kryo, Input input) {
        this.root = (KeyHistograms.HistogramTreeNode) kryo.readClassAndObject(input);
        this.comparator = (ActorKelpFunctions.KeyComparator<?>) kryo.readClassAndObject(input);
        this.completed0 = (HistogramTreeNodeLeaf) kryo.readClassAndObject(input);
        this.completed = (LinkedList<HistogramTreeNodeLeaf>) kryo.readClassAndObject(input);
        this.leafSize = input.readVarLong(true);
        this.leafSizeNonZero = input.readVarLong(true);
        this.treeLimit = input.readVarInt(true);
        this.nodeSizeOnMemory = input.readVarLong(true);
        this.leafSizeOnMemory = input.readVarLong(true);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeClassAndObject(output, this.root);
        kryo.writeClassAndObject(output, this.comparator);
        kryo.writeClassAndObject(output, this.completed0);
        kryo.writeClassAndObject(output, this.completed);
        output.writeVarLong(this.leafSize, true);
        output.writeVarLong(this.leafSizeNonZero, true);
        output.writeVarInt(this.treeLimit, true);
        output.writeVarLong(this.nodeSizeOnMemory, true);
        output.writeVarLong(this.leafSizeOnMemory, true);
    }

    public HistogramTree copy() {
        try {
            HistogramTree tree = (HistogramTree) super.clone();
            Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> leafMap = new IdentityHashMap<>();
            if (root != null) {
                tree.root = root.copy(leafMap);
            }
            if (completed0 != null) {
                tree.completed0 = (HistogramTreeNodeLeaf) leafMap.get(completed0);
            }
            tree.completed = new LinkedList<>();
            completed.forEach(i ->
                    tree.completed.add((HistogramTreeNodeLeaf) leafMap.get(i)));
            return tree;
        } catch (CloneNotSupportedException ce) {
            throw new RuntimeException(ce);
        }
    }

    public HistogramTree init(PersistentFileManager persistent) {
        if (this.persistent == null) {
            this.persistent = persistent;
            if (root != null) {
                root.initPersistent(persistent);
            }
        }
        return this;
    }

    public PersistentFileManager getPersistent() {
        return persistent;
    }

    public boolean needToReduce() {
        return false;
    }

    public void setRoot(KeyHistograms.HistogramTreeNode root) {
        this.root = root;
    }

    public void restructure(boolean load) {
        if (!load && noPersistedTable(root)) {
            return;
        }
        RestructureIterator res = leafIterator(load);
        this.root = null;
        clearTree(false);
        HistogramTreeNodeLeaf leaf;
        while ((leaf = res.next()) != null) {
            if (leaf.size() != 0) {
                leafSize++;
                leafSizeNonZero++;
                if (!leaf.isPersisted()) {
                    addLeafSizeOnMemory(1L);
                }
                leaf.height = 0;
                if (root == null) {
                    root = leaf;
                } else {
                    KeyHistograms.HistogramTreeNode newNode = root.put(comparator, this, leaf);
                    if (newNode != null) {
                        root = createNodeTree(root.height() + 1, treeLimit, KeyHistograms.sort(comparator, newNode, root));
                        addNodeSizeOnMemory(1);
                    }
                }
            }
        }
    }

    protected boolean noPersistedTable(KeyHistograms.HistogramTreeNode node) {
        if (node.isPersisted() && node instanceof HistogramTreeNodeTable) {
            return false;
        } else if (node instanceof HistogramTreeNodeTable) {
            for (KeyHistograms.HistogramTreeNode c : ((HistogramTreeNodeTable) node).getChildrenOnMemory()) {
                if (!noPersistedTable(c)) {
                    return false;
                }
            }
            return true;
        } else {
            return true;
        }
    }

    protected void clearTree(boolean clearCompleted) {
        leafSize = 0;
        leafSizeNonZero = 0;
        nodeSizeOnMemory = 0;
        leafSizeOnMemory = 0;
        if (clearCompleted) {
            clearCompleted();
        }
    }

    protected void clearCompleted() {
        completed0 = null;
        completed.clear();
    }

    public RestructureIterator leafIterator(boolean load) {
        return new RestructureIterator(this, root, load);
    }

    public static class RestructureIterator {
        protected HistogramTree tree;
        protected boolean load;
        protected LinkedList<Iterator<KeyHistograms.HistogramTreeNode>> parents = new LinkedList<>();
        protected Iterator<KeyHistograms.HistogramTreeNode> current;
        public RestructureIterator(HistogramTree tree, KeyHistograms.HistogramTreeNode root, boolean load) {
            this.tree = tree;
            this.load = load;
            if (root instanceof HistogramTreeNodeTable) {
                current = iteratorFromTable(root);
            } else if (root instanceof HistogramTreeNodeLeaf) {
                current = Collections.singletonList(root).iterator();
            } else {
                current = Collections.emptyIterator();
            }
        }

        protected Iterator<KeyHistograms.HistogramTreeNode> iteratorFromTable(KeyHistograms.HistogramTreeNode node) {
            return (load ?
                    ((HistogramTreeNodeTable) node).getChildren(tree) :
                    ((HistogramTreeNodeTable) node).getChildrenOnMemory()).iterator();
        }

        public HistogramTreeNodeLeaf next() {
            if (current.hasNext()) {
                KeyHistograms.HistogramTreeNode next = current.next();
                if (next instanceof HistogramTreeNodeLeaf) {
                    return (HistogramTreeNodeLeaf) next;
                } else if (next instanceof HistogramTreeNodeTable) {
                    parents.addLast(current);
                    current = iteratorFromTable(next);
                    return next();
                } else {
                    return null;
                }
            } else if (!parents.isEmpty()) {
                current = parents.removeLast();
                return next();
            } else {
                return null;
            }
        }
    }

    public void close() { }
}
