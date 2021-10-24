package csl.actor.kelp.behavior;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.kelp.ActorKelpFunctions.KeyComparator;
import csl.actor.persist.PersistentFileManager;

import java.io.Serializable;
import java.util.*;

public class KeyHistograms {
    protected PersistentFileManager persistent;

    public KeyHistograms(PersistentFileManager persistent) {
        this.persistent = persistent;
    }

    public KeyHistograms() {
        this(PersistentFileManager.getPersistentFile(null, ""));
    }

    /** @return implementation field getter */
    public PersistentFileManager getPersistent() {
        return persistent;
    }

    public HistogramTree create(KeyComparator<?> comparator, int treeLimit) {
        return new HistogramTree(comparator, treeLimit, persistent);
    }

    public HistogramTree init(HistogramTree tree) {
        return tree.init(persistent);
    }

    public static class HistogramTree implements Serializable, KryoSerializable, Cloneable {
        public static final long serialVersionUID = 1L;
        protected HistogramNode root;
        protected KeyComparator<?> comparator;

        protected int treeLimit;

        protected LinkedList<HistogramNodeLeaf> completed = new LinkedList<>();
        protected long leafSize;
        protected long leafSizeNonZero;

        protected PersistentFileManager persistent;

        public HistogramTree(KeyComparator<?> comparator) {
            this(comparator, 32);
        }

        public HistogramTree(KeyComparator<?> comparator, int treeLimit) {
            this(comparator, treeLimit, PersistentFileManager.getPersistentFile(null, ""));
        }

        public HistogramTree(KeyComparator<?> comparator, int treeLimit, PersistentFileManager persistent) {
            this(null, comparator, treeLimit, persistent);
        }

        public HistogramTree(HistogramNode root, KeyComparator<?> comparator, int treeLimit,
                             PersistentFileManager persistent) {
            this.root = root;
            this.comparator = comparator;
            this.treeLimit = treeLimit;
            init(persistent);
        }

        public HistogramTree() {}

        public boolean hasSufficientPoints() {
            if (root == null) {
                return false;
            } else {
                return leafSize >= 2;
            }
        }

        public void put(Object key, HistogramPutContext context) {
            context.putTree = this;
            context.putTreeLimit = treeLimit;

            context.putIndexCurrentRangeStart = 0;
            context.putIndexCurrentRangeLength = 1;
            if (root == null) {
                root = context.createLeafWithCountUp(key, 0);
            } else {
                HistogramNode l = root.put(comparator, key, context);
                if (l != null) {
                    root = createNodeTree(root.height() + 1, treeLimit, sort(comparator, l, root));
                }
            }
        }

        protected HistogramNodeTree createNodeTree(int height, int treeLimit, HistogramNode... children) {
            return new HistogramNodeTree(height, treeLimit, children);
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

        public HistogramTree createTree(HistogramNode root) {
            return new HistogramTree(root, comparator, treeLimit, persistent);
        }

        @SuppressWarnings("unchecked")
        protected void splitCompleted(HistogramTree left) {
            Object rightKey = splitPointAsRightHandSide(left);
            for (Iterator<HistogramNodeLeaf> iter = completed.iterator(); iter.hasNext(); ) {
                HistogramNodeLeaf l = iter.next();
                if (((KeyComparator<Object>) comparator).compare(l.getKey(), rightKey) < 0) {
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
                return ((KeyComparator<Object>) comparator).centerPoint(splitLeft.getRoot().keyEnd(), root.keyStart());
            }
        }

        public void merge(HistogramTree tree) {
            HistogramNode rootAnother = tree.getRoot();
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

            completed.addAll(tree.getCompleted());
            leafSize = 0;
            leafSizeNonZero = 0;
            prune(true);
        }

        protected HistogramNode merge(int treeLimit, KeyComparator<?> comparator, HistogramNode target, HistogramNode merged) {
            int height = target.height();

            HistogramNodeTree targetNode;
            if (target instanceof HistogramNodeLeaf) {
                targetNode = createNodeTree(
                        (height > 0 ? height : 1),
                        treeLimit,
                        (height > 0 ? target.increaseHeight(-1) : target));
            } else {
                targetNode = (HistogramNodeTree) target;
            }

            HistogramNode newNode = targetNode.merge(treeLimit, comparator, merged);
            if (newNode != null) {
                return createNodeTree(height + 1, treeLimit, sort(comparator, targetNode, newNode));
            } else {
                return targetNode;
            }
        }

        public void complete(HistogramNodeLeaf n) {
            completed.add(n);
        }

        public HistogramNodeLeaf takeCompleted() {
            if (completed.isEmpty()) {
                return null;
            } else {
                return completed.removeFirst();
            }
        }

        public HistogramNode getRoot() {
            return root;
        }

        public long getTreeSize() {
            return root == null ? 0 : root.size();
        }

        public long getTreeSizeForReduceCheck() {
            return getTreeSize();
        }

        public int getTreeHeight() {
            return root == null ? 0 : root.height();
        }

        /** @return implementation field getter */
        public KeyComparator<?> getComparator() {
            return comparator;
        }

        /** @return implementation field getter */
        public LinkedList<HistogramNodeLeaf> getCompleted() {
            return completed;
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

        public HistogramLeafList createEmptyList() {
            return new HistogramLeafList();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void read(Kryo kryo, Input input) {
            this.root = (HistogramNode) kryo.readClassAndObject(input);
            this.comparator = (KeyComparator<?>) kryo.readClassAndObject(input);
            this.completed = (LinkedList<HistogramNodeLeaf>) kryo.readClassAndObject(input);
            this.leafSize = input.readLong();
            this.leafSizeNonZero = input.readLong();
            this.treeLimit = input.readInt();
        }

        @Override
        public void write(Kryo kryo, Output output) {
            kryo.writeClassAndObject(output, this.root);
            kryo.writeClassAndObject(output, this.comparator);
            kryo.writeClassAndObject(output, this.completed);
            output.writeLong(this.leafSize);
            output.writeLong(this.leafSizeNonZero);
            output.writeInt(this.treeLimit);
        }

        public HistogramTree copy() {
            try {
                HistogramTree tree = (HistogramTree) super.clone();
                Map<HistogramNode,HistogramNode> leafMap = new IdentityHashMap<>();
                if (root != null) {
                    tree.root = root.copy(leafMap);
                }
                tree.completed = new LinkedList<>();
                completed.forEach(i ->
                        tree.completed.add((HistogramNodeLeaf) leafMap.get(i)));
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
            return true;
        }
    }

    public static abstract class HistogramPutContext {
        /** (in most cases) fixed at construction */
        public int putRequiredSize;
        /** fixed by put and processing methods */
        public HistogramTree putTree;
        /** the value set after key-matching */
        public Comparable<?> putPosition;
        /** set by put method */
        public Object putValue;
        /** set by put and processing methods */
        public int putTreeLimit;

        /** dynamically updated by put processes */
        public float putIndexCurrentRangeStart;
        /** dynamically updated by put processes */
        public float putIndexCurrentRangeLength;

        /**
         * creates a new leaf. called under put processes
         * @param key the key for the leaf
         * @param height the height of the leaf
         * @return a new leaf
         */
        protected abstract HistogramNodeLeaf createLeaf(Object key, int height);

        /**
         * creates a new leaf (by {@link #createLeaf(Object, int)})
         * and puts the value set to this. called under put processes. 
         *  increments the size of {@link #putTree}
         * @param key the key for the leaf
         * @param height the height of the leaf
         * @return a new leaf with a value
         */
        public HistogramNodeLeaf createLeafWithCountUp(Object key, int height) {
            HistogramNodeLeaf l = createLeaf(key, height);
            l.putValue(this);
            putTree.incrementLeafSize(1);
            return l;
        }

        /**
         * just calls {@link #putTree}'s {@link HistogramTree#createEmptyList()} 
         * @return a new empty list for constructing a new leaf
         */
        public HistogramLeafList createEmptyList() {
            return putTree.createEmptyList();
        }

        /**
         * just calls {@link #putTree}'s 
         *  {@link HistogramTree#incrementLeafSizeNonZero(int)} with +1
         */
        public void incrementLeafSizeNonZero() {
            putTree.incrementLeafSizeNonZero(1);
        }
        
        public Comparable<?> position(HistogramNodeLeaf leaf) {
            return putPosition;
        }

        /**
         * just calls {@link #putTree}'s {@link HistogramTree#complete(HistogramNodeLeaf)}
         * @param leaf the completed leaf
         */
        public void complete(HistogramNodeLeaf leaf) {
            putTree.complete(leaf);
        }
    }


    public interface HistogramNode extends Serializable {
        long size();
        int height();
        HistogramNode increaseHeight(int heightDelta);

        default HistogramNode load(HistogramPutContext context) {
            return this;
        }

        HistogramNode put(KeyComparator<?> comparator, Object key, HistogramPutContext context);

        /**
         * @param comparator the comparator
         * @param key a compared key
         * @return comparator.compare(key, this.key)
         */
        int keyIn(KeyComparator<?> comparator, Object key);
        Object keyStart();
        Object keyEnd();

        /**
         * @param halfSize the half of total size of the entire tree
         * @param currentLeft recursively summed up sizes of left siblings
         * @return a new left hand side sibling or null
         */
        HistogramNode split(long halfSize, long currentLeft);

        void setParent(HistogramNodeTree node);
        HistogramNodeTree getParent();

        boolean prune(HistogramTree tree, boolean countUpLeafSize);

        void initPersistent(PersistentFileManager persistent);

        HistogramNode copy(Map<HistogramNode,HistogramNode> oldToNew);
    }

    @SuppressWarnings("unchecked")
    public static HistogramNode[] sort(KeyComparator<?> comparator, HistogramNode... nodes) {
        Arrays.sort(nodes, (n1,n2) ->
                ((KeyComparator<Object>)comparator).compare(n1.keyStart(), n2.keyStart()));
        return nodes;
    }

    public static class HistogramNodeTree implements HistogramNode, Cloneable {
        public static final long serialVersionUID = 1L;
        public HistogramNodeTree parent;
        public List<HistogramNode> children;
        public long size;
        public Object keyStart;
        public Object keyEnd;
        public int height;

        public HistogramNodeTree() {}

        public HistogramNodeTree(int height, List<HistogramNode> children) {
            this.height = height;
            this.children = children;
            updateChildren();
        }

        public HistogramNodeTree(int height, int capacity, HistogramNode... children) {
            this.height = height;
            this.children = new ArrayList<>(Math.min(200, capacity));
            this.children.addAll(Arrays.asList(children));
            updateChildren();
        }

        @Override
        public HistogramNodeTree copy(Map<HistogramNode, HistogramNode> oldToNew) {
            try {
                HistogramNodeTree node = (HistogramNodeTree) super.clone();
                oldToNew.put(this, node);

                if (parent != null) {
                    node.parent = (HistogramNodeTree) oldToNew.get(parent);
                }
                node.children = new ArrayList<>(children.size());
                for (HistogramNode child : children) {
                    node.children.add(child.copy(oldToNew));
                }
                return node;
            } catch (CloneNotSupportedException ce){
                throw new RuntimeException(ce);
            }
        }

        /**
         * @return the list of children. the returned list is actual reference of the node.
         *   elements in the list might not yet be loaded.
         */
        public List<HistogramNode> getChildren() {
            return children;
        }

        @Override
        public HistogramNodeTree getParent() {
            return parent;
        }

        @Override
        public void setParent(HistogramNodeTree parent) {
            this.parent = parent;
        }

        @Override
        public int height() {
            return height;
        }

        @Override
        public HistogramNode increaseHeight(int heightDelta) {
            height += heightDelta;
            children.forEach(c -> c.increaseHeight(heightDelta));
            return this;
        }

        @Override
        public long size() {
            return size;
        }

        @SuppressWarnings("unchecked")
        @Override
        public int keyIn(KeyComparator<?> comparator, Object key) {
            int sc = ((KeyComparator<Object>) comparator).compare(key, this.keyStart);
            if (sc < 0) { //key < keyStart
                return sc;
            } else if (sc > 0) {
                int ec = ((KeyComparator<Object>) comparator).compare(key, this.keyEnd);
                if (ec <= 0) { //key < keyEnd
                    return 0; //within
                } else {
                    return ec;
                }
            } else {
                return 0;
            }
        }

        @Override
        public Object keyStart() {
            return keyStart;
        }

        @Override
        public Object keyEnd() {
            return keyEnd;
        }

        @Override
        public HistogramNode put(KeyComparator<?> comparator, Object key, HistogramPutContext context) {
            size++;

            HistogramNode newNode = null;
            int index = search(comparator, key);
            updatePutIndex(context, index);
            if (index >= 0) { //found
                newNode = putChildAt(comparator, key, context, index);
                if (newNode == null) {
                    return null;
                }
            } else {
                index = -index - 1;
            }
            int treeLimit = context.putTreeLimit;
            if (newNode != null || height <= 1) { //appropriate range or bottom
                if (newNode == null) {
                    newNode = context.createLeafWithCountUp(key, height - 1);
                }
                children.add(index, newNode);
                newNode.setParent(this);
            } else { //upper tree
                index = selectChildTree(index, treeLimit);
                newNode = putChildAt(comparator, key, context, index);
                HistogramNode childTarget = children.get(index);
                if (newNode != null && childTarget instanceof HistogramNodeLeaf) { //it can create a new sub-tree
                    newNode = createNodeTree(height - 1, treeLimit, sort(comparator,
                            childTarget.increaseHeight(-1), newNode.increaseHeight(-1)));
                    children.set(index, newNode); //replace
                    newNode.setParent(this);
                } else if (newNode != null) {
                    children.add(index, newNode); //tree.put always returns a new left sibling
                    newNode.setParent(this);
                }
            }
            if (children.size() > treeLimit) {
                return split();
            } else {
                if (index <= 0 ||
                        index >= children.size() - 2) {  //consider increased size
                    updateKeys();
                }
                return null;
            }
        }

        protected HistogramNode putChildAt(KeyComparator<?> comparator, Object key, HistogramPutContext context, int index) {
            HistogramNode child = children.get(index);
            HistogramNode newChild = child.load(context);
            if (newChild != child) {
                children.set(index, newChild);
            }
            return newChild.put(comparator, key, context);
        }

        protected HistogramNode split() {
            int n = children.size() / 2;
            List<HistogramNode> newChildren = new ArrayList<>(children.subList(n, children.size()));
            HistogramNode l = createNodeTree(height, new ArrayList<>(children.subList(0, n)));
            this.children = newChildren;
            updateChildren();
            return l;
        }

        /**
         * @param comparator a comparator for keys
         * @param key the key
         * @return the index containing the key,
         *    or -(the insertion point) -1 which has the same semantics of JDK binarySearch
         *     e.g. {@link Collections#binarySearch(List, Object, Comparator)}
         */
        protected int search(KeyComparator<?> comparator, Object key) {
            int start = 0;
            int end = children.size() - 1;
            while (start <= end) {
                int i = (start + end) / 2;
                HistogramNode n = children.get(i);
                int c = n.keyIn(comparator, key);
                if (c > 0) {
                    start = i + 1;
                } else if (c < 0) {
                    end = i - 1;
                } else {
                    //found
                    return i;
                }
            }
            return -(start + 1);
        }

        protected void updatePutIndex(HistogramPutContext context, int index) {
            float s = context.putIndexCurrentRangeStart;
            float l = context.putIndexCurrentRangeLength;
            float cs = children.size();
            float p = Math.min(1.0f, ((float) Math.abs(index)) / cs);
            context.putIndexCurrentRangeStart = s + l * p;
            context.putIndexCurrentRangeLength = l * (1f / cs);
        }

        protected int selectChildTree(int index, int treeLimit) {
            if (index - 1 < 0) {
                if (index >= children.size()) { //empty
                    HistogramNodeTree childTarget = createNodeTree(height - 1, treeLimit);
                    children.add(childTarget);
                    childTarget.setParent(this);
                    updateKeys();
                    return children.size() - 1;
                } else { //left end
                    return index;
                }
            } else {
                if (index >= children.size()) { //right end
                    return index - 1;
                } else {
                    if (children.get(index - 1).size() < children.get(index).size()) { //prefer smaller sub-tree
                        return index - 1;
                    } else {
                        return index;
                    }
                }
            }
        }

        protected HistogramNodeTree createNodeTree(int height, int treeLimit, HistogramNode... children) {
            return new HistogramNodeTree(height, treeLimit, children);
        }

        protected HistogramNodeTree createNodeTree(int height, List<HistogramNode> children) {
            return new HistogramNodeTree(height, children);
        }

        protected void updateChildren() {
            long size = 0;
            for (HistogramNode n : children) {
                size += n.size();
                n.setParent(this);
            }
            this.size = size;
            updateKeys();
        }

        protected void updateKeys() {
            if (!children.isEmpty()) {
                keyStart = children.get(0).keyStart();
                keyEnd = children.get(children.size() - 1).keyEnd();
            }
        }

        @Override
        public HistogramNode split(long halfSize, long currentLeft) {
            List<HistogramNode> lefts = new ArrayList<>(children.size());
            int i = 0;
            for (HistogramNode n : children) {
                if (currentLeft + n.size() >= halfSize) {
                    HistogramNode nLeft = n.split(halfSize, currentLeft);
                    if (nLeft != null) {
                        lefts.add(nLeft);
                    }
                    List<HistogramNode> rights = new ArrayList<>(children.size());
                    rights.addAll(children.subList(i, children.size())); //includes n
                    this.children = rights;
                    updateChildren();
                    if (!lefts.isEmpty()) {
                        return createNodeTree(height, lefts);
                    } else {
                        return null;
                    }
                } else {
                    lefts.add(n);
                    currentLeft += n.size();
                }
                ++i;
            }
            return null;
        }

        public void reduce(int size) {
            this.size -= size;
            if (parent != null) {
                parent.reduce(size);
            }
        }

        @Override
        public boolean prune(HistogramTree tree, boolean countUpLeafSize) {
            boolean zero = (size == 0);
            if (children.removeIf(c -> c.prune(tree, countUpLeafSize))) {
                if (!zero) {
                    updateKeys();
                }
            }
            return zero;
        }

        @Override
        public String toString() {
            return String.format("%s(size=%,d, keys=%s..%s)",
                    getClass().getSimpleName(), size, keyStart, keyEnd);
        }

        public HistogramNode merge(int treeLimit, KeyComparator<?> comparator, HistogramNode lowerNode) {
            int h1 = this.height();
            int h2 = lowerNode.height();
            if (h1 == h2) {
                if (lowerNode instanceof HistogramNodeLeaf) {
                    return merge(treeLimit, comparator, lowerNode.increaseHeight(-1)); //always h2 > 0 because the height of a HistogramNodeTree always > 0
                } else if (lowerNode instanceof HistogramNodeTree) {
                    HistogramNodeTree node = (HistogramNodeTree) lowerNode;
                    int r = keyIn(comparator, node.keyEnd());
                    if (r < 0) {
                        children.addAll(0, node.getChildren());
                    } else { //r > 0: no r==0
                        children.addAll(node.getChildren());
                    }
                    if (children.size() > treeLimit) {
                        return split();
                    } else {
                        updateChildren();
                        return null;
                    }
                }
            } else { //h1 > h2
                if (children.isEmpty()) {
                    int upHeight = h1 + 1 - h2;
                    children.add(lowerNode.increaseHeight(upHeight));
                    updateChildren();
                } else {
                    int r = keyIn(comparator, lowerNode.keyEnd());
                    HistogramNode target;
                    int insPoint;
                    if (r < 0) {
                        insPoint = 0;
                    } else { //r > 0: no r==0
                        insPoint = children.size() - 1;
                    }
                    target = children.get(insPoint);
                    if (target instanceof HistogramNodeTree) {
                        HistogramNode newNode = ((HistogramNodeTree) target).merge(treeLimit, comparator, lowerNode);
                        if (newNode != null) { //always returns left: newNode, target
                            children.add(insPoint, newNode);
                        }
                    } else {
                        if (target.keyIn(comparator, lowerNode.keyEnd()) < 0) { //lowerNode, target
                            children.add(insPoint, lowerNode);
                        } else { //target, lowerNode
                            children.add(insPoint + 1, lowerNode);
                        }
                    }
                    if (children.size() > treeLimit) {
                        return split();
                    } else {
                        updateChildren();
                    }
                }
            }
            return null;
        }

        @Override
        public void initPersistent(PersistentFileManager persistent) {
            children.forEach(h ->
                    h.initPersistent(persistent));
        }
    }


    public static abstract class HistogramNodeLeaf implements HistogramNode, Cloneable {
        public static final long serialVersionUID = 1L;
        public HistogramNodeTree parent;
        public Object key;
        public long size;
        public int height;

        public HistogramNodeLeaf() {}

        public HistogramNodeLeaf(Object key, HistogramPutContext context, int height) {
            this.key = key;
            this.height = height;
            initStruct(context);
        }

        @Override
        public HistogramNode copy(Map<HistogramNode, HistogramNode> oldToNew) {
            try {
                HistogramNodeLeaf node = (HistogramNodeLeaf) super.clone();
                oldToNew.put(this, node);
                if (parent != null) {
                    node.parent = (HistogramNodeTree) oldToNew.get(parent);
                }
                return node;
            } catch (CloneNotSupportedException ce) {
                throw new RuntimeException(ce);
            }
        }

        public void setSize(long size) {
            this.size = size;
        }

        public Object getKey() {
            return key;
        }

        @Override
        public HistogramNodeTree getParent() {
            return parent;
        }

        @Override
        public void setParent(HistogramNodeTree parent) {
            this.parent = parent;
        }

        @Override
        public int height() {
            return height;
        }

        @Override
        public HistogramNode increaseHeight(int heightDelta) {
            height += heightDelta;
            return this;
        }

        @Override
        public long size() {
            return size;
        }

        @SuppressWarnings("unchecked")
        @Override
        public int keyIn(KeyComparator<?> comparator, Object key) {
            return ((KeyComparator<Object>) comparator).compare(key, this.key);
        }

        @Override
        public Object keyStart() {
            return key;
        }

        @Override
        public Object keyEnd() {
            return key;
        }

        @SuppressWarnings("unchecked")
        @Override
        public HistogramNode put(KeyComparator<?> comparator, Object key, HistogramPutContext context) {
            int c = ((KeyComparator<Object>) comparator).compare(key, this.key);
            if (c == 0) {
                putValue(context);
                return null;
            } else {
                return context.createLeafWithCountUp(key, height);
            }
        }

        protected void putValue(HistogramPutContext context) {
            putValueStruct(context);
            if (size == 0) {
                context.incrementLeafSizeNonZero();
            }
            size++;
            if (completedAfterPut(context)) {
                context.complete(this);
            }
        }

        @Override
        public HistogramNode split(long halfSize, long currentLeft) {
            return null;
        }

        protected abstract void initStruct(HistogramPutContext context);
        protected abstract void putValueStruct(HistogramPutContext context);

        protected abstract boolean completedAfterPut(HistogramPutContext context);

        public void afterTake(int removedSize, HistogramTree tree) {
            size -= removedSize;
            if (size <= 0) {
                tree.incrementLeafSizeNonZero(-1);
            }
            if (parent != null) {
                parent.reduce(removedSize);
            }
        }

        @Override
        public String toString() {
            return String.format("%s(size=%,d, keys=%s)",
                    getClass().getSimpleName(), size, key);
        }

        @Override
        public boolean prune(HistogramTree tree, boolean countUpLeafSize) {
            if (size == 0) {
                if (!countUpLeafSize) {
                    tree.incrementLeafSize(-1);
                }
                return true;
            } else {
                if (countUpLeafSize) {
                    tree.incrementLeafSize(1);
                    tree.incrementLeafSizeNonZero(1);
                }
                return false;
            }
        }

        public abstract List<HistogramLeafList> getStructList();
        public abstract void setStructList(int i, HistogramLeafList list);

        public HistogramNodeLeaf load(HistogramPutContext context) {
            return this;
        }

        @Override
        public void initPersistent(PersistentFileManager persistent) { }
    }

    public static class HistogramNodeLeafMap extends HistogramNodeLeaf {
        public static final long serialVersionUID = 1L;
        public TreeMap<Comparable<?>, HistogramLeafList> values;
        public int nextPosition;

        public HistogramNodeLeafMap() {}

        public HistogramNodeLeafMap(Object key, HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        public HistogramNodeLeafMap copy(Map<KeyHistograms.HistogramNode, KeyHistograms.HistogramNode> oldToNew) {
            HistogramNodeLeafMap node = (HistogramNodeLeafMap) super.copy(oldToNew);
            node.values = new TreeMap<>();
            values.forEach((k,v) ->
                    node.values.put(k, v == null ? null : v.copy()));
            return node;
        }

        @Override
        protected void initStruct(HistogramPutContext context) {
            values = new TreeMap<>();
        }

        @Override
        protected void putValueStruct(HistogramPutContext context) {
            values.compute(context.position(this),
                    (p, l) -> HistogramLeafList.add(
                            l == null ? context.createEmptyList() : l,
                            context.putTree,
                            context.putValue));
        }

        @Override
        protected boolean completedAfterPut(HistogramPutContext context) {
            return context.putRequiredSize <= values.size();
        }

        public int nextPosition(HistogramPutContext context) {
            int p = nextPosition;
            ++nextPosition;
            if (context.putRequiredSize > 0 && context.putRequiredSize <= nextPosition) {
                nextPosition = 0;
            }
            return p;
        }

        /** @return implementation field getter */
        public TreeMap<Comparable<?>, HistogramLeafList> getValues() {
            return values;
        }

        /** @return implementation field getter */
        public int getNextPosition() {
            return nextPosition;
        }

        public Object[] take(int requiredSize, HistogramTree tree) {
            if (values.size() >= requiredSize) {
                Object[] res = new Object[requiredSize];
                int i = 0;
                for (Iterator<Map.Entry<Comparable<?>, HistogramLeafList>> ei = values.entrySet().iterator();
                     ei.hasNext(); ) {
                    Map.Entry<Comparable<?>, HistogramLeafList> e = ei.next();
                    HistogramLeafList ev = e.getValue();
                    if (!ev.isEmpty()) {
                        res[i] = ev.poll(tree);
                        if (res[i] != null) {
                            ++i;
                        }
                    }
                    if (ev.isEmpty()) {
                        ei.remove();
                    }
                }
                afterTake(requiredSize, tree);
                return res;
            } else {
                return null;
            }
        }

        @Override
        public List<HistogramLeafList> getStructList() {
            return new ArrayList<>(this.values.values());
        }

        @Override
        public void setStructList(int i, HistogramLeafList list) {
            values.put(i, list);
        }
    }


    public static class HistogramPutContextMap extends HistogramPutContext {
        protected HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeafMap(key, this, height);
        }

        public Comparable<?> position(HistogramNodeLeaf leaf) {
            if (putPosition == null) {
                return ((HistogramNodeLeafMap) leaf).nextPosition(this);
            } else {
                return putPosition;
            }
        }

        public Object[] take(HistogramTree tree, HistogramNodeLeaf leaf) {
            if (putRequiredSize <= leaf.size()) {
                HistogramNodeLeafMap m = (HistogramNodeLeafMap) leaf.load(this);
                return m.take(putRequiredSize, tree);
            } else {
                return null;
            }
        }
    }

    public static class HistogramLeafList implements /*Iterable<Object>, */Serializable, KryoSerializable, Cloneable {
        public static final long serialVersionUID = 1L;
        public HistogramLeafCell head;
        public HistogramLeafCell tail;

        public HistogramLeafList copy() {
            try {
                HistogramLeafList list = (HistogramLeafList) super.clone();
                if (head != null) {
                    HistogramLeafCell cell = head;
                    HistogramLeafCell lastCopy = null;
                    while (cell != null) {
                        HistogramLeafCell nextCopy = cell.copy();
                        if (lastCopy == null) {
                            lastCopy = nextCopy;
                            list.head = lastCopy;
                        } else {
                            lastCopy.next = nextCopy;
                            lastCopy = nextCopy;
                        }
                        cell = cell.next;
                    }
                }
                return list;
            } catch (CloneNotSupportedException ce) {
                throw new RuntimeException(ce);
            }
        }

        public static HistogramLeafList add(HistogramLeafList list, HistogramTree tree, Object v) {
            if (list == null) {
                list = new HistogramLeafList();
            }
            list.add(tree, v);
            return list;
        }

        public void add(HistogramTree tree, Object value) {
            HistogramLeafCell c = new HistogramLeafCell(value);
            if (head == null) {
                head = c;
                tail = head;
            } else {
                tail.next = c;
                tail = c;
            }
        }

        public boolean isEmpty() {
            return head == null;
        }

        public Object poll(HistogramTree tree) {
            Object v = head.value;
            head = head.next;
            if (head == null) {
                tail = null;
            }
            return v;
        }

        public long count() {
            long n = 0;
            HistogramLeafCell cell = head;
            while (cell != null) {
                cell = cell.next;
                ++n;
            }
            return n;
        }

        public Iterator<Object> iterator() {
            return new Iterator<Object>() {
                HistogramLeafCell next = head;
                @Override
                public boolean hasNext() {
                    return next != null;
                }

                @Override
                public Object next() {
                    Object v = next.value;
                    next = next.next;
                    return v;
                }
            };
        }

        @Override
        public void write(Kryo kryo, Output output) {
            HistogramLeafCell cell = head;
            while (cell != null) {
                kryo.writeClassAndObject(output, cell);
                cell = cell.next;
            }
            kryo.writeClassAndObject(output, new HistogramLeafCellSerializedEnd());
        }

        @Override
        public void read(Kryo kryo, Input input) {
            Object o = kryo.readClassAndObject(input);
            HistogramLeafCell prev = null;
            while (o instanceof HistogramLeafCell) {
                HistogramLeafCell cell = (HistogramLeafCell) o;
                if (prev == null) {
                    prev = cell;
                    head = prev;
                } else {
                    prev.next = cell;
                    prev = cell;
                }
                o = kryo.readClassAndObject(input);
            }
            tail = prev;
        }
    }

    public static class HistogramLeafCellSerializedEnd implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    public static class HistogramLeafCell implements Serializable, KryoSerializable, Cloneable {
        public static final long serialVersionUID = 1L;
        public Object value;
        public HistogramLeafCell next;

        public HistogramLeafCell() {}

        public HistogramLeafCell(Object value) {
            this.value = value;
        }

        public HistogramLeafCell copy() {
            try {
                HistogramLeafCell cell = (HistogramLeafCell) super.clone();
                cell.next = null;
                return cell;
            } catch (CloneNotSupportedException ce) {
                throw new RuntimeException(ce);
            }
        }

        @Override
        public void write(Kryo kryo, Output output) {
            kryo.writeClassAndObject(output, value);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            value = kryo.readClassAndObject(input);
        }

        public long valueCount() {
            return 1;
        }
    }
}
