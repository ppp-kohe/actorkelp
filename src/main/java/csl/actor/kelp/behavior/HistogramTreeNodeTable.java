package csl.actor.kelp.behavior;

import csl.actor.kelp.ActorKelpFunctions;
import csl.actor.persist.PersistentFileManager;

import java.util.*;

public class HistogramTreeNodeTable implements KeyHistograms.HistogramTreeNode, Cloneable {
    public static final long serialVersionUID = 1L;
    public HistogramTreeNodeTable parent;
    public List<KeyHistograms.HistogramTreeNode> children;
    public long size;
    public Object keyStart;
    public Object keyEnd;

    public HistogramTreeNodeTable() {
    }

    public HistogramTreeNodeTable(List<KeyHistograms.HistogramTreeNode> children) {
        this.children = children;
        updateChildren();
    }

    public HistogramTreeNodeTable(int capacity, KeyHistograms.HistogramTreeNode... children) {
        ArrayList<KeyHistograms.HistogramTreeNode> cs = new ArrayList<>(capacity);
        Collections.addAll(cs, children);
        this.children = cs;
        updateChildren();
    }

    @Override
    public HistogramTreeNodeTable copy(Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> oldToNew) {
        try {
            HistogramTreeNodeTable node = (HistogramTreeNodeTable) super.clone();
            oldToNew.put(this, node);

            if (parent != null) {
                node.parent = (HistogramTreeNodeTable) oldToNew.get(parent);
            }
            node.children = new ArrayList<>(children.size());
            for (KeyHistograms.HistogramTreeNode child : children) {
                node.children.add(child.copy(oldToNew));
            }
            return node;
        } catch (CloneNotSupportedException ce) {
            throw new RuntimeException(ce);
        }
    }

    /**
     * @param tree  the caller tree or null (the persisted node might load sub-nodes and update node-size info. for the tree)
     * @return the list of children. the returned list is actual reference of the node.
     * elements in the list might not yet be loaded.
     */
    public List<KeyHistograms.HistogramTreeNode> getChildren(HistogramTree tree) {
        return children;
    }

    /**
     * @return the list of children without loading any persisted data. the returned list is actual reference of the node.
     *      elements in the list might not yet be loaded.
     */
    public List<KeyHistograms.HistogramTreeNode> getChildrenOnMemory() {
        return children;
    }

    @Override
    public HistogramTreeNodeTable getParent() {
        return parent;
    }

    @Override
    public void setParent(HistogramTreeNodeTable parent) {
        this.parent = parent;
    }

    @Override
    public int height() {
        return children.stream()
                .mapToInt(KeyHistograms.HistogramTreeNode::height)
                .max().orElse(0) + 1;
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    @Override
    public long size() {
        return size;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int keyIn(ActorKelpFunctions.KeyComparator<?> comparator, Object key) {
        int sc = ((ActorKelpFunctions.KeyComparator<Object>) comparator).compare(key, this.keyStart);
        if (sc < 0) { //key < keyStart
            return sc;
        } else if (sc > 0) {
            int ec = ((ActorKelpFunctions.KeyComparator<Object>) comparator).compare(key, this.keyEnd);
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
    public KeyHistograms.HistogramTreeNode put(ActorKelpFunctions.KeyComparator<?> comparator, Object key, KeyHistograms.HistogramPutContext context, int height) {
        size++;

        KeyHistograms.HistogramTreeNode newNode = null;
        int index = search(comparator, key);
        updatePutIndex(context, index);
        if (index >= 0) { //found
            newNode = putChildAt(comparator, key, context, index, height);
            if (newNode == null) {
                return null;
            }
        } else {
            index = -index - 1;
        }
        int treeLimit = context.putTreeLimit;
        if (newNode != null || height <= 1) { //appropriate range or bottom
            if (newNode == null) {
                newNode = context.createLeafWithCountUp(key);
            }
            children.add(index, newNode);
            newNode.setParent(this);
        } else { //upper tree
            index = selectChildTree(index, treeLimit, context.putTree);
            newNode = putChildAt(comparator, key, context, index, height);
            KeyHistograms.HistogramTreeNode childTarget = children.get(index);
            if (newNode != null && childTarget.isLeaf()) { //it can create a new sub-tree
                newNode = createNodeTree(treeLimit, KeyHistograms.sort(comparator, childTarget, newNode));
                context.putTree.addNodeSizeOnMemory(1);
                children.set(index, newNode); //replace
                newNode.setParent(this);
            } else if (newNode != null) {
                children.add(index, newNode); //tree.put always returns a new left sibling
                newNode.setParent(this);
            }
        }
        if (children.size() > treeLimit) {
            return splitWithCountUp(context.putTree);
        } else {
            if (index <= 0 ||
                    index >= children.size() - 2) {  //consider increased size
                updateKeys();
            }
            return null;
        }
    }

    @Override
    public KeyHistograms.HistogramTreeNode put(ActorKelpFunctions.KeyComparator<?> comparator, HistogramTree tree, HistogramTreeNodeLeaf leaf, int height) {
        size += leaf.size();

        KeyHistograms.HistogramTreeNode newNode = null;
        int index = search(comparator, leaf.getKey());
        //updatePutIndex(context, index);
        if (index >= 0) { //found
            newNode = putChildAt(comparator, tree, leaf, index, height); //no persisted: putChildAt(comparator, key, context, index);
            if (newNode == null) {
                return null;
            }
        } else {
            index = -index - 1;
        }
        int treeLimit = tree.getTreeLimit();
        if (newNode != null || height <= 1) { //appropriate range or bottom
            if (newNode == null) {
                newNode = leaf;
            }
            children.add(index, newNode);
            newNode.setParent(this);
        } else { //upper tree
            index = selectChildTree(index, treeLimit, tree);
            newNode = putChildAt(comparator, tree, leaf, index, height);
            KeyHistograms.HistogramTreeNode childTarget = children.get(index);
            if (newNode != null && childTarget.isLeaf()) { //it can create a new sub-tree
                newNode = createNodeTree(treeLimit, KeyHistograms.sort(comparator, childTarget, newNode));
                tree.addNodeSizeOnMemory(1);
                children.set(index, newNode); //replace
                newNode.setParent(this);
            } else if (newNode != null) {
                children.add(index, newNode); //tree.put always returns a new left sibling
                newNode.setParent(this);
            }
        }
        if (children.size() > treeLimit) {
            return splitWithCountUp(tree);
        } else {
            if (index <= 0 ||
                    index >= children.size() - 2) {  //consider increased size
                updateKeys();
            }
            return null;
        }
    }

    protected KeyHistograms.HistogramTreeNode putChildAt(ActorKelpFunctions.KeyComparator<?> comparator, Object key, KeyHistograms.HistogramPutContext context,
                                                         int index, int height) {
        KeyHistograms.HistogramTreeNode child = children.get(index);
        KeyHistograms.HistogramTreeNode newChild = child.load(context); // the child will be replaced with newChild in load() if child!=newChild
        return newChild.put(comparator, key, context, height - 1);
    }

    protected KeyHistograms.HistogramTreeNode putChildAt(ActorKelpFunctions.KeyComparator<?> comparator, HistogramTree tree, HistogramTreeNodeLeaf leaf,
                                                         int index, int height) {
        return children.get(index).put(comparator, tree, leaf, height - 1); //suppose no persisted nodes
    }

    protected KeyHistograms.HistogramTreeNode splitWithCountUp(HistogramTree tree) {
        int cap = tree.getTreeLimit();
        int n = children.size() / 2;
        ArrayList<KeyHistograms.HistogramTreeNode> right = new ArrayList<>(cap);
        ArrayList<KeyHistograms.HistogramTreeNode> left = new ArrayList<>(cap);
        int i = 0;
        for (KeyHistograms.HistogramTreeNode child : children) {
            if (i < n) {
                left.add(child);
            } else {
                right.add(child);
            }
            ++i;
        }
        KeyHistograms.HistogramTreeNode l = createNodeTree(left);
        tree.addNodeSizeOnMemory(1L);
        this.children = right;
        updateChildren();
        return l;
    }

    /**
     * @param comparator a comparator for keys
     * @param key        the key
     * @return the index containing the key,
     * or -(the insertion point) -1 which has the same semantics of JDK binarySearch
     * e.g. {@link Collections#binarySearch(List, Object, Comparator)}
     */
    protected int search(ActorKelpFunctions.KeyComparator<?> comparator, Object key) {
        int start = 0;
        int end = children.size() - 1;
        while (start <= end) {
            int i = (start + end) / 2;
            KeyHistograms.HistogramTreeNode n = children.get(i);
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

    protected void updatePutIndex(KeyHistograms.HistogramPutContext context, int index) {
        float s = context.putIndexCurrentRangeStart;
        float l = context.putIndexCurrentRangeLength;
        float cs = children.size();
        float p = Math.min(1.0f, ((float) Math.abs(index)) / cs);
        context.putIndexCurrentRangeStart = s + l * p;
        context.putIndexCurrentRangeLength = l * (1f / cs);
    }

    protected int selectChildTree(int index, int treeLimit, HistogramTree tree) {
        if (index - 1 < 0) {
            if (index >= children.size()) { //empty
                HistogramTreeNodeTable childTarget = createNodeTree(treeLimit);
                children.add(childTarget);
                childTarget.setParent(this);
                tree.addNodeSizeOnMemory(1);
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

    protected HistogramTreeNodeTable createNodeTree(int treeLimit, KeyHistograms.HistogramTreeNode... children) {
        return new HistogramTreeNodeTable(treeLimit, children);
    }

    protected HistogramTreeNodeTable createNodeTree(List<KeyHistograms.HistogramTreeNode> children) {
        return new HistogramTreeNodeTable(children);
    }

    protected void updateChildren() {
        long size = 0;
        for (KeyHistograms.HistogramTreeNode n : children) {
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
    public KeyHistograms.HistogramTreeNode split(long halfSize, long currentLeft) {
        List<KeyHistograms.HistogramTreeNode> lefts = new ArrayList<>(children.size());
        int i = 0;
        for (KeyHistograms.HistogramTreeNode n : children) {
            if (currentLeft + n.size() >= halfSize) {
                KeyHistograms.HistogramTreeNode nLeft = n.split(halfSize, currentLeft);
                if (nLeft != null) {
                    lefts.add(nLeft);
                }
                List<KeyHistograms.HistogramTreeNode> rights = new ArrayList<>(children.size());
                rights.addAll(children.subList(i, children.size())); //includes n
                this.children = rights;
                updateChildren();
                if (!lefts.isEmpty()) {
                    return createNodeTree(lefts);
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

    public void reduce(long size) {
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

    public KeyHistograms.HistogramTreeNode merge(HistogramTree tree, int treeLimit, ActorKelpFunctions.KeyComparator<?> comparator, KeyHistograms.HistogramTreeNode lowerNode) {
        int h1 = this.height();
        int h2 = lowerNode.height();
        if (h1 == h2) {
            if (lowerNode instanceof HistogramTreeNodeLeaf) {
                return merge(tree, treeLimit, comparator, lowerNode); //always h2 > 0 because the height of a HistogramNodeTree always > 0
            } else if (lowerNode instanceof HistogramTreeNodeTable) {
                HistogramTreeNodeTable node = (HistogramTreeNodeTable) lowerNode;
                int r = keyIn(comparator, node.keyEnd());
                if (r < 0) {
                    children.addAll(0, node.getChildren(null));
                } else { //r > 0: no r==0
                    children.addAll(node.getChildren(null));
                }
                if (children.size() > treeLimit) {
                    return splitWithCountUp(tree);
                } else {
                    updateChildren();
                    return null;
                }
            }
        } else { //h1 > h2
            if (children.isEmpty()) {
                children.add(lowerNode);
                updateChildren();
            } else {
                int r = keyIn(comparator, lowerNode.keyEnd());
                KeyHistograms.HistogramTreeNode target;
                int insPoint;
                if (r < 0) {
                    insPoint = 0;
                } else { //r > 0: no r==0
                    insPoint = children.size() - 1;
                }
                target = children.get(insPoint);
                if (target instanceof HistogramTreeNodeTable) {
                    KeyHistograms.HistogramTreeNode newNode = ((HistogramTreeNodeTable) target).merge(tree, treeLimit, comparator, lowerNode);
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
                    return splitWithCountUp(tree);
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
