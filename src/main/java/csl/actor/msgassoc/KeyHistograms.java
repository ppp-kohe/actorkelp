package csl.actor.msgassoc;

import java.io.Serializable;
import java.util.*;

public class KeyHistograms {
    public static class HistogramTree implements Serializable {
        protected HistogramNode root;
        protected KeyComparator<?> comparator;

        protected int treeLimit;

        protected LinkedList<HistogramNodeLeaf> completed = new LinkedList<>();
        protected long leafSize;
        protected long leafSizeNonZero;

        public HistogramTree(KeyComparator<?> comparator) {
            this(null, comparator, 32);
        }

        public HistogramTree(KeyComparator<?> comparator, int treeLimit) {
            this(null, comparator, treeLimit);
        }

        public HistogramTree(HistogramNode root, KeyComparator<?> comparator, int treeLimit) {
            this.root = root;
            this.comparator = comparator;
            this.treeLimit = treeLimit;
        }

        public boolean hasMultiplePoints() {
            if (root == null) {
                return false;
            } else {
                return !root.keyStart().equals(root.keyEnd());
            }
        }

        public void put(Object key, HistogramPutContext context) {
            context.putTree = this;
            context.putTreeLimit = treeLimit;
            if (root == null) {
                root = context.createLeafWithCountUp(key, 0);
            } else {
                HistogramNode l = root.put(comparator, key, context);
                if (l != null) {
                    root = new HistogramNodeTree(root.height() + 1, treeLimit, sort(comparator, l, root));
                }
            }
        }

        /**
         * @return a new split of left hand side
         */
        public HistogramTree split() {
            if (root == null) {
                return new HistogramTree(comparator, treeLimit);
            } else {
                return new HistogramTree(root.split(root.size(), 0), comparator, treeLimit);
            }
        }

        public Object splitPointAsRightHandSide(HistogramTree splitLeft) {
            //this is the right hand side and the right has least one root node.
            // so the returned point is inclusive for the right split
            return root.keyStart();
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

        /** @return implementation field getter */
        public KeyComparator<?> getComparator() {
            return comparator;
        }

        public void prune() {
            if (root != null) {
                if (root.prune(this)) {
                    root = null;
                }
            }
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
    }

    public static abstract class HistogramPutContext {
        public int putRequiredSize;
        public HistogramTree putTree;
        public Comparable<?> putPosition;
        public Object putValue;
        public int putTreeLimit;

        protected abstract HistogramNodeLeaf createLeaf(Object key, int height);

        public HistogramNodeLeaf createLeafWithCountUp(Object key, int height) {
            HistogramNodeLeaf l = createLeaf(key, height);
            putTree.incrementLeafSize(1);
            return l;
        }

        public void incrementLeafSizeNonZero() {
            putTree.incrementLeafSizeNonZero(1);
        }

        public Comparable<?> position(HistogramNodeLeaf leaf) {
            return putPosition;
        }

        public void complete(HistogramNodeLeaf leaf) {
            putTree.complete(leaf);
        }
    }

    public interface KeyComparator<KeyType> extends Serializable, Comparator<KeyType> {
        int compare(KeyType key1, KeyType key2);
    }

    public interface HistogramNode extends Serializable {
        long size();
        int height();
        HistogramNode lowerHeight();

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

        boolean prune(HistogramTree tree);
    }

    public static final int TREE_LIMIT = 32;

    @SuppressWarnings("unchecked")
    public static HistogramNode[] sort(KeyComparator<?> comparator, HistogramNode... nodes) {
        Arrays.sort(nodes, (n1,n2) ->
                ((KeyComparator<Object>)comparator).compare(n1.keyStart(), n2.keyStart()));
        return nodes;
    }

    public static class HistogramNodeTree implements HistogramNode {
        protected HistogramNodeTree parent;
        protected List<HistogramNode> children;
        protected long size;
        protected Object keyStart;
        protected Object keyEnd;
        protected int height;

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

        public List<HistogramNode> getChildren() {
            return children;
        }

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
        public HistogramNode lowerHeight() {
            --height;
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
            if (index >= 0) { //found
                newNode = children.get(index).put(comparator, key, context);
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
                HistogramNode childTarget = children.get(index);
                newNode = childTarget.put(comparator, key, context);
                if (newNode != null && childTarget instanceof HistogramNodeLeaf) { //it can create a new sub-tree
                    newNode = new HistogramNodeTree(height - 1, treeLimit, sort(comparator, childTarget.lowerHeight(), newNode.lowerHeight()));
                    children.set(index, newNode); //replace
                    newNode.setParent(this);
                } else if (newNode != null) {
                    children.add(index, newNode); //tree.put always returns a new left sibling
                    newNode.setParent(this);
                }
            }
            if (children.size() > treeLimit) {
                int n = children.size() / 2;
                List<HistogramNode> newChildren = new ArrayList<>(children.subList(n, children.size()));
                HistogramNode l = new HistogramNodeTree(height, new ArrayList<>(children.subList(0, n)));
                this.children = newChildren;
                updateChildren();
                return l;
            } else {
                if (index <= 0 ||
                        index >= children.size() - 2) {  //consider increased size
                    updateKeys();
                }
                return null;
            }
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

        protected int selectChildTree(int index, int treeLimit) {
            if (index - 1 < 0) {
                if (index >= children.size()) { //empty
                    HistogramNodeTree childTarget = new HistogramNodeTree(height - 1, treeLimit);
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
                        return new HistogramNodeTree(height, lefts);
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
        public boolean prune(HistogramTree tree) {
            boolean zero = (size == 0);
            if (children.removeIf(c -> c.prune(tree))) {
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
    }


    public static abstract class HistogramNodeLeaf implements HistogramNode {
        protected HistogramNodeTree parent;
        protected Object key;
        protected long size;
        protected int height;

        public HistogramNodeLeaf(Object key, HistogramPutContext context, int height) {
            this.key = key;
            this.height = height;
            initStruct();
            putValue(context);
        }

        public Object getKey() {
            return key;
        }

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
        public HistogramNode lowerHeight() {
            --height;
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

        protected abstract void initStruct();
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
        public boolean prune(HistogramTree tree) {
            if (size == 0) {
                tree.incrementLeafSize(-1);
                return true;
            } else {
                return false;
            }
        }
    }

    public static class HistogramNodeLeafMap extends HistogramNodeLeaf {
        protected TreeMap<Comparable<?>, HistogramLeafList> values;
        protected int nextPosition;

        public HistogramNodeLeafMap(Object key, HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        protected void initStruct() {
            values = new TreeMap<>();
        }

        @Override
        protected void putValueStruct(HistogramPutContext context) {
            values.compute(context.position(this),
                    (p, l) -> HistogramLeafList.add(l, context.putValue));
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

        public Object[] take(int requiredSize, HistogramTree tree) {
            if (values.size() >= requiredSize) {
                Object[] res = new Object[requiredSize];
                int i = 0;
                for (Iterator<Map.Entry<Comparable<?>, HistogramLeafList>> ei = values.entrySet().iterator();
                     ei.hasNext(); ) {
                    Map.Entry<Comparable<?>, HistogramLeafList> e = ei.next();
                    HistogramLeafList ev = e.getValue();
                    res[i] = ev.poll();
                    ++i;
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
    }

    public static class HistogramLeafList implements Iterable<Object>, Serializable {
        protected HistogramLeafCell head;
        protected HistogramLeafCell tail;

        public static HistogramLeafList add(HistogramLeafList list, Object v) {
            if (list == null) {
                list = new HistogramLeafList();
            }
            list.add(v);
            return list;
        }

        public void add(Object value) {
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

        public Object poll() {
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
    }

    public static class HistogramLeafCell implements Serializable {
        public Object value;
        public HistogramLeafCell next;

        public HistogramLeafCell(Object value) {
            this.value = value;
        }
    }
}
