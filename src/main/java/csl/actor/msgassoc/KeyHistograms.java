package csl.actor.msgassoc;

import java.io.Serializable;
import java.util.*;

public class KeyHistograms {

    @Deprecated public interface Histogram extends Serializable {
        void put(Object v);
        Comparable<?> findSplitPoint();
        int compareToSplitPoint(Object v, Comparable<?> splitPoint);
        int compareSplitPoints(Comparable<?> v1, Comparable<?> v2);

        Histogram create();

        boolean hasMultiplePoints();
    }

    @Deprecated public static class HistogramComparable implements Histogram {
        protected long total;
        protected TreeMap<Comparable<?>, Long> counts = new TreeMap<>();

        @Override
        public void put(Object v) {
            total++;
            counts.compute((Comparable<?>) v, (k,count) -> count == null ? 0L : (count + 1L));
        }

        public Comparable<?> findSplitPoint() {
            long acc = 0;

            long half = total / 2L;

            Comparable<?> last = null;
            for (Map.Entry<Comparable<?>, Long> e : counts.entrySet()) {
                last = e.getKey();
                acc += e.getValue();
                if (acc >= half) {
                    return last;
                }
            }
            return last;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public int compareToSplitPoint(Object v, Comparable<?> splitPoint) {
            return ((Comparable) v).compareTo(splitPoint);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public int compareSplitPoints(Comparable<?> v1, Comparable<?> v2) {
            return ((Comparable) v1).compareTo(v2);
        }

        @Override
        public Histogram create() {
            return new HistogramComparable();
        }

        @Override
        public boolean hasMultiplePoints() {
            return counts.size() > 1;
        }
    }

    @Deprecated public static class HistogramNonComparable extends HistogramComparable {
        @Override
        public void put(Object v) {
            Comparable<?> hc = toKey(v);
            super.put(hc);
        }

        public Comparable<?> toKey(Object v) {
            return Objects.hashCode(v);
        }

        @Override
        public int compareToSplitPoint(Object v, Comparable<?> splitPoint) {
            return super.compareToSplitPoint(toKey(v), splitPoint);
        }

        @Override
        public Histogram create() {
            return new HistogramNonComparable();
        }
    }

    /////////////////

    public static class HistogramTree implements Serializable {
        public HistogramNode root;
        public KeyComparator<?> comparator;

        protected LinkedList<HistogramNodeLeaf> completed = new LinkedList<>();

        public HistogramTree(KeyComparator<?> comparator) {
            this(null, comparator);
        }

        public HistogramTree(HistogramNode root, KeyComparator<?> comparator) {
            this.root = root;
            this.comparator = comparator;
        }

        public HistogramTree create() {
            return new HistogramTree(comparator);
        }

        //TODO constructing the context
        public void put(Object key, HistogramPutContext context) {
            context.putCompletionReceiver = this;
            if (root == null) {
                root = context.createLeaf(key);
            } else {
                HistogramNode r = root.put(comparator, key, context);
                if (r != null) {
                    root = new HistogramNodeTree(root, r);
                }
            }
        }

        public HistogramTree split() {
            if (root == null) {
                return new HistogramTree(comparator);
            } else {
                return new HistogramTree(root.split(root.size(), 0), comparator);
            }
        }

        public Object splitPoint(HistogramTree splitRight) {
            //TODO
            return root.keyEnd();
        }

        @SuppressWarnings("unchecked")
        public boolean compareSplitPoints(Object l, Object r) {
            return ((KeyComparator<Object>) comparator).compare(l, r) <= 0;
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
    }

    public static abstract class HistogramPutContext {
        public int putRequiredSize;
        public HistogramTree putCompletionReceiver;
        public Comparable<?> putPosition;
        public Object putValue;

        public abstract HistogramNodeLeaf createLeaf(Object key);

        public Comparable<?> position(HistogramNodeLeaf leaf) {
            return putPosition;
        }

        public void complete(HistogramNodeLeaf leaf) {
            putCompletionReceiver.complete(leaf);
        }
    }

    public interface KeyComparator<KeyType> extends Serializable {
        int compare(KeyType key1, KeyType key2);
    }

    public interface HistogramNode extends Serializable {
        long size();

        HistogramNode put(KeyComparator<?> comparator, Object key, HistogramPutContext context);

        int keyIn(KeyComparator<?> comparator, Object key);
        Object keyStart();
        Object keyEnd();

        HistogramNode split(long halfSize, long currentLeft);

        void setParent(HistogramNodeTree node);
    }

    public static final int TREE_LIMIT = 32;

    public static class HistogramNodeTree implements HistogramNode {
        protected HistogramNodeTree parent;
        public List<HistogramNode> children;
        protected long size;
        protected Object keyStart;
        protected Object keyEnd;

        public HistogramNodeTree(List<HistogramNode> children) {
            this.children = children;
            updateChildren();
        }

        public HistogramNodeTree(HistogramNode... children) {
            this.children = new ArrayList<>(TREE_LIMIT);
            this.children.addAll(Arrays.asList(children));
            updateChildren();
        }

        @Override
        public void setParent(HistogramNodeTree parent) {
            this.parent = parent;
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
            int start = 0;
            int end = children.size() - 1;
            while (start <= end) {
                int i = (start + end) / 2;
                HistogramNode n = children.get(i);
                int c = n.keyIn(comparator, key);
                if (c < 0) {
                    start = i + 1;
                } else if (c > 0) {
                    end = i - 1;
                } else {
                    //found
                    newNode = n.put(comparator, key, context);
                    if (newNode == null) {
                        return null;
                    } else {
                        start = i + 1; //put always returns right-hand-side of split nodes
                        break;
                    }
                }
            }

            //not found
            if (newNode == null) {
                newNode = context.createLeaf(key);
            }
            children.add(start, newNode);
            newNode.setParent(this);
            if (children.size() > TREE_LIMIT) {
                int n = children.size() / 2;
                List<HistogramNode> newChildren = new ArrayList<>(children.subList(0, n));
                HistogramNode r = new HistogramNodeTree(new ArrayList<>(children.subList(n, children.size())));
                this.children = newChildren;
                updateChildren();
                return r;
            } else {
                if (start == 0 || start >= children.size() - 1) {
                    updateKeys();
                }
                return null;
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
            keyStart = children.get(0).keyStart();
            keyEnd = children.get(children.size() - 1).keyEnd();
        }

        @Override
        public HistogramNode split(long halfSize, long currentLeft) {
            List<HistogramNode> lefts = new ArrayList<>(children.size());
            int i = 0;
            for (HistogramNode n : children) {
                currentLeft += n.size();
                lefts.add(n);
                if (currentLeft >= halfSize) {
                    List<HistogramNode> rights = new ArrayList<>(children.size());
                    HistogramNode nRight = n.split(halfSize, currentLeft);
                    if (nRight != null) {
                        rights.add(0, nRight);
                    }
                    rights.addAll(children.subList(i + 1, children.size()));
                    this.children = lefts;
                    updateChildren();
                    if (!rights.isEmpty()) {
                        return new HistogramNodeTree(rights);
                    } else {
                        return null;
                    }
                }
                ++i;
            }
            return null;
        }

        public void reduce(int size) {
            this.size += size;
            if (parent != null) {
                parent.reduce(size);
            }
        }
    }


    public static abstract class HistogramNodeLeaf implements HistogramNode {
        protected HistogramNodeTree parent;
        public Object key;
        protected long size;

        public HistogramNodeLeaf(Object key, HistogramPutContext context) {
            this.key = key;
            initStruct();
            putValue(context);
        }

        @Override
        public void setParent(HistogramNodeTree parent) {
            this.parent = parent;
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
                return context.createLeaf(key);
            }
        }

        protected void putValue(HistogramPutContext context) {
            putValueStruct(context);
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

        public void afterTake(int removedSize) {
            size -= removedSize;
            if (parent != null) {
                parent.reduce(removedSize);
            }
        }
    }

    public static class HistogramNodeLeafMap extends HistogramNodeLeaf {
        protected TreeMap<Comparable<?>, HistogramLeafList> values;
        protected int nextPosition;

        public HistogramNodeLeafMap(Object key, HistogramPutContext context) {
            super(key, context);
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

        public Object[] take(int requiredSize) {
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
                afterTake(requiredSize);
                return res;
            } else {
                return null;
            }
        }
    }


    public static class HistogramPutContextMap extends HistogramPutContext {
        public HistogramNodeLeaf createLeaf(Object key) {
            return new HistogramNodeLeafMap(key, this);
        }

        public Comparable<?> position(HistogramNodeLeaf leaf) {
            if (putPosition == null) {
                return ((HistogramNodeLeafMap) leaf).nextPosition(this);
            } else {
                return putPosition;
            }
        }
    }

    public static class HistogramLeafList {
        public HistogramLeafCell head;
        public HistogramLeafCell tail;

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
    }

    public static class HistogramLeafCell {
        public Object value;
        public HistogramLeafCell next;

        public HistogramLeafCell(Object value) {
            this.value = value;
        }
    }
}
