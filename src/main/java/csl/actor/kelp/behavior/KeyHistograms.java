package csl.actor.kelp.behavior;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.Actor;
import csl.actor.kelp.ActorKelpFunctions.KeyComparator;
import csl.actor.persist.PersistentFileManager;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;

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

    public static abstract class HistogramPutContext {
        /** set by actor caller*/
        public Actor putActor;
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
        protected abstract HistogramTreeNodeLeaf createLeaf(Object key, int height);

        /**
         * creates a new leaf (by {@link #createLeaf(Object, int)})
         * and puts the value set to this. called under put processes. 
         *  increments the size of {@link #putTree}
         * @param key the key for the leaf
         * @param height the height of the leaf
         * @return a new leaf with a value
         */
        public HistogramTreeNodeLeaf createLeafWithCountUp(Object key, int height) {
            HistogramTreeNodeLeaf l = createLeaf(key, height);
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
        
        public Comparable<?> position(HistogramTreeNodeLeaf leaf) {
            return putPosition;
        }

        /**
         * just calls {@link #putTree}'s {@link HistogramTree#complete(HistogramTreeNodeLeaf)}
         * @param leaf the completed leaf
         */
        public void complete(HistogramTreeNodeLeaf leaf) {
            putTree.complete(leaf);
        }

        /**
         * @return true if allow FullTree persisting:
         *    the tree will be cleared and eventually processed
         */
        public boolean allowFullTreePersist() {
            return false;
        }

        public HistogramTreeNodeLeaf createLeafPersisted(Class<?> leafType, Object key, int height) throws Exception {
            return (HistogramTreeNodeLeaf) leafType
                    .getConstructor(Object.class, KeyHistograms.HistogramPutContext.class, int.class)
                    .newInstance(key, this, height);
        }
    }


    public interface HistogramTreeNode extends Serializable {
        long size();
        int height();
        HistogramTreeNode increaseHeight(int heightDelta);

        default HistogramTreeNode load(HistogramPutContext context) {
            return this;
        }

        default boolean isPersisted() {
            return false;
        }

        HistogramTreeNode put(KeyComparator<?> comparator, Object key, HistogramPutContext context);
        HistogramTreeNode put(KeyComparator<?> comparator, HistogramTree tree, HistogramTreeNodeLeaf leaf);

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
        HistogramTreeNode split(long halfSize, long currentLeft);

        void setParent(HistogramTreeNodeTable node);
        HistogramTreeNodeTable getParent();

        boolean prune(HistogramTree tree, boolean countUpLeafSize);

        void initPersistent(PersistentFileManager persistent);

        HistogramTreeNode copy(Map<HistogramTreeNode, HistogramTreeNode> oldToNew);
    }

    @SuppressWarnings("unchecked")
    public static HistogramTreeNode[] sort(KeyComparator<?> comparator, HistogramTreeNode n1, HistogramTreeNode n2) {
        int r = ((KeyComparator<Object>) comparator).compare(n1.keyStart(), n2.keyStart());
        if (r > 0) {
            HistogramTreeNode tmp = n1;
            n1 = n2;
            n2 = tmp;
        }
        return new HistogramTreeNode[] {n1, n2};
    }
    /*
    @SuppressWarnings("unchecked")
    public static HistogramNode[] sort(KeyComparator<?> comparator, HistogramNode... nodes) {
        Arrays.sort(nodes, (n1,n2) ->
                ((KeyComparator<Object>)comparator).compare(n1.keyStart(), n2.keyStart()));
        return nodes;
    }*/


    public static class HistogramNodeLeafMap extends HistogramTreeNodeLeaf {
        public static final long serialVersionUID = 1L;
        public TreeMap<Comparable<?>, HistogramLeafList> values;
        public int nextPosition;
        public int maxPosition;

        public HistogramNodeLeafMap() {}

        public HistogramNodeLeafMap(Object key, HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        public HistogramNodeLeafMap copy(Map<HistogramTreeNode, HistogramTreeNode> oldToNew) {
            HistogramNodeLeafMap node = (HistogramNodeLeafMap) super.copy(oldToNew);
            node.values = new TreeMap<>();
            values.forEach((k,v) ->
                    node.values.put(k, v == null ? null : v.copy()));
            return node;
        }

        @Override
        protected void initStruct(HistogramPutContext context) {
            values = new TreeMap<>();
            setStructListSizeAsAllPersisted(context.putRequiredSize, context);
        }

        @Override
        public void setStructListSizeAsAllPersisted(int listSize, HistogramPutContext context) {
            maxPosition = listSize;
            for (int i = 0; i < maxPosition; ++i) {
                values.computeIfAbsent(i, k_ -> context.createEmptyList());
            }
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
            return completedAll();
        }

        public boolean completedAll() {
            if (maxPosition >= values.size()) {
                for (HistogramLeafList list : values.values()) {
                    if (list.isEmpty()) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        }

        public int nextPosition() {
            int p = nextPosition;
            ++nextPosition;
            if (maxPosition <= nextPosition) {
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
            if (completed(requiredSize)) {
                Object[] res = new Object[requiredSize];
                int i = 0;
                for (Iterator<Map.Entry<Comparable<?>, HistogramLeafList>> ei = values.entrySet().iterator();
                     ei.hasNext(); ) {
                    Map.Entry<Comparable<?>, HistogramLeafList> e = ei.next();
                    HistogramLeafList ev = e.getValue();
                    if (!ev.isEmpty()) {
                        res[i] = ev.poll(tree, this);
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

        public boolean completed(int requiredSize) {
            long remain = requiredSize;
            for (HistogramLeafList list : values.values()) {
                remain -= list.count();
                if (remain <= 0) {
                    return true;
                }
            }
            return false;
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
        public HistogramPutContextMap() {}
        public HistogramPutContextMap(HistogramTree tree) {
            putTree = tree;
        }

        protected HistogramTreeNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeafMap(key, this, height);
        }

        public Comparable<?> position(HistogramTreeNodeLeaf leaf) {
            if (putPosition == null) {
                return ((HistogramNodeLeafMap) leaf).nextPosition();
            } else {
                return putPosition;
            }
        }

        public Object[] take(HistogramTree tree, HistogramTreeNodeLeaf leaf) {
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
                            lastCopy.setNext(nextCopy);
                            lastCopy = nextCopy;
                        }
                        cell = cell.next;
                    }
                    list.tail = lastCopy;
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

        public void add(HistogramTree tree, Object value) { //suppose no persist increment
            if (head == null) {
                head = new HistogramLeafCellArray(100);
                tail = head;
            }
            while (!tail.offer(value)) {
                if (tail.next == null) {
                    int cap = Math.max(Math.min(10_000_000, tail.sizeOnMemory() * 2), 10_000); //100 -> 10_000 -> 20_000 -> 40_000 -> ...
                    tail.setNext(new HistogramLeafCellArray(cap));
                }
                tail = tail.next;
            }
        }

        public boolean isEmpty() {
            HistogramLeafCell cell = head;
            while (cell != null) {
                if (cell.isNonEmpty()) {
                    return false;
                }
                cell = cell.next;
            }
            return true;
        }

        public Object poll(HistogramTree tree, HistogramTreeNodeLeaf leaf) {
            while (head != null) {
                if (head.isNonEmpty()) {
                    Object v = head.poll(tree, leaf);
                    if (!head.isNonEmpty()) {
                        head = head.next;
                        if (head != null) {
                            head.prev = null;
                        } else {
                            tail = null;
                        }
                    }
                    return v;
                } else {
                    head = null;
                    tail = null;
                }
            }
            return null;
        }

        public void polls(HistogramTree tree, HistogramTreeNodeLeaf leaf, int consuming, List<Object> target, boolean onMemory) {
            HistogramLeafCell cell = head;
            while (cell != null && consuming > 0) {
                long remain = onMemory ? cell.sizeOnMemory() : cell.size();
                while (remain > 0 && consuming > 0) {
                    target.add(cell.poll(tree, leaf));
                    --remain;
                    --consuming;
                }
                if (!cell.isNonEmpty()) { //remove cell
                    HistogramLeafCell prev = cell.prev;
                    HistogramLeafCell next = cell.next;
                    if (prev != null) {
                        prev.setNext(next);
                    } else { //cell==head
                        head = next;
                        if (head != null) {
                            head.prev = null;
                        }
                    }
                    if (cell == tail) {
                        if (next != null) {
                            tail = next;
                        } else {
                            tail = prev;
                        }
                    }
                }
                cell = cell.next;
            }
        }

        public long count() {
            long n = 0;
            HistogramLeafCell cell = head;
            while (cell != null) {
                long s = cell.size();
                if (s == 0) { //the rest of chain is empty
                    break;
                }
                n += s;
                cell = cell.next;
            }
            return n;
        }

        public Iterator<Object> iterator(HistogramTree tree, HistogramTreeNodeLeaf leaf) {
            return new Iterator<Object>() {
                HistogramLeafCell next = head;
                Iterator<Object> nextIter = (head == null ?
                        Collections.emptyIterator() :
                        head.iterator(tree, leaf));
                @Override
                public boolean hasNext() {
                    return nextIter.hasNext();
                }

                @Override
                public Object next() {
                    Object v = nextIter.next();
                    if (!nextIter.hasNext()) {
                        next = next.next;
                        if (next != null) {
                            nextIter = next.iterator(tree, leaf);
                        }
                    }
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
            while (o instanceof HistogramLeafCell) { //ends with HistogramLeafCellSerializedEnd
                HistogramLeafCell cell = (HistogramLeafCell) o;
                if (prev == null) {
                    prev = cell;
                    head = prev;
                } else {
                    prev.setNext(cell);
                    prev = cell;
                }
                o = kryo.readClassAndObject(input);
            }
            tail = prev;
        }

        public void replaceRest(HistogramLeafCell exCell, HistogramLeafCell newCell) {
            if (head == exCell) {
                head = newCell;
            }
            if (exCell.prev != null) {
                exCell.prev.setNext(newCell);
            }
            tail = newCell;
        }

        public void insert(HistogramLeafCell newCell) {
            HistogramLeafCell newCellTail = newCell;
            while (newCellTail.next != null) {
                newCellTail = newCellTail.next;
            }
            newCellTail.setNext(head);
            head = newCell;
            if (tail == null) {
                tail = newCell;
            }
        }
    }

    public static class HistogramLeafCellSerializedEnd implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    public static abstract class HistogramLeafCell implements Serializable, KryoSerializable {
        public HistogramLeafCell prev;
        public HistogramLeafCell next;
        public abstract HistogramLeafCell copy();
        public abstract long size();
        public abstract int sizeOnMemory();
        public abstract long sizePersisted();

        /**
         * @return the cell has free space for {@link #offer(Object)}
         */
        public abstract boolean hasRemaining();

        public boolean isNonEmpty() {
            return size() > 0;
        }

        public void setNext(HistogramLeafCell next) {
            this.next = next;
            if (next != null) {
                next.prev = this;
            }
        }

        /**
         *
         * @param tree a tree or null
         * @return read a value from the cell
         */
//        public Object poll(HistogramTree tree) {
//            return poll(tree, null);
//        }
        public abstract Object poll(HistogramTree tree, HistogramTreeNodeLeaf leaf);

        public abstract Object pollWithReader(Function<PersistentFileManager.PersistentFileReaderSource, PersistentFileManager.PersistentReader> readerFactory);

        /**
         * @param v the appended value
         * @return true if successfully appended
         */
        public abstract boolean offer(Object v);
        public abstract Iterator<Object> iterator(HistogramTree tree, HistogramTreeNodeLeaf leaf);
    }

    public static class HistogramLeafCellArray extends HistogramLeafCell implements Cloneable {
        public static final long serialVersionUID = 1L;
        private Object[] values;
        private int head;
        private int tail; // >=0: [head,...,tail],   <0: [head,...,value.length-1]++[0,...,-tail+1]

        public HistogramLeafCellArray() {}

        public HistogramLeafCellArray(int capacity) {
            this.values = new Object[capacity];
            this.head = 0;
            this.tail = 0;
        }
        public HistogramLeafCellArray(Object[] value, int head, int tail) {
            this.values = value;
            this.head = head;
            this.tail = tail;
        }
        @Override
        public HistogramLeafCell copy() {
            try {
                HistogramLeafCellArray cell = (HistogramLeafCellArray) super.clone();
                cell.values = Arrays.copyOf(values, values.length);
                cell.head = head;
                cell.tail = tail;
                cell.next = null;
                cell.prev = null;
                return cell;
            } catch (CloneNotSupportedException ce) {
                throw new RuntimeException(ce);
            }
        }

        @Override
        public void write(Kryo kryo, Output output) {
            int s = sizeOnMemory();
            int cap = capacity();
            output.writeVarInt(s, true);
            output.writeVarInt(capacity(), true);
            if (tail < 0) {
                for (int i = head; i < cap; ++i) {
                    kryo.writeClassAndObject(output, values[i]);
                }
                for (int i = 0, e = -(tail+1); i < e; ++i) {
                    kryo.writeClassAndObject(output, values[i]);
                }
            } else {
                for (int i = head; i < tail; ++i) {
                    kryo.writeClassAndObject(output, values[i]);
                }
            }
        }

        @Override
        public void read(Kryo kryo, Input input) {
            int s = input.readVarInt(true);
            int cap = input.readVarInt(true);
            Object[] vs = new Object[cap];
            for (int i = 0; i < s; ++i) {
                vs[i] = kryo.readClassAndObject(input);
            }
            this.values = vs;
            this.head = 0;
            if (s == values.length) {
                tail = -1;
            } else {
                tail = s;
            }
        }

        public boolean offer(Object v) {
            if (tail < 0) {
                int t = -(tail+1);
                if (head == t) {
                    return false;
                } else {
                    values[t] = v;
                    --tail;
                    return true;
                }
            } else {
                /*if (head == 0 && tail != value.length) { //never happen
                    //full
                } else { */
                values[tail] = v;
                ++tail;
                if (tail == values.length) {
                    tail = -1;
                }
                return true;
                //}
            }
        }

        public int capacity() {
            return values.length;
        }

        public int remaining() {
            if (tail < 0) {
                int t = -(tail+1);
                return (head - t);
            } else {
                return values.length - (tail - head);
            }
        }

        @Override
        public boolean hasRemaining() {
            return remaining() > 0;
        }
        @Override
        public int sizeOnMemory() {
            if (tail < 0) {
                int t = -(tail+1);
                return values.length - (head - t);
            } else {
                return tail - head;
            }
        }

        @Override
        public long sizePersisted() {
            return 0;
        }

        @Override
        public long size() {
            return sizeOnMemory();
        }

        public Object poll(HistogramTree tree, HistogramTreeNodeLeaf leaf) {
            if (tail < 0) {
                int t = -(tail+1);
                Object v = values[head];
                values[head] = null;
                head++;
                if (head == values.length) {
                    head = 0;
                    tail = t;
                }
                return v;
            } else {
                if (head == tail) { //empty
                    return null;
                } else {
                    Object v = values[head];
                    values[head] = null;
                    head++;
                    return v;
                }
            }
        }

        @Override
        public Object pollWithReader(Function<PersistentFileManager.PersistentFileReaderSource, PersistentFileManager.PersistentReader> readerFactory) {
            return poll(null, null);
        }

        @Override
        public Iterator<Object> iterator(HistogramTree tree, HistogramTreeNodeLeaf leaf) {
            return new Iterator<Object>() {
                int toCapacityIndex;
                final int toCapacityEnd;
                int toTailIndex;
                final int toTailEnd;
                {
                    toCapacityEnd = capacity();
                    if (tail < 0) {
                        toCapacityIndex = head;
                        toTailIndex = 0;
                        toTailEnd = -(tail+1);
                    } else {
                        toCapacityIndex = toCapacityEnd;
                        toTailIndex = head;
                        toTailEnd = tail;
                    }
                }
                @Override
                public boolean hasNext() {
                    return toCapacityIndex < toCapacityEnd || toTailIndex < toTailEnd;
                }

                @Override
                public Object next() {
                    if (toCapacityIndex < toCapacityEnd) {
                        Object v = values[toCapacityIndex];
                        ++toCapacityIndex;
                        return v;
                    } else if (toTailIndex < toTailEnd) {
                        Object v = values[toTailIndex];
                        ++toTailIndex;
                        return v;
                    } else {
                        throw new ArrayIndexOutOfBoundsException();
                    }
                }
            };
        }
    }
}
