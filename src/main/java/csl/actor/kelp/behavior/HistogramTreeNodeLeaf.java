package csl.actor.kelp.behavior;

import csl.actor.kelp.ActorKelpFunctions;
import csl.actor.persist.PersistentFileManager;

import java.util.List;
import java.util.Map;

public abstract class HistogramTreeNodeLeaf implements KeyHistograms.HistogramTreeNode, Cloneable {
    public static final long serialVersionUID = 1L;
    public HistogramTreeNodeTable parent;
    public Object key;
    public long size;
    public long sizePersisted;

    public HistogramTreeNodeLeaf() {
    }

    public HistogramTreeNodeLeaf(Object key, KeyHistograms.HistogramPutContext context) {
        this.key = key;
        initStruct(context);
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    public long sizeOnMemory() {
        return size - sizePersisted;
    }

    @Override
    public KeyHistograms.HistogramTreeNode copy(Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> oldToNew) {
        try {
            HistogramTreeNodeLeaf node = (HistogramTreeNodeLeaf) super.clone();
            oldToNew.put(this, node);
            if (parent != null) {
                node.parent = (HistogramTreeNodeTable) oldToNew.get(parent);
            }
            return node;
        } catch (CloneNotSupportedException ce) {
            throw new RuntimeException(ce);
        }
    }

    public void setSizeAsAllPersisted(long size) {
        this.size = size;
        this.sizePersisted = size;
    }

    public void setStructListSizeAsAllPersisted(int listSize, KeyHistograms.HistogramPutContext context) {}

    public void decrementPersistedSize() {
        sizePersisted--;
    }

    public Object getKey() {
        return key;
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
        return 0;
    }

    @Override
    public long size() {
        return size;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int keyIn(ActorKelpFunctions.KeyComparator<?> comparator, Object key) {
        return ((ActorKelpFunctions.KeyComparator<Object>) comparator).compare(key, this.key);
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
    public KeyHistograms.HistogramTreeNode put(ActorKelpFunctions.KeyComparator<?> comparator, Object key, KeyHistograms.HistogramPutContext context, int height) {
        int c = ((ActorKelpFunctions.KeyComparator<Object>) comparator).compare(key, this.key);
        if (c == 0) {
            putValue(context);
            return null;
        } else {
            return context.createLeafWithCountUp(key);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public KeyHistograms.HistogramTreeNode put(ActorKelpFunctions.KeyComparator<?> comparator, HistogramTree tree, HistogramTreeNodeLeaf leaf, int height) {
        //suppose all leaves have distinct key
        return leaf;
    }

    protected void putValue(KeyHistograms.HistogramPutContext context) {
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
    public KeyHistograms.HistogramTreeNode split(long halfSize, long currentLeft) {
        return null;
    }

    protected abstract void initStruct(KeyHistograms.HistogramPutContext context);

    /**
     * currently the method does not change {@link #sizePersisted}
     *
     * @param context the context
     */
    protected abstract void putValueStruct(KeyHistograms.HistogramPutContext context);

    protected abstract boolean completedAfterPut(KeyHistograms.HistogramPutContext context);

    public void afterTake(long removedSize, HistogramTree tree) {
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

    public abstract List<KeyHistograms.HistogramLeafList> getStructList();

    /**
     * set the leaf struct of i.
     * currently it does not affect {@link #sizePersisted}
     *
     * @param i    the key ID of the list
     * @param list the new list
     */
    public abstract void setStructList(int i, KeyHistograms.HistogramLeafList list);

    public HistogramTreeNodeLeaf load(KeyHistograms.HistogramPutContext context) {
        return this;
    }

    @Override
    public void initPersistent(PersistentFileManager persistent) {
    }
}
