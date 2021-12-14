package csl.actor.kelp.persist;

import csl.actor.kelp.ActorKelpFunctions;
import csl.actor.kelp.behavior.HistogramTree;
import csl.actor.kelp.behavior.HistogramTreeNodeLeaf;
import csl.actor.kelp.behavior.HistogramTreeNodeTable;
import csl.actor.kelp.behavior.KeyHistograms;
import csl.actor.persist.PersistentFileManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HistogramTreeNodeTableOnStorage extends HistogramTreeNodeTable implements HistogramTreePersistable.HistogramNodeOnStorage {
    public static final long serialVersionUID = 1L;
    public PersistentFileManager.PersistentFileReaderSource source;
    public boolean loaded;

    public HistogramTreeNodeTableOnStorage() {
    }

    static ArrayList<KeyHistograms.HistogramTreeNode> empty = new ArrayList<>(0);

    public HistogramTreeNodeTableOnStorage(KeyHistogramsPersistable.NodeTreeData data, PersistentFileManager.PersistentFileReaderSource source) {
        super(empty);
        this.source = source;
        this.keyStart = data.keyStart;
        this.keyEnd = data.keyEnd;
        this.size = data.size;
        loaded = false;
    }

    @Override
    public HistogramTreeNodeTableOnStorage copy(Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> oldToNew) {
        return (HistogramTreeNodeTableOnStorage) super.copy(oldToNew);
    }

    @Override
    public void initPersistent(PersistentFileManager persistent) {
        if (source.getManager() == null) {
            source.setManager(persistent);
        }
    }

    @Override
    public PersistentFileManager.PersistentFileReaderSource getSource() {
        return source;
    }

    @Override
    public KeyHistogramsPersistable.NodeTreeData toData() {
        KeyHistogramsPersistable.NodeTreeData d = new KeyHistogramsPersistable.NodeTreeData();
        d.leaf = false;
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
    public List<KeyHistograms.HistogramTreeNode> getChildren(HistogramTree tree) {
        load(tree);
        return super.getChildren(tree);
    }

    @Override
    public KeyHistograms.HistogramTreeNode put(ActorKelpFunctions.KeyComparator<?> comparator, Object key, KeyHistograms.HistogramPutContext context, int height) {
        load(context);
        return super.put(comparator, key, context, height);
    }

    @Override
    public KeyHistograms.HistogramTreeNode put(ActorKelpFunctions.KeyComparator<?> comparator, HistogramTree tree, HistogramTreeNodeLeaf leaf, int height) {
        load(tree);
        return super.put(comparator, tree, leaf, height);
    }


    @Override
    public KeyHistograms.HistogramTreeNode merge(HistogramTree tree, int treeLimit, ActorKelpFunctions.KeyComparator<?> comparator, KeyHistograms.HistogramTreeNode lowerNode) {
        load((HistogramTree) null); //the caller tree has responsibility to update node-size
        return super.merge(tree, treeLimit, comparator, lowerNode);
    }

    @Override
    public KeyHistograms.HistogramTreeNode split(long halfSize, long currentLeft) {
        load((HistogramTree) null);
        return super.split(halfSize, currentLeft);
    }

    @Override
    public boolean isPersisted() {
        return !loaded;
    }

    @Override
    public HistogramTreeNodeTable load(KeyHistograms.HistogramPutContext context) {
        load(context.putTree);
        return this;
    }

    @Override
    public PersistentFileManager getFileManager() {
        PersistentFileManager m = source.getManager();
        if (m == null && parent instanceof HistogramTreePersistable.HistogramNodeOnStorage) {
            m = ((HistogramTreePersistable.HistogramNodeOnStorage) parent).getFileManager();
            source.setManager(m);
        }
        return m;
    }

    /**
     * load direct children as {@link HistogramTreeNodeLeafOnStorage} or {@link HistogramTreeNodeTableOnStorage}
     * @param tree the tree for updating node-size or null
     * @see KeyHistogramsPersistable.NodeTreeData
     */
    protected void load(HistogramTree tree) {
        if (!loaded) {
            loaded = true;
            int cap = 6;
            if (tree != null) {
                tree.addNodeSizeOnMemory(1L); //onMemory means the node is loaded
                cap = tree.getTreeLimit();
            }
            ArrayList<KeyHistograms.HistogramTreeNode> cs = new ArrayList<>(cap);
            getFileManager(); //setup manager for reading
            try (PersistentFileManager.PersistentFileReader r = source.createReader()) {
                long thisSibling = r.nextLong(); //long sibling
                KeyHistogramsPersistable.NodeTreeData thisData = (KeyHistogramsPersistable.NodeTreeData) r.next(); //NodeTreData

                while (true) {
                    long pos = r.position();
                    long sibling = r.nextLong(); //long sibling
                    Object childObj = r.next(); //PersistentFileEnd | PersistentFileReaderSource | NodeTreeData
                    if (childObj instanceof PersistentFileManager.PersistentFileEnd) { //no further child
                        break;
                    } else if (childObj instanceof PersistentFileManager.PersistentFileReaderSource) { //persisted
                        PersistentFileManager.PersistentFileReaderSource src = (PersistentFileManager.PersistentFileReaderSource) childObj;
                        src.setManager(source.getManager());
                        KeyHistogramsPersistable.NodeTreeData child = (KeyHistogramsPersistable.NodeTreeData) r.next(); //NodeTreeData
                        if (child.leaf) {
                            cs.add(new HistogramTreeNodeLeafOnStorage(child, src));
                        } else {
                            cs.add(new HistogramTreeNodeTableOnStorage(child, src));
                        }
                    } else { //leaf or node
                        KeyHistogramsPersistable.NodeTreeData child = (KeyHistogramsPersistable.NodeTreeData) childObj;
                        if (child.leaf) {
                            cs.add(new HistogramTreeNodeLeafOnStorage(child, source.newSource(pos)));
                        } else {
                            cs.add(new HistogramTreeNodeTableOnStorage(child, source.newSource(pos)));
                        }
                    }
                    if (sibling > 0) {
                        r.position(sibling);
                    } else {
                        break;
                    }
                }
                this.children = cs;
                updateChildren();
                if (PersistentFileManager.logDebugPersist) r.getManager().getLogger().log(KeyHistogramsPersistable.logPersistColor, "close: %s", r);
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
