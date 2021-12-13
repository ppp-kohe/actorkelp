package csl.actor.kelp.persist;

import csl.actor.kelp.behavior.HistogramTreeNodeLeaf;
import csl.actor.kelp.behavior.KeyHistograms;
import csl.actor.persist.PersistentFileManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HistogramTreeNodeLeafOnStorage extends HistogramTreeNodeLeaf implements HistogramTreePersistable.HistogramNodeOnStorage {
    public static final long serialVersionUID = 1L;
    public PersistentFileManager.PersistentFileReaderSource source;

    public HistogramTreeNodeLeafOnStorage() {
    }

    public HistogramTreeNodeLeafOnStorage(KeyHistogramsPersistable.NodeTreeData data, PersistentFileManager.PersistentFileReaderSource source) {
        super(data.keyStart, null);
        this.source = source;
        this.size = data.size;
        this.sizePersisted = size;
    }

    @Override
    public HistogramTreeNodeLeafOnStorage copy(Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> oldToNew) {
        HistogramTreeNodeLeafOnStorage node = (HistogramTreeNodeLeafOnStorage) super.copy(oldToNew);
        return node;
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
        d.leaf = true;
        d.keyStart = key;
        d.keyEnd = key;
        d.size = size;
        return d;
    }

    @Override
    protected void initStruct(KeyHistograms.HistogramPutContext context) {
    }

    @Override
    protected void putValueStruct(KeyHistograms.HistogramPutContext context) {
        PersistentFileManager m = getFileManager();
        if (m != null) {
            m.getLogger().log(PersistentFileManager.logPersist, KeyHistogramsPersistable.logPersistColor,
                    "HistogramNodeLeafOnStorage.putValueStruct: illegal operation %s",
                    this);
        }
    }

    @Override
    protected boolean completedAfterPut(KeyHistograms.HistogramPutContext context) {
        return false;
    }

    @Override
    public List<KeyHistograms.HistogramLeafList> getStructList() {
        return Collections.emptyList();
    }

    @Override
    public void setStructList(int i, KeyHistograms.HistogramLeafList list) {
    }

    @Override
    public boolean isPersisted() {
        return true;
    }

    @Override
    public long sizeOnMemory() {
        return 0;
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
     * @param context the context
     * @return the loaded leaf
     * @see KeyHistogramsPersistable.NodeTreeData
     */
    @Override
    public HistogramTreeNodeLeaf load(KeyHistograms.HistogramPutContext context) {
        getFileManager();
        try (PersistentFileManager.PersistentFileReader r = source.createReader()) { //leaf
            long thisSibling = r.nextLong(); //long sibling
            KeyHistogramsPersistable.NodeTreeData thisData = (KeyHistogramsPersistable.NodeTreeData) r.next(); //NodeTreeData
            Class<?> leafType = (Class<?>) r.next(); //Class nodeType
            HistogramTreeNodeLeaf leaf = context.createLeafPersisted(leafType, key);
            leaf.setSizeAsAllPersisted(size);

            int listCount = r.nextVarInt(true);
            leaf.setStructListSizeAsAllPersisted(listCount, context);

            KeyHistogramsPersistable.LeafCellHeader[] headers = KeyHistogramsPersistable.createCellHeaders(listCount);
            for (KeyHistogramsPersistable.LeafCellHeader h : headers) {
                h.read(r);
            }

            List<HistogramLeafCellOnStorage> cells = new ArrayList<>(listCount);
            for (int i = 0; i < listCount; ++i) {
                KeyHistogramsPersistable.LeafCellHeader header = headers[i];
                cells.add(new HistogramLeafCellOnStorage(
                        source.newSource(header.listPointer), header.size, header.maxLinkDepth));
            }
            List<KeyHistograms.HistogramLeafList> structs = leaf.getStructList();
            for (int i = 0; i < listCount; ++i) {
                KeyHistograms.HistogramLeafList list;
                if (i < structs.size()) {
                    list = structs.get(i);
                } else {
                    list = context.createEmptyList();
                    leaf.setStructList(i, list);
                }
                HistogramLeafCellOnStorage cell = cells.get(i);
                list.insert(cell);
            }

            if (getParent() != null) {
                leaf.setParent(getParent());
                List<KeyHistograms.HistogramTreeNode> ns = getParent().getChildrenOnMemory();
                int idx = ns.indexOf(this);
                if (idx >= 0) {
                    ns.set(idx, leaf);
                }
            }
            if (PersistentFileManager.logDebugPersist) r.getManager().getLogger().log(KeyHistogramsPersistable.logPersistColor, "close: %s", r);

            if (context.putTree != null) {
                context.putTree.addLeafSizeOnMemory(1L);
            }
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
