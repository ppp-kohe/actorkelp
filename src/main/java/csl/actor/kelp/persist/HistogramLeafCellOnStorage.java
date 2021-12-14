package csl.actor.kelp.persist;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.kelp.behavior.HistogramTree;
import csl.actor.kelp.behavior.HistogramTreeNodeLeaf;
import csl.actor.kelp.behavior.KeyHistograms;
import csl.actor.persist.PersistentFileManager;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

/**
 * <pre>
 *     var long totalSize, //remainingCount
 *     var int maxLinkDepth,
 *     PersistentFileReaderSource source, //the top position of segment
 *     -----------------------------
 *     CellSegment segments...
 *        0 (null)
 *     |  1 (array):  long size, Object[] values[size]
 *     |  2 (obj)  :  Object object //including CellOnStorage
 *     |  3 (end)  : (none)
 *     HistogramLeafCellSerializedEnd
 * </pre>
 */
public class HistogramLeafCellOnStorage extends KeyHistograms.HistogramLeafCell implements Cloneable {
    public static final long serialVersionUID = 1L;
    public static final byte CELL_SEGMENT_NULL = 0;
    public static final byte CELL_SEGMENT_ARRAY = 1;
    public static final byte CELL_SEGMENT_OBJECT = 2;
    public static final byte CELL_SEGMENT_END = 3;
    public static final CellSegmentNull CELL_SEGMENT_NULL_OBJ = new CellSegmentNull();

    protected PersistentFileManager.PersistentFileReaderSource source; //offset: listPointer
    protected transient PersistentFileManager.PersistentReader reader;
    protected transient Function<PersistentFileManager.PersistentFileReaderSource, PersistentFileManager.PersistentReader> readerFactory;
    protected long remainingCount;
    protected int maxLinkDepth; //the CellSegmentObject maxLinkDepth + 1
    protected CellSegment currentSegment = CELL_SEGMENT_NULL_OBJ;

    public HistogramLeafCellOnStorage(PersistentFileManager.PersistentFileReaderSource source, long remainingCount, int maxLinkDepth) {
        this.source = source;
        this.remainingCount = remainingCount;
        this.maxLinkDepth = maxLinkDepth;
    }

    public HistogramLeafCellOnStorage(PersistentFileManager.PersistentFileReaderSource source, long remainingCount, int maxLinkDepth, CellSegment currentSegment) {
        this.source = source;
        this.remainingCount = remainingCount;
        this.maxLinkDepth = maxLinkDepth;
        this.currentSegment = currentSegment;
    }

    @Override
    public long size() {
        return remainingCount;
    }

    @Override
    public int sizeOnMemory() {
        return 0;
    }

    @Override
    public long sizePersisted() {
        return remainingCount;
    }

    @Override
    public boolean hasRemaining() {
        return false;
    }

    @Override
    public Object poll(HistogramTree tree, HistogramTreeNodeLeaf leaf) {
        return poll(tree, leaf, true);
    }

    public Object poll(HistogramTree tree, HistogramTreeNodeLeaf leaf, boolean decrementTreePersistedSize) {
        if (currentSegment == CELL_SEGMENT_NULL_OBJ) {
            loadSegment(tree);
        }
        if (currentSegment != CELL_SEGMENT_NULL_OBJ && remainingCount > 0) {
            Object v = currentSegment.poll(tree, leaf, this, false); //avoid double decrement
            --remainingCount;
            if (!currentSegment.isNonEmpty()) {
                currentSegment = CELL_SEGMENT_NULL_OBJ;
            }
            if (remainingCount == 0) {
                if (reader != null) {
                    setSource(source); //close
                }
            }
            if (decrementTreePersistedSize) {
                if (tree instanceof HistogramTreePersistable) {
                    ((HistogramTreePersistable) tree).decrementPersistedSize();
                }
                if (leaf != null) {
                    leaf.decrementPersistedSize();
                }
            }
            return v;
        } else {
            return null;
        }
    }

    @Override
    public Object pollWithReader(Function<PersistentFileManager.PersistentFileReaderSource, PersistentFileManager.PersistentReader> readerFactory) {
        if (currentSegment == CELL_SEGMENT_NULL_OBJ) {
            loadSegment(readerFactory);
        }
        if (currentSegment != CELL_SEGMENT_NULL_OBJ && remainingCount > 0) {
            Object v = currentSegment.pollWithReader(reader, readerFactory);
            --remainingCount;
            if (!currentSegment.isNonEmpty()) {
                currentSegment = CELL_SEGMENT_NULL_OBJ;
            }
            if (remainingCount == 0) {
                if (reader != null) {
                    setSource(source);
                }
            }
            return v;
        } else {
            return null;
        }
    }


    @Override
    public boolean offer(Object v) {
        return false;
    }

    public PersistentFileManager.PersistentFileReaderSource currentSource() {
        return reader == null ? source :
                source.newSource(reader.position());
    }

    public PersistentFileManager.PersistentReader reader(HistogramTree tree) {
        if (reader == null) {
            if (readerFactory != null) {
                reader = readerFactory.apply(source);
            } else if (tree != null) {
                reader = reader(tree, source);
            }
        }
        return reader;
    }

    public static PersistentFileManager.PersistentReader reader(HistogramTree tree, PersistentFileManager.PersistentFileReaderSource source) {
        if (source.getManager() == null) {
            source.setManager(tree.getPersistent());
        }
        return source.createReader();
    }

    public void setSource(PersistentFileManager.PersistentFileReaderSource source) {
        if (this.source != null && reader != null) {
            reader.close();
            reader = null;
        }
        this.source = source;
    }

    public int getMaxLinkDepth() {
        return maxLinkDepth;
    }

    public PersistentFileManager.PersistentFileReaderSource getSource() {
        return source;
    }

    public CellSegment getCurrentSegment() {
        return currentSegment;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeVarLong(remainingCount, true);
        output.writeVarInt(maxLinkDepth, true);
        kryo.writeClassAndObject(output, currentSource());
        currentSegment.write(kryo, output, this);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        remainingCount = input.readVarLong(true);
        maxLinkDepth = input.readVarInt(true);
        source = (PersistentFileManager.PersistentFileReaderSource) kryo.readClassAndObject(input);
        byte b = input.readByte();
        switch (b) {
            case CELL_SEGMENT_ARRAY:
                currentSegment = new CellSegmentArray(kryo, input);
                break;
            case CELL_SEGMENT_OBJECT:
                currentSegment = new CellSegmentObject(kryo, input);
                break;
            case CELL_SEGMENT_END:
                currentSegment = new CellSegmentEnd();
                break;
            case CELL_SEGMENT_NULL:
                currentSegment = CELL_SEGMENT_NULL_OBJ;
                break;
        }
    }

    protected void loadSegment(HistogramTree tree) {
        currentSegment = loadSegment(reader(tree));
    }

    protected void loadSegment(Function<PersistentFileManager.PersistentFileReaderSource, PersistentFileManager.PersistentReader> readerFactory) {
        if (readerFactory != null) {
            this.readerFactory = readerFactory;
        }
        currentSegment = loadSegment(reader(null));
    }

    public static CellSegment loadSegment(PersistentFileManager.PersistentReader reader) {
        byte b = reader.nextByte(); //set loader
        switch (b) {
            case CELL_SEGMENT_ARRAY:
                return new CellSegmentArray(reader);
            case CELL_SEGMENT_OBJECT:
                return new CellSegmentObject(reader);
            case CELL_SEGMENT_END:
                return new CellSegmentEnd();
            case CELL_SEGMENT_NULL:
                return CELL_SEGMENT_NULL_OBJ;
            default:
                return CELL_SEGMENT_NULL_OBJ;
        }
    }

    @Override
    public KeyHistograms.HistogramLeafCell copy() {
        try {
            HistogramLeafCellOnStorage copy = (HistogramLeafCellOnStorage) super.clone();
            copy.prev = null;
            copy.next = null;
            copy.source = currentSource();
            copy.remainingCount = remainingCount;
            copy.currentSegment = currentSegment.copy(this);
            copy.reader = null;
            copy.maxLinkDepth = maxLinkDepth;
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Object> iterator(HistogramTree tree, HistogramTreeNodeLeaf leaf) {
        return new Iterator<Object>() {
            final HistogramLeafCellOnStorage reading = (HistogramLeafCellOnStorage) copy();

            @Override
            public boolean hasNext() {
                return reading.isNonEmpty();
            }

            @Override
            public Object next() {
                return reading.poll(tree, leaf, false);
            }
        };
    }

    /**
     * writing {@link CellSegment}s ending with {@link #CELL_SEGMENT_END}
     *    and {@link KeyHistograms.HistogramLeafCellSerializedEnd}
     */
    public static class  HistogramLeafCellOnStorageWriting {
        /** newly persisted size */
        public long persistedSize;
        public long totalSize;
        public int maxLinkDepth;
        /** available by {@link #writeCell(HistogramTree, HistogramTreeNodeLeaf, KeyHistograms.HistogramLeafCell, PersistentFileManager.PersistentWriter)} */
        public HistogramLeafCellOnStorage cell;

        public static HistogramLeafCellOnStorageWriting writeCell(HistogramTree tree,
                                                                  HistogramTreeNodeLeaf leaf,
                                                                  KeyHistograms.HistogramLeafCell persistedCellChain, PersistentFileManager.PersistentWriter writer) {
            PersistentFileManager.PersistentFileReaderSource src = writer.createReaderSourceFromCurrentPosition();
            HistogramLeafCellOnStorageWriting w = new HistogramLeafCellOnStorageWriting(tree, leaf, persistedCellChain, writer);
            w.cell = new HistogramLeafCellOnStorage(src, w.totalSize, w.maxLinkDepth);
            return w;
        }

        public static HistogramLeafCellOnStorageWriting write(HistogramTree tree,
                                                              HistogramTreeNodeLeaf leaf,
                                                              KeyHistograms.HistogramLeafCell persistedCellChain, PersistentFileManager.PersistentWriter writer) {
            return new HistogramLeafCellOnStorageWriting(tree, leaf, persistedCellChain, writer);
        }

        public HistogramLeafCellOnStorageWriting(HistogramTree tree,
                                                 HistogramTreeNodeLeaf leaf,
                                                 KeyHistograms.HistogramLeafCell persistedCellChain, PersistentFileManager.PersistentWriter writer) {
            long totalSize = 0;
            long persistedSize = 0;
            int maxLinkDepth = 0;
            try {
                KeyHistograms.HistogramLeafCell next = persistedCellChain;
                while (next != null) {
                    persistedSize += next.sizeOnMemory();
                    totalSize += next.size();
                    if (next instanceof  KeyHistograms.HistogramLeafCellArray) {
                        writeAsArray(tree, leaf, next, writer);
                    } else if (next instanceof HistogramLeafCellOnStorage) {
                        int max = ((HistogramLeafCellOnStorage) next).getMaxLinkDepth();
                        if (max >= 3) {
                            writeAsArray(tree, leaf, next, writer);
                        } else {
                            writer.writeByte(CELL_SEGMENT_OBJECT);
                            writer.write(next);
                            maxLinkDepth = Math.max(max + 1, maxLinkDepth);
                        }
                    } else {
                        writer.writeByte(CELL_SEGMENT_OBJECT);
                        writer.write(next);
                    }
                    next = next.next;
                }
                writer.writeByte(CELL_SEGMENT_END);
                writer.write(KeyHistograms.CELL_SERIALIZED_END);
                this.totalSize = totalSize;
                this.persistedSize = persistedSize;
                this.maxLinkDepth = maxLinkDepth;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        protected void writeAsArray(HistogramTree tree,
                                    HistogramTreeNodeLeaf leaf,
                                    KeyHistograms.HistogramLeafCell next, PersistentFileManager.PersistentWriter writer) throws IOException {
            writer.writeByte(CELL_SEGMENT_ARRAY);
            long remain = next.size();
            writer.writeVarLong(remain, true);
            Iterator<Object> iter = next.iterator(tree, leaf);
            while (remain > 0) {
                if (iter.hasNext()) {
                    writer.write(iter.next());
                } else {
                    writer.write(null); //error
                }
                --remain;
            }
        }

        ///////////////

        public HistogramLeafCellOnStorageWriting(PersistentFileManager.PersistentFileReader reader,
                                                 PersistentFileManager.PersistentWriter writer) throws IOException {
            totalSize = 0;
            persistedSize = 0;
            CellSegment segment = loadSegment(reader);
            while (!(segment instanceof CellSegmentEnd || segment instanceof CellSegmentNull)) {
                long[] res = segment.move(reader, writer);
                persistedSize += res[0];
                totalSize += res[1];
                maxLinkDepth = Math.max(maxLinkDepth, (int) res[2]);
                segment = loadSegment(reader);
            }
            writer.writeByte(CELL_SEGMENT_END);
            writer.write(KeyHistograms.CELL_SERIALIZED_END);
        }
    }

    public static abstract class CellSegment {
        public abstract CellSegment copy(HistogramLeafCellOnStorage cell);
        public abstract Object poll(HistogramTree tree, HistogramTreeNodeLeaf leaf, HistogramLeafCellOnStorage cell, boolean decrementTreePersistedSize);
        public abstract void write(Kryo kryo, Output output, HistogramLeafCellOnStorage cell);
        public abstract boolean isNonEmpty();
        //{persistedSize, totalSize, maxLinkDepth}
        public abstract long[] move(PersistentFileManager.PersistentFileReader reader, PersistentFileManager.PersistentWriter writer) throws IOException;
        public abstract Object pollWithReader(PersistentFileManager.PersistentReader reader,
                                              Function<PersistentFileManager.PersistentFileReaderSource, PersistentFileManager.PersistentReader> readerFactory);
    }

    public static class CellSegmentArray extends CellSegment {
        protected long remainingCount;

        public CellSegmentArray(PersistentFileManager.PersistentReader reader) {
            remainingCount = reader.nextVarLong(true);
        }

        public CellSegmentArray(Kryo kryo, Input input) {
            remainingCount = input.readVarLong(true);
        }

        public CellSegmentArray(long remainingCount) {
            this.remainingCount = remainingCount;
        }

        @Override
        public CellSegment copy(HistogramLeafCellOnStorage cell) {
            return new CellSegmentArray(remainingCount);
        }

        @Override
        public Object poll(HistogramTree tree, HistogramTreeNodeLeaf leaf, HistogramLeafCellOnStorage cell, boolean decrementTreePersistedSize) {
            return pollWithReader(cell.reader(tree), null);
        }

        @Override
        public void write(Kryo kryo, Output output, HistogramLeafCellOnStorage cell) {
            output.writeByte(CELL_SEGMENT_ARRAY);
            output.writeVarLong(remainingCount, true);
        }

        @Override
        public boolean isNonEmpty() {
            return remainingCount > 0;
        }

        @Override
        public long[] move(PersistentFileManager.PersistentFileReader reader, PersistentFileManager.PersistentWriter writer) throws IOException {
            writer.writeByte(CELL_SEGMENT_ARRAY);
            writer.writeVarLong(remainingCount, true);
            for (long i = 0; i < remainingCount; ++i) {
                writer.write(reader.next());
            }
            return new long[] {remainingCount, remainingCount, 0};
        }

        @Override
        public Object pollWithReader(PersistentFileManager.PersistentReader reader,
                                     Function<PersistentFileManager.PersistentFileReaderSource, PersistentFileManager.PersistentReader> readerFactory) {
            if (remainingCount > 0) {
                Object v = reader.next();
                --remainingCount;
                return v;
            } else {
                return null;
            }
        }
    }

    public static class CellSegmentObject extends CellSegment {
        protected KeyHistograms.HistogramLeafCell cell;

        public CellSegmentObject(PersistentFileManager.PersistentReader reader) {
            cell = (KeyHistograms.HistogramLeafCell) reader.next();
        }

        public CellSegmentObject(Kryo kryo, Input input) {
            cell = (KeyHistograms.HistogramLeafCell) kryo.readClassAndObject(input);
        }

        public CellSegmentObject(KeyHistograms.HistogramLeafCell cell) {
            this.cell = cell;
        }

        @Override
        public Object poll(HistogramTree tree, HistogramTreeNodeLeaf leaf, HistogramLeafCellOnStorage cell, boolean decrementTreePersistedSize) {
            if (this.cell instanceof HistogramLeafCellOnStorage) {
                return ((HistogramLeafCellOnStorage) this.cell).poll(tree, leaf, decrementTreePersistedSize);
            } else {
                return this.cell.poll(tree, leaf);
            }
        }

        @Override
        public void write(Kryo kryo, Output output, HistogramLeafCellOnStorage cell) {
            output.writeByte(CELL_SEGMENT_OBJECT);
            kryo.writeClassAndObject(output, this.cell);
        }

        @Override
        public CellSegment copy(HistogramLeafCellOnStorage cell) {
            return new CellSegmentObject(this.cell.copy());
        }

        @Override
        public boolean isNonEmpty() {
            return this.cell.isNonEmpty();
        }

        @Override
        public long[] move(PersistentFileManager.PersistentFileReader reader, PersistentFileManager.PersistentWriter writer) throws IOException {
            writer.writeByte(CELL_SEGMENT_OBJECT);
            writer.write(this.cell);
            int depth = 0;
            if (cell instanceof HistogramLeafCellOnStorage) {
                depth = ((HistogramLeafCellOnStorage) cell).getMaxLinkDepth();
            }
            return new long[] {cell.sizeOnMemory(), cell.size(), depth};
        }

        @Override
        public Object pollWithReader(PersistentFileManager.PersistentReader reader,
                                     Function<PersistentFileManager  .PersistentFileReaderSource, PersistentFileManager.PersistentReader> readerFactory) {
            return cell.pollWithReader(readerFactory);
        }
    }

    public static class CellSegmentEnd extends CellSegment {
        @Override
        public Object poll(HistogramTree tree, HistogramTreeNodeLeaf leaf,
                           HistogramLeafCellOnStorage cell, boolean decrementTreePersistedSize) {
            return null;
        }

        @Override
        public void write(Kryo kryo, Output output, HistogramLeafCellOnStorage cell) {
            output.writeByte(CELL_SEGMENT_END);
        }

        @Override
        public CellSegment copy(HistogramLeafCellOnStorage cell) {
            return this;
        }

        @Override
        public boolean isNonEmpty() {
            return false;
        }

        @Override
        public long[] move(PersistentFileManager.PersistentFileReader reader, PersistentFileManager.PersistentWriter writer) throws IOException {
            writer.writeByte(CELL_SEGMENT_END);
            return new long[] {0, 0, 0};
        }

        @Override
        public Object pollWithReader(PersistentFileManager.PersistentReader reader, Function<PersistentFileManager.PersistentFileReaderSource, PersistentFileManager.PersistentReader> readerFactory) {
            return null;
        }
    }

    public static class CellSegmentNull extends CellSegment {
        @Override
        public Object poll(HistogramTree tree, HistogramTreeNodeLeaf leaf,
                           HistogramLeafCellOnStorage cell, boolean decrementTreePersistedSize) {
            return null;
        }

        @Override
        public void write(Kryo kryo, Output output, HistogramLeafCellOnStorage cell) {
            output.writeByte(CELL_SEGMENT_NULL);
        }

        @Override
        public CellSegment copy(HistogramLeafCellOnStorage cell) {
            return this;
        }

        @Override
        public boolean isNonEmpty() {
            return false;
        }

        @Override
        public long[] move(PersistentFileManager.PersistentFileReader reader, PersistentFileManager.PersistentWriter writer) throws IOException {
            writer.writeByte(CELL_SEGMENT_NULL);
            return new long[] {0, 0, 0};
        }

        @Override
        public Object pollWithReader(PersistentFileManager.PersistentReader reader, Function<PersistentFileManager.PersistentFileReaderSource, PersistentFileManager.PersistentReader> readerFactory) {
            return null;
        }
    }
}
