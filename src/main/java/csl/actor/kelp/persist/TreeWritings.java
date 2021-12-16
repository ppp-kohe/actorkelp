package csl.actor.kelp.persist;

import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.persist.PersistentFileManager;
import csl.actor.remote.KryoBuilder;
import csl.actor.util.MmapOutput;
import csl.actor.util.MmapState;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.TreeMap;

public class TreeWritings {


//    public static final int MAX_LOCAL_WRITERS = 10;
//    public static final ArrayBlockingQueue<Integer> localWriters = new ArrayBlockingQueue<>(MAX_LOCAL_WRITERS);
    public static void lockWriter() {
//        try {
//            localWriters.put(1);
//        } catch (InterruptedException ie) {
//            ie.printStackTrace();
//        }
    }

    public static void unlockWriter() {
//        localWriters.poll();
    }


    public static TreeWriting createWriter(PersistentFileManager manager, String pathExpanded, long offset) throws IOException {
        return new TreeWriting(manager, pathExpanded, offset);
    }

    public static class TreeWriting implements Cloneable, PersistentFileManager.PersistentWriter {
        protected TreeWriting parent;
        protected long pointerTarget = -1L;
        protected long positionStart;
        protected PersistentFileManager manager;
        protected String pathExpanded;
        protected KryoBuilder.SerializerFunction serializer;
        /**
         * buffer for obtaining bytes and total size
         */
        protected Output out;
        protected MmapState.MmapStateOutputStream outMmap;
        protected Path filePath;

        public TreeWriting(PersistentFileManager manager, String pathExpanded, long offset) throws IOException {
            this.manager = manager;
            this.serializer = manager.getSerializer();
            if (this.serializer instanceof KryoBuilder.SerializerPoolDefault) {
                this.serializer = ((KryoBuilder.SerializerPoolDefault) this.serializer).cached();
            }
            this.pathExpanded = pathExpanded;
            Path p = manager.getPathForExpandedPath(pathExpanded, true);
            filePath = p;
            manager.openForWrite(p);
            Path file = manager.getPathForExpandedPath(pathExpanded, true);
            initDataStore(file);
            if (offset > 0) {
                dataStoreSeek(offset);
            }
        }

        @Override
        public PersistentFileManager.PersistentFileReaderSource createReaderSourceFromCurrentPosition() {
            return new PersistentFileManager.PersistentFileReaderSource(pathExpanded, position(), manager);
        }

        protected void initDataStore(Path file) throws IOException {
            outMmap = new MmapState.MmapStateOutputStream(new MmapState.MmapStateWrite(file));
            this.out = new Output(outMmap);
        }

        public String getPathExpanded() {
            return pathExpanded;
        }

        public PersistentFileManager getManager() {
            return manager;
        }

        @Override
        public long position() {
            //return out.total(); //reset by dataStoreSeek(n)
            return outMmap.position() + out.position(); //mmap position plus out buffer position
        }

        @Override
        public void close() throws IOException {
            if (parent == null) {
                closeDataStore();
                manager.close(filePath);
                if (serializer instanceof KryoBuilder.SerializerPoolCached) {
                    ((KryoBuilder.SerializerPoolCached) serializer).close();
                }
            }
        }

        public void flush() {
            out.flush();
            out.reset();
        }

        protected void closeDataStore() throws IOException {
            out.close();
        }

        public TreeWriting subWriting() {
            try {
                TreeWriting w = (TreeWriting) super.clone();
                w.parent = this;
                w.pointerTarget = -1L;
                return w;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

        public PersistentFileManager.PersistentFileReaderSource writeBegin() throws IOException {
            writePositionToPointer(pointerTarget);
            positionStart = position();
            writeLong(0L); //sibling pointer
            return new PersistentFileManager.PersistentFileReaderSource(pathExpanded, positionStart, manager);
        }

        @Override
        public Serializer<?> serializer(Class<?> type) {
            return serializer.serializer(type);
        }

        @Override
        public void write(Object v, Serializer<?> serializer) {
            this.serializer.write(out, v, serializer);
        }

        @Override
        public void write(Object obj) throws IOException {
            serializer.write(out, obj);
        }

        public void writeBytes(byte[] data) {
            out.writeBytes(data, 0, data.length);
        }

        public long positionAndWriteLong(long n) throws IOException {
            long p = position();
            writeLong(n);
            return p;
        }

        public long positionAndWriteInt(int n) throws IOException {
            long p = position();
            writeInt(n);
            return p;
        }

        @Override
        public void writeVarInt(int n, boolean optimizePositive) throws IOException {
            out.writeVarInt(n, optimizePositive);
        }

        @Override
        public void writeVarLong(long n, boolean optimizePositive) throws IOException {
            out.writeVarLong(n, optimizePositive);
        }

        @Override
        public void writeLong(long n) throws IOException {
            out.writeLong(n);
        }

        @Override
        public void writeInt(int i) throws IOException {
            out.writeInt(i);
        }

        @Override
        public void writeByte(byte b) throws IOException {
            out.writeByte(b);
        }

        public void writePositionToPointer(long p) throws IOException {
            if (p >= 0) {
                long pos = position();
                dataStoreSeek(p);
                writeLong(pos);
                dataStoreSeek(pos);
            }
        }

        protected void dataStoreSeek(long p) throws IOException {
            out.flush();
            out.reset();
            outMmap.seek(p);
        }

        public void writeEnd() throws IOException {
            pointerTarget = positionStart;
        }

        public void writeSeekDataToPointer(long p, TreeWritingSeekData d) throws IOException {
            if (p >= 0) {
                long pos = position();
                dataStoreSeek(p);
                d.write(this);
                dataStoreSeek(pos);
            }
        }

        @Override
        public String toString() {
            return String.format("tree-writer(path=%s, pos=%,d", pathExpanded, position());
        }
    }

    public static class TreeReader implements PersistentFileManager.PersistentReader {
        protected PersistentFileManager manager;
        protected KryoBuilder.SerializerFunction serializer;
        protected String pathExpanded;
        protected Path file;
        protected int bufferSize = 4_096;
        protected Input input;
        protected MmapState.MmapStateRead state;
        protected boolean closable;

        public TreeReader(PersistentFileManager manager, String pathExpanded, long offset) throws IOException {
            this.manager = manager;
            this.serializer = manager.getSerializer();
            if (this.serializer instanceof KryoBuilder.SerializerPoolDefault) {
                this.serializer = ((KryoBuilder.SerializerPoolDefault) this.serializer).cached();
            }
            this.pathExpanded = pathExpanded;
            file = manager.getPathForExpandedPath(pathExpanded, true);
            state = new MmapState.MmapStateRead(file);
            state.seek(offset);
            input = new Input(new MmapState.MmapStateInputStream(state), bufferSize);
            closable = true;
        }

        public TreeReader(TreeReader base, long offset) throws IOException {
            this.manager = base.manager;
            this.serializer = base.serializer;
            this.pathExpanded = base.pathExpanded;
            this.file = base.file;
            state = base.state.copy();
            state.seek(offset);
            input = new Input(new MmapState.MmapStateInputStream(state), bufferSize);
            closable = false;
        }

        public TreeReader copy() throws IOException {
            return new TreeReader(this, position());
        }

        public PersistentFileManager.PersistentReader get(PersistentFileManager.PersistentFileReaderSource source) {
            if (pathExpanded.equals(source.pathExpanded)) {
                try {
                    return new TreeReader(this, source.offset);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                if (source.getManager() != null) {
                    source.setManager(manager);
                }
                return source.createReader();
            }
        }

        @Override
        public long position() {
            return state.current.position();
        }
        @Override
        public byte nextByte() {
            return input.readByte();
        }
        @Override
        public int nextInt() {
            return input.readInt();
        }
        @Override
        public long nextLong() {
            return input.readLong();
        }

        @Override
        public long nextVarLong(boolean optimizePositive) {
            return input.readVarLong(optimizePositive);
        }
        @Override
        public int nextVarInt(boolean optimizePositive) {
            return input.readVarInt(optimizePositive);
        }

        @Override
        public Serializer<?> serializer(Class<?> type) {
            return serializer.serializer(type);
        }

        @Override
        public Object next(Class<?> type, Serializer<?> serializer) {
            return this.serializer.read(input, type, serializer);
        }

        @Override
        public Object next() {
            return serializer.read(input);
        }

        public void seek(long pos) throws IOException {
            state.seek(pos);
            input.setLimit(0); //require to re-loading
            input.reset();
        }

        public void close() {
            if (closable) {
                try {
                    state.close();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    public static class TreeWritingDelay extends TreeWriting {
        TreeWritingDelayData pointers;
        public TreeWritingDelay(PersistentFileManager manager, String pathExpanded, long offset) throws IOException {
            super(manager, pathExpanded, offset);
            pointers = new TreeWritingDelayData();
        }

        @Override
        public void writePositionToPointer(long p) throws IOException {
            if (p >= 0) {
                pointers.put(this, p, position());
            }
        }

        @Override
        public void writeSeekDataToPointer(long p, TreeWritingSeekData d) throws IOException {
            if (p >= 0) {
                pointers.put(this, p, d);
            }
        }

        @Override
        public void close() throws IOException {
            if (parent == null) {
                pointers.flush(this);
            }
            super.close();
        }
    }

    public interface TreeWritingSeekData {
        void write(TreeWriting w) throws IOException;
    }

    public static class TreeWritingDelayData {
        protected TreeMap<Long,Object> pointToData;

        public TreeWritingDelayData() {
            pointToData = new TreeMap<>();
        }

        public void put(TreeWriting w, long pointer, long pos) throws IOException {
            flushIfOver(w);
            pointToData.put(pointer, pos);
        }
        public void put(TreeWriting w, long pointer, TreeWritingSeekData d) throws IOException {
            flushIfOver(w);
            pointToData.put(pointer, d);
        }
        private void flushIfOver(TreeWriting w) throws IOException {
            if (pointToData.size() >= 300_000) {
                flush(w);
            }
        }

        protected void flush(TreeWriting w) throws IOException {
            Instant n = Instant.now();
            long backPos = w.position();
            pointToData.forEach((loc, data) -> {
                try {
                    w.dataStoreSeek(loc);
                    if (data instanceof Long) {
                        w.writeLong((Long) data);
                    } else {
                        ((TreeWritingSeekData) data).write(w);
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
            w.dataStoreSeek(backPos);
            pointToData.clear();
            if (PersistentFileManager.logPersist) w.getManager().getLogger().log(KeyHistogramsPersistable.logPersistColor,
                    "flush sorted pointers %s", Duration.between(n, Instant.now()));
        }
    }
}
