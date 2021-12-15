package csl.actor.util;

import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public abstract class MmapState {
    protected FileChannel channel;
    protected List<MmapBlock> blocks;
    public MmapBlock current;

    protected BufferIndexer indexer;

    public MmapState(FileChannel channel, BufferIndexer indexer) throws IOException {
        this.channel = channel;
        this.indexer = indexer;
        blocks = new ArrayList<>();
        initBlock();
    }

    public boolean isReadOnly() {
        return false;
    }

    public void close() throws IOException {
        blocks.clear();
        channel.close();
    }

    protected void initBlock() throws IOException {
        newBlock(0);
    }

    protected void nextNewBlock() throws IOException {
        int newIndex = blocks.size();
        MmapBlock last = blocks.get(newIndex - 1);
        if (last.end() >= indexer.getLength()) {
            return;
        }
        newBlock(newIndex);
    }

    private void newBlock(int newIndex) throws IOException {
        long start = indexer.getMinDataIndexForBufferIndex(newIndex);
        long end = indexer.getMaxDataIndexForBufferIndex(newIndex);

        MmapBlock b = new MmapBlock(newIndex, start, end, channel, isReadOnly());
        blocks.add(b);
        current = b;
    }

    public void nextBlock() throws IOException {
        if (current.index + 1 == blocks.size()) {
            nextNewBlock();
        } else {
            current = blocks.get(current.index + 1);
            current.buffer.position(0);
        }
    }

    public void seek(long p) {
        int index = indexer.getBufferIndexFromDataIndex(p);
        if (index < blocks.size()) {
            current = blocks.get(index).position(p);
        } else {
            while (true) {
                try {
                    nextNewBlock();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                if (index < blocks.size()) {
                    current.position(p);
                    return;
                }
            }
        }
    }

    public static class MmapBlock {
        public int index;
        public long position;
        public long positionEnd;
        public MappedByteBuffer buffer;

        public MmapBlock(int index, long position, long positionEnd, FileChannel ch, boolean readOnly) throws IOException {
            this.index = index;
            this.position = position;
            this.positionEnd = positionEnd;
            this.buffer = ch.map(readOnly ?
                    FileChannel.MapMode.READ_ONLY :
                    FileChannel.MapMode.READ_WRITE, position, positionEnd - position);
        }

        public boolean in(long pos) {
            return position <= pos && pos < (positionEnd);
        }

        /**
         * @param pos  checked position
         * @return 1: out-of-bounds and before, 0:in-bounds, -1:out-of-bounds and after
         */
        public int comparePosition(long pos) {
            if (pos < position) {
                return 1;
            } else if (pos < positionEnd) {
                return 0;
            } else {
                return -1;
            }
        }

        public MmapBlock position(long pos) {
            buffer.position((int) (pos - this.position));
            return this;
        }

        public long position() {
            return this.position + buffer.position();
        }

        public long end() {
            return positionEnd;
        }

        @Override
        public String toString() {
            return "MmapBlock{" +
                    "index=" + index +
                    ", position=" + position +
                    ", positionEnd=" + positionEnd +
                    ", buffer=" + buffer +
                    '}';
        }
    }

    public static class MmapStateWrite extends MmapState {
        public MmapStateWrite(Path file) throws IOException {
            super(FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE), new ExponentialBufferIndexer(20, 30));
        }
        public MmapStateWrite(Path file, BufferIndexer indexer) throws IOException {
            super(FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE), indexer);
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }
        @Override
        public void close() throws IOException {
            long size = blocks.get(blocks.size() - 1).position();
            channel.truncate(size);
            for (MmapBlock b : blocks) {
                b.buffer.force();
            }
            super.close();
        }
    }

    public static class MmapStateOutputStream extends OutputStream {
        protected MmapStateWrite state;

        public MmapStateOutputStream(MmapStateWrite state) {
            this.state = state;
        }

        public void seek(long pos) {
            state.seek(pos);
        }

        public long position() {
            return state.current.position();
        }

        @Override
        public void write(int b) throws IOException {
            if (!state.current.buffer.hasRemaining()) {
                state.nextBlock();
            }
            state.current.buffer.put((byte) b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            while (len > 0) {
                MappedByteBuffer buffer = state.current.buffer;
                int rem = buffer.remaining();
                int wLen = Math.min(len, rem);
                buffer.put(b, off, wLen);
                len -= wLen;
                off += wLen;
                if (len > 0) {
                    state.nextBlock();
                } else {
                    break;
                }
            }
        }

        @Override
        public void close() throws IOException {
            state.close();
        }
    }

    public static class MmapStateInputStream extends InputStream {
        protected MmapStateRead state;

        public MmapStateInputStream(MmapStateRead state) {
            this.state = state;
        }

        public MmapStateRead getState() {
            return state;
        }

        @Override
        public int read() throws IOException {
            return state.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return state.read(b, off, len);
        }

        @Override
        public int available() {
            return (int) Math.min(Integer.MAX_VALUE, state.available());
        }

        @Override
        public long skip(long n) throws IOException {
            return state.skip(n);
        }

        @Override
        public void close() throws IOException {
            state.close();
        }
    }

    public static class MmapStateRead extends MmapState {

        public MmapStateRead(Path file) throws IOException {
            this(FileChannel.open(file, StandardOpenOption.READ), Files.size(file));
        }

        public MmapStateRead(FileChannel channel, long length) throws IOException {
            this(channel, new BlockBufferIndexer(1000_000_000, length));
        }

        public MmapStateRead(FileChannel channel, BufferIndexer indexer) throws IOException {
            super(channel, indexer);
            initBlockRead();
        }

        @Override
        protected void initBlock() throws IOException {}

        protected void initBlockRead() throws IOException { //after length is set
            super.initBlock();
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }

        public MmapStateRead copy() throws IOException {
            return new MmapStateRead(channel, indexer);
        }

        public long available() {
            return Math.max(0, indexer.getLength() - current.position);
        }

        public int read() throws IOException {
            int rem = current.buffer.remaining();
            if (rem == 0) {
                if (current.position() < indexer.getLength()) {
                    nextBlock();
                } else {
                    return -1;
                }
            }
            byte b = current.buffer.get();
            if (!current.buffer.hasRemaining() && current.position() < indexer.getLength()) {
                nextBlock();
            }
            return b;
        }

        public long skip(long n) throws IOException {
            long pre = current.position();
            seek(pre + n);
            return pre - current.position();
        }

        public int read(byte[] buffer, int offset, int count) throws IOException {
            int remain = count;
            remain = Math.max(0, Math.min(buffer.length - offset, remain));
            int read = 0;
            while (remain > 0) {
                ByteBuffer byteBuffer = current.buffer;
                int rn = Math.min(remain, byteBuffer.remaining());
                byteBuffer.get(buffer, offset + read, rn);
                remain -= rn;
                read += rn;
                if (!byteBuffer.hasRemaining()) {
                    if (current.position() < indexer.getLength()) {
                        nextBlock();
                    } else{
                        break;
                    }
                }
            }
            return read;
        }
    }

    public interface BufferIndexer {
        int getBufferIndexFromDataIndex(long dataIndex);
        /** inclusive */
        long getMinDataIndexForBufferIndex(int bufferIndex);
        /** exclusive */
        long getMaxDataIndexForBufferIndex(int bufferIndex);
        default long getLength() {
            return Long.MAX_VALUE;
        }
    }

    public static final ExponentialBufferIndexer EXP_INDEXER = new ExponentialBufferIndexer();

    /**
     * <pre>
     *   0: [0    ... +2^16]
     *   1: [2^16 ... +2^16]
     *   2: [2^17 ...        +2^17]
     *   3: [2^18 ...              +2^18]
     *  ...
     *  15: [2^30 ...                   +2^30]
     *  ...:[2^31 ...                   +2^30]
     *  ...
     *  ...:[     ...                   +2^30]
     * </pre>
     */
    public static class ExponentialBufferIndexer implements BufferIndexer {
        protected final int minShift;
        protected final int maxShift;
        protected final int diffShift;
        protected final long minBufferDataSize;
        protected final long maxBufferDataSize;
        protected final long maxBufferDataSizeX2;

        protected final int minZeros;

        public ExponentialBufferIndexer() {
            this(16, 30);
        }

        public ExponentialBufferIndexer(int minShift, int maxShift) {
            this.minShift = minShift;
            this.maxShift = maxShift;
            diffShift = maxShift - minShift;
            minBufferDataSize = 1L << minShift;
            maxBufferDataSize = 1L << maxShift;
            maxBufferDataSizeX2 = maxBufferDataSize * 2L;
            minZeros = Long.numberOfLeadingZeros(minBufferDataSize);
        }

        @Override
        public int getBufferIndexFromDataIndex(long dataIndex) {
            if (dataIndex < minBufferDataSize) {
                return 0;
            } else if (dataIndex >= maxBufferDataSizeX2) {
                //return (int) (dataIndex / maxBufferDataSize) + diffShift;
                return (int) (dataIndex >>> maxShift) + diffShift;
            } else {
                /*
                long mask = maxBufferDataSize;
                int i = diffShift + 1;
                while ((mask & dataIndex) != mask) {
                    mask >>= 1;
                    i--;
                }
                return i;
                */
                return minZeros - Long.numberOfLeadingZeros(dataIndex) + 1;
            }
        }

        @Override
        public long getMinDataIndexForBufferIndex(int bufferIndex) {
            if (bufferIndex == 0) {
                return 0;
            } else if (bufferIndex <= diffShift) {
                return ~(0xFFFF_FFFF_FFFF_FFFFL << (bufferIndex + minShift - 1)) + 1;
            } else {
                //return (long) (bufferIndex - (diffShift)) * maxBufferDataSize;
                return (long) (bufferIndex - diffShift) << maxShift;
            }
        }

        @Override
        public long getMaxDataIndexForBufferIndex(int bufferIndex) {
            if (bufferIndex == 0) {
                return minBufferDataSize;
            } else if (bufferIndex <= diffShift) {
                return ~(0xFFFF_FFFF_FFFF_FFFFL << (bufferIndex + minShift)) + 1;
            } else {
                //return (long) (bufferIndex - (diffShift) + 1) * maxBufferDataSize;
                return (long) (bufferIndex - diffShift + 1) << maxShift;
            }
        }

        @Override
        public String toString() {
            return String.format("Exp(min=%,d,max=%,d)", minBufferDataSize, maxBufferDataSize);
        }
    }

    public static final BlockBufferIndexer BLOCK_INDEXER = new BlockBufferIndexer(1_000_000_000L);


    public static class BlockBufferIndexer implements BufferIndexer {
        protected long blockSize;
        protected long length;

        public BlockBufferIndexer(long blockSize) {
            this(blockSize, Long.MAX_VALUE);
        }

        public BlockBufferIndexer(long blockSize, long length) {
            this.blockSize = blockSize;
            this.length = length;
        }

        public long getBlockSize() {
            return blockSize;
        }
        public long getLength() {
            return length;
        }

        @Override
        public int getBufferIndexFromDataIndex(long dataIndex) {
            return (int) (dataIndex / blockSize);
        }

        @Override
        public long getMinDataIndexForBufferIndex(int bufferIndex) {
            return ((long) bufferIndex) * blockSize;
        }

        @Override
        public long getMaxDataIndexForBufferIndex(int bufferIndex) {
            return Math.min(length, ((long) (bufferIndex + 1)) * blockSize);
        }

        @Override
        public String toString() {
            return String.format("Block(blockSize=%,d, len=%,d)", blockSize, length);
        }
    }

}
