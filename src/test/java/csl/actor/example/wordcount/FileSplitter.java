package csl.actor.example.wordcount;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileSplitter {
    protected long splitLength;
    protected long splits;

    public static FileSplitter getWithSplitLength(long splitLength) {
        return new FileSplitter(splitLength, 0);
    }

    public static FileSplitter getWithSplitCount(long splits) {
        return new FileSplitter(10_000_000L, splits);
    }

    public FileSplitter(long splitLength, long splits) {
        this.splitLength = splitLength;
        this.splits = splits;
    }

    public List<FileSplit> split(String path) throws IOException {
        ArrayList<FileSplit> sps = new ArrayList<>();
        splitIterator(path).forEachRemaining(sps::add);
        sps.trimToSize();
        return sps;
    }

    public FileSplit getTopSplit(String path) throws IOException {
        FileSplit s = new FileSplit(path);
        s.fileLength = Files.size(Paths.get(s.path));
        s.splitLength = getSplitLength(path, s.fileLength);
        s.splitIndex = 0;
        return s;
    }

    public Iterator<FileSplit> splitIterator(String path) throws IOException {
        FileSplit topSplit = getTopSplit(path);
        return new Iterator<FileSplit>() {
            FileSplit current = topSplit;
            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public FileSplit next() {
                FileSplit s = current;
                long nextStart = current.splitStart + current.splitLength;

                if (nextStart < current.fileLength) {
                    current = new FileSplit(s.path, s.fileLength,
                            s.splitIndex + 1L,
                            nextStart,
                            Math.min(s.fileLength, nextStart + s.splitLength) - nextStart);
                } else {
                    current = null;
                }
                return s;
            }
        };
    }

    public long getSplitLength(String path, long length) {
        return splits == 0 ? Math.min(splitLength, length) : Math.min(length / splits, length);
    }

    public RandomAccessFile open(FileSplit split) throws IOException {
        RandomAccessFile f = new RandomAccessFile(split.path, "r");
        f.seek(split.splitStart);

        ByteBuffer buf = ByteBuffer.allocate(4096);
        long startPos = split.splitStart;
        long endPos = startPos + split.splitLength;

        boolean found = (startPos == 0);

        while (!found &&
                startPos < endPos) {
            buf.clear();
            int len = f.read(buf.array());
            if (len == 0) {
                break;
            }
            buf.position(len).flip();

            while (buf.hasRemaining()) {
                if (buf.get() == '\n') {
                    found = true;
                    break;
                }
            }
            startPos += buf.position();
        }
        if (!found) {
            startPos = endPos;
        }
        f.seek(startPos);
        return f;
    }

    public Iterator<String> openLineIterator(FileSplit split) throws IOException {
        return new FileSplitLineIterator(new FileSplitReader(split));
    }

    public static class FileSplit implements Serializable {
        String path;
        long fileLength;
        long splitIndex;
        long splitStart;
        long splitLength;

        public FileSplit(String path) {
            this.path = path;
        }

        public FileSplit(String path, long fileLength, long splitIndex, long splitStart, long splitLength) {
            this.path = path;
            this.fileLength = fileLength;
            this.splitIndex = splitIndex;
            this.splitStart = splitStart;
            this.splitLength = splitLength;
        }

        public long splitEnd() {
            return splitStart + splitLength;
        }

        @Override
        public String toString() {
            return String.format("(%s,%,d: [%,d] %,d,+%,d)", path, fileLength, splitIndex, splitStart, splitLength);
        }
    }

    public static class FileSplitLineIterator implements Iterator<String> {
        protected FileSplitReader reader;
        protected ByteBuffer next;

        public FileSplitLineIterator(FileSplitReader reader) {
            this.reader = reader;
        }

        @Override
        public boolean hasNext() {
            if (next == null && reader == null) {
                return false;
            }
            if (next == null) {
                obtain();
            }
            return next != null;
        }

        @Override
        public String next() {
            if (next == null && reader != null) {
                obtain();
            }
            String line = null;
            if (next != null) {
                line = StandardCharsets.UTF_8.decode(next).toString();
                next = null;
            }
            return line;
        }

        private void obtain() {
            try {
                if (reader.isOver()) {
                    reader.close();
                    reader = null;
                    next = null;
                } else {
                    next = reader.readLine();
                    if (next == null) {
                        next = reader.lastAfterReadLineNull();
                        reader.close();
                        reader = null;
                    }
                }
            } catch (Exception ex) {
                next = null;
            }
        }
    }

    public static class FileSplitReader {
        protected FileSplit split;
        protected RandomAccessFile file;
        protected ByteBuffer buffer;
        protected boolean over;
        protected int bufferLineStart;
        protected int newLinesBeforeLineStart;
        protected long filePosition;

        public FileSplitReader(FileSplit split) throws IOException {
            this.split = split;
            open();
        }

        public void open() throws IOException {
            if (file != null) {
                close();
            }
            RandomAccessFile f = new RandomAccessFile(split.path, "r");
            if (buffer == null) {
                buffer = ByteBuffer.allocate(4096);
            } else {
                buffer.clear();
            }
            buffer.limit(0);
            this.file = f;
            f.seek(split.splitStart);
            filePosition = split.splitStart;
            bufferLineStart = 0;
            if (split.splitStart > 0) {
                ByteBuffer last = readLine();
                int ns = getNewLinesBeforeLineStart();
                while (ns == 0 && last != null) {
                    last = readLine();
                    ns = getNewLinesBeforeLineStart();
                }
            }
        }

        public void close() throws IOException {
            file.close();
            file = null;
        }

        public boolean isOver() {
            return over;
        }

        public int getNewLinesBeforeLineStart() {
            return newLinesBeforeLineStart;
        }

        public ByteBuffer lastAfterReadLineNull() {
            ByteBuffer lineBuffer = ByteBuffer.wrap(buffer.array());
            lineBuffer.limit(buffer.position());
            lineBuffer.position(bufferLineStart);
            return lineBuffer;
        }

        public ByteBuffer readLine() throws IOException {
            while (true) {
                if (!buffer.hasRemaining()) {
                    int bufferCapacityRemain = buffer.capacity() - buffer.position();
                    int len = file.read(buffer.array(), buffer.position(), bufferCapacityRemain);
                    if (len < 0 || (len == 0 && bufferCapacityRemain > 0)) {
                        return null;
                    }
                    buffer.limit(buffer.position() + len);
                }
                int ns = 0;
                while (buffer.hasRemaining()) {
                    ns = bufferTopNewLine();
                    if (ns > 0) {
                        break;
                    } else {
                        buffer.get();
                    }
                }
                if (!buffer.hasRemaining()) {
                    if (bufferLineStart > 0) {
                        ByteBuffer reset = ByteBuffer.wrap(buffer.array());
                        buffer.position(bufferLineStart);
                        reset.put(buffer); //write to same array: move data to top
                        buffer.position(reset.position());
                        buffer.limit(buffer.capacity());
                        bufferLineStart = 0;
                    }
                    if (!buffer.hasRemaining()) {
                        long nextCapacity = buffer.capacity() * 2L;
                        if (nextCapacity > (long) Integer.MAX_VALUE) {
                            ByteBuffer lineBuffer = ByteBuffer.wrap(buffer.array());
                            lineBuffer.position(0);
                            lineBuffer.limit(buffer.limit());
                            newLinesBeforeLineStart = 0;
                            filePosition += lineBuffer.remaining();
                            return lineBuffer;
                        } else {
                            ByteBuffer newBuffer = ByteBuffer.allocate((int) nextCapacity);
                            buffer.position(0);
                            newBuffer.put(buffer);
                            newBuffer.limit(newBuffer.position());
                            buffer = newBuffer;
                        }
                    } else {
                        buffer.limit(buffer.position());
                    }
                } else { //ns > 0
                    ByteBuffer lineBuffer = ByteBuffer.wrap(buffer.array());
                    lineBuffer.position(bufferLineStart);
                    lineBuffer.limit(buffer.position());
                    buffer.position(buffer.position() + ns);
                    bufferLineStart = buffer.position();
                    newLinesBeforeLineStart = ns;
                    filePosition += lineBuffer.remaining() + ns;
                    over = (filePosition >= split.splitEnd());
                    return lineBuffer;
                }
            }
        }

        public int bufferTopNewLine() {
            if (buffer.position() < buffer.limit()) {
                byte b = buffer.get(buffer.position());
                if (b == '\n') {
                    if (buffer.position() + 1 < buffer.limit()) {
                        b = buffer.get(buffer.position() + 1);
                        if (b == '\r') {
                            return 2;
                        } else {
                            return 1;
                        }
                    } else {
                        return 1;
                    }
                } else if (b == '\r') {
                    return 1;
                } else {
                    return 0;
                }
            } else {
                return 0;
            }
        }
    }
}
