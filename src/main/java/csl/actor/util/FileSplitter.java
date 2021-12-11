package csl.actor.util;

import csl.actor.Message;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class FileSplitter {
    protected long splitLength;
    protected long splits;

    protected PathModifier pathModifier;

    public static FileSplitter getWithSplitLength(long splitLength) {
        return new FileSplitter(splitLength, 0);
    }

    public static FileSplitter getWithSplitLength(long splitLength, PathModifier pm) {
        return new FileSplitter(splitLength, 0, pm);
    }

    public static FileSplitter getWithSplitCount(long splits) {
        return new FileSplitter(10_000_000L, splits);
    }

    public static FileSplitter getWithSplitCount(long splits, PathModifier pm) {
        return new FileSplitter(10_000_000L, splits, pm);
    }

    public FileSplitter(long splitLength, long splits) {
        this(splitLength, splits, Paths::get);
    }

    public FileSplitter(long splitLength, long splits, PathModifier pathModifier) {
        this.splitLength = splitLength;
        this.splits = splits;
        this.pathModifier = pathModifier;
    }

    public long getSplitLength() {
        return splitLength;
    }

    public long getSplits() {
        return splits;
    }

    public PathModifier getPathModifier() {
        return pathModifier;
    }

    public List<FileSplit> split(String path) throws IOException {
        ArrayList<FileSplit> sps = new ArrayList<>();
        splitIterator(path).forEachRemaining(sps::add);
        sps.trimToSize();
        return sps;
    }

    public FileSplit getTopSplit(String path) throws IOException {
        long len = Files.size(pathModifier.getExpanded(path));
        return new FileSplit(path,
                len,
                0,
                0,getSplitLength(path, len));
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

    public Iterator<String> openLineIterator(FileSplit split) throws IOException {
        return new FileSplitLineIterator(new FileSplitReader(split, pathModifier));
    }

    public static class FileSplit implements Serializable, Message.MessageData {
        public static final long serialVersionUID = 1L;
        public String path;
        public long fileLength;
        public long splitIndex;
        public long splitStart;
        public long splitLength;

        public FileSplit() {
            this(null);
        }

        public FileSplit(String path) {
            this(path, 0, 0, 0, 0);
        }

        public FileSplit(String path, long fileLength, long splitIndex, long splitStart, long splitLength) {
            this.path = path;
            this.fileLength = fileLength;
            this.splitIndex = splitIndex;
            this.splitStart = splitStart;
            this.splitLength = splitLength;
        }

        public long getSplitStart() {
            return splitStart;
        }

        public long getSplitIndex() {
            return splitIndex;
        }

        public long getSplitLength() {
            return splitLength;
        }

        public long getFileLength() {
            return fileLength;
        }

        public String getPath() {
            return path;
        }

        public long splitEnd() {
            return splitStart + splitLength;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FileSplit fileSplit = (FileSplit) o;
            return splitStart == fileSplit.splitStart &&
                    splitLength == fileSplit.splitLength &&
                    Objects.equals(path, fileSplit.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, splitStart, splitLength);
        }

        @Override
        public String toString() {
            return String.format("(%s,%,d: [%,d] %,d,+%,d)", path, fileLength, splitIndex, splitStart, splitLength);
        }
        public String toStringShort() {
            String p = path;
            int n = (p == null ? -1 : p.lastIndexOf('/'));
            if (n != -1) {
                p = p.substring(n + 1);
            }
            return String.format("%s[%,d]", p, splitIndex);
        }
    }

    public static class FileSplitLineIterator implements Iterator<String> {
        protected FileSplit split;
        protected FileSplitReader reader;
        protected ByteBuffer next;

        public FileSplitLineIterator(FileSplitReader reader) {
            split = (reader == null ? null : reader.getSplit());
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
                throw new RuntimeException("reader=" + reader, ex);
            }
        }

        @Override
        public String toString() {
            return "{reader=" + reader + "}";
        }

        public String toStringProgress() {
            FileSplitReader r = reader;
            if (r != null) {
                return r.toStringProgress();
            } else {
                return "reader=null";
            }
        }

        public double getPositionRate() {
            return reader == null ? 1.0 : reader.getPositionRate();
        }

        public String toStringShort() {
            String splitStr = "?";
            if (split != null) {
                splitStr = split.toStringShort();
            }
            FileSplitReader r = reader;
            if (r != null) {
                return String.format("splitReader(%s, %2.2f%%)", splitStr, r.getPositionRate() * 100.0);
            } else {
                return String.format("splitReader(%s, 100%%)", splitStr);
            }
        }
    }

    public static class FileSplitReader {
        protected FileSplit split;
        //protected RandomAccessFile file;
        protected MmapState.MmapStateRead file;
        protected ByteBuffer buffer;
        protected boolean over;
        protected int bufferLineStart;
        protected int newLinesBeforeLineStart;
        protected long filePosition;
        protected Path actualPath;
        protected PathModifier pathModifier;

        public FileSplitReader(FileSplit split, PathModifier pathModifier) throws IOException {
            this.split = split;
            this.pathModifier = pathModifier;
            open();
        }

        public FileSplit getSplit() {
            return split;
        }

        public double getPositionRate() {
            return (filePosition - split.splitStart) / (double) split.splitLength;
        }

        public String toStringProgress() {
            return String.format("pos=%,d (%2.2f%%)", filePosition, getPositionRate() * 100.0);
        }

        @Override
        public String toString() {
            return String.format("%s(over=%s, bLs=%,d, nlLs=%,d, filePos=%,d, path=%s, buf=%s)",
                    getClass().getSimpleName(), over, bufferLineStart, newLinesBeforeLineStart,
                    filePosition, actualPath, buffer);
        }

        public void open() throws IOException {
            if (file != null) {
                close();
            }
            actualPath = pathModifier.getExpanded(split.path);
            //RandomAccessFile f = new RandomAccessFile(actualPath.toFile(), "r");
            MmapState.MmapStateRead f = new MmapState.MmapStateRead(actualPath);
            if (buffer == null) {
                buffer = ByteBuffer.allocate(4096);
            } else {
                buffer.clear();
            }
            buffer.limit(0);
            this.file = f;
            f.seek(Math.max(split.splitStart - 2L, 0)); //-2 for readPreviousEnd
            filePosition = split.splitStart;
            bufferLineStart = 0;
            if (split.splitStart > 0) {
                if (readPreviousEndNewLine() == 0) { //otherwise, the previous split ends with the new line
                    ByteBuffer last = readLine();
                    int ns = getNewLinesBeforeLineStart();
                    while (ns == 0 && last != null) {
                        last = readLine();
                        ns = getNewLinesBeforeLineStart();
                    }
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

        public int readPreviousEndNewLine() throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(2);
            int len = file.read(buffer.array(), 0, split.splitStart >= 2 ? 2 : 1);
            if (len <= 0) {
                return 0;
            } else {
                buffer.position(0);
                buffer.limit(len);
                int n = bufferTopNewLine(buffer);
                if (n >= 2) { //\n\r
                    return n;
                } else {
                    buffer.get();
                    if (buffer.hasRemaining()) {
                        n = bufferTopNewLine(buffer); //\n
                        return n;
                    } else {
                        return 0;
                    }
                }
            }
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
                    ns = bufferTopNewLine(buffer);
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

        public int bufferTopNewLine(ByteBuffer buffer) {
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
