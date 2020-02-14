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

        boolean found = false;

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
            startPos = split.splitStart;
        }
        f.seek(startPos);
        return f;
    }

    public Iterator<String> openLineIterator(FileSplit split) throws IOException {
        RandomAccessFile file = open(split);
        return new Iterator<String>() {
            RandomAccessFile reader = file;
            ByteBuffer lineBuffer = ByteBuffer.allocate(4096);

            {
                lineBuffer.limit(0);
            }

            @Override
            public boolean hasNext() {
                try {
                    if (lineBuffer.hasRemaining()) {
                        return true;
                    } else if (reader != null) {
                        if (reader.getFilePointer() < split.splitEnd()) {
                            return true;
                        } else {
                            reader.close();
                            reader = null;
                            return false;
                        }
                    } else {
                        return false;
                    }
                } catch (IOException e) {
                    report(e);
                    return false;
                }
            }

            @Override
            public String next() {
                try {
                    if (lineBuffer.hasRemaining()) {
                        byte[] array = lineBuffer.array();
                        boolean newLine = false;
                        int bufferLimit = lineBuffer.limit();
                        for (int i = lineBuffer.position(); i < bufferLimit; ++i) {
                            if (array[i] == '\n') {
                                lineBuffer.limit(i);
                                newLine = true;
                                break;
                            }
                        }
                        String line = StandardCharsets.UTF_8.decode(lineBuffer).toString();
                        lineBuffer.limit(bufferLimit);
                        if (newLine) {
                            lineBuffer.get(); //discard new line
                        }
                        return line;
                    } else {
                        lineBuffer.clear();
                        int bufferLimit = lineBuffer.capacity();
                        boolean found = false;
                        while (!found) {
                            int len = reader.read(lineBuffer.array(), lineBuffer.position(), lineBuffer.remaining());
                            if (len < 0) {
                                break;
                            }
                            bufferLimit = lineBuffer.position() + len;
                            for (int i = 0; i < len; ++i) {
                                byte b = lineBuffer.get();
                                if (b == '\n') {
                                    lineBuffer.flip();
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                if (lineBuffer.capacity() * 2L > Integer.MAX_VALUE) {
                                    lineBuffer.flip();
                                    break;
                                }
                                ByteBuffer newBuffer = ByteBuffer.allocate(lineBuffer.capacity() * 2);
                                lineBuffer.flip();
                                newBuffer.put(lineBuffer);
                                lineBuffer = newBuffer;
                            }
                        }
                        String line = StandardCharsets.UTF_8.decode(lineBuffer).toString();
                        lineBuffer.limit(bufferLimit);
                        return line;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public void report(IOException e) {
        System.err.println(e);
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
}
