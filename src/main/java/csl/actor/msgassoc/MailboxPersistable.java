package csl.actor.msgassoc;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Pool;
import csl.actor.ActorSystem;
import csl.actor.Mailbox;
import csl.actor.MailboxDefault;
import csl.actor.Message;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class MailboxPersistable extends MailboxDefault implements Mailbox, Cloneable {
    protected AtomicLong size;
    protected volatile long previousSize;
    protected ReentrantLock persistLock;

    protected long sizeLimit;
    protected long onMemorySize;

    protected volatile MessagePersistent persistent;

    public interface MessagePersistent {
        MessagePersistentWriter get();
    }

    public interface MessagePersistentWriter extends AutoCloseable {
        void save(Message<?> msg);
        MessageOnStorage reader();
    }

    public static abstract class MessageOnStorage extends Message<Object> {
        public MessageOnStorage() {
            super(null, null, null);
        }

        public abstract boolean isOpened();
        public abstract Message<?> readNext();

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + toStringContents() + ")";
        }

        public String toStringContents() {
            return "open=" + isOpened();
        }
    }

    public MailboxPersistable(PersistentFileManager manager, long sizeLimit, long onMemorySize) {
        this(new MessagePersistentFile(manager), sizeLimit, onMemorySize);
    }

    public MailboxPersistable(MessagePersistent persistent, long sizeLimit, long onMemorySize) {
        this.sizeLimit = sizeLimit;
        this.onMemorySize = onMemorySize;
        this.persistent = persistent;
        init();
    }

    public MessagePersistent getPersistent() {
        return persistent;
    }

    public ConcurrentLinkedQueue<Message<?>> getQueue() {
        return queue;
    }

    /** @return implementation field getter */
    public AtomicLong getSize() {
        return size;
    }

    /** @return implementation field getter */
    public long getPreviousSize() {
        return previousSize;
    }


    @Override
    public MailboxPersistable create() {
        try {
            MailboxPersistable p = (MailboxPersistable) super.clone();
            p.init();
            return p;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    protected void init() {
        queue = new ConcurrentLinkedQueue<>();
        size = new AtomicLong();
        previousSize = 0;
        persistLock = new ReentrantLock();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public void offer(Message<?> message) {
        long s = size.incrementAndGet();
        previousSize = s;
        if (s > sizeLimit) {
            persistLock.lock(); //delay offering
            queue.offer(message);
            persistLock.unlock();
        } else {
            queue.offer(message);
        }
    }

    public void persist() {
        persistLock.lock();
        try {
            persist();
        } finally {
            persistLock.unlock();
        }
    }

    protected void persistLocked() {
        long s = size.get();
        if (s > sizeLimit) {
            ConcurrentLinkedQueue<Message<?>> oldQueue = queue;
            ConcurrentLinkedQueue<Message<?>> newQueue = new ConcurrentLinkedQueue<>();
            long end = onMemorySize;

            long newQueueSize = 0;

            for (int i = 0; i < end; ++i) {
                Message<?> m = oldQueue.poll();
                if (m == null) {
                    break;
                }
                newQueue.offer(m);
                newQueueSize++;
            }
            long saved = 0;
            MessageOnStorage reader = null;
            try (MessagePersistentWriter ms = persistent.get()) {
                reader = ms.reader();
                while (true) {
                    Message<?> m = oldQueue.peek();
                    if (m == null) {
                        break;
                    }
                    if (saved == 0 && newQueueSize == 0 && m instanceof MessageOnStorage) { //top item might be intermediate state
                        MessageOnStorage mOnS = (MessageOnStorage) m;
                        if (mOnS.isOpened()) {
                            saved += persistRemaining(ms, mOnS);
                        } else {
                            ms.save(m);
                            ++saved;
                        }
                    } else {
                        ms.save(m);
                        ++saved;
                    }
                    oldQueue.poll();
                }
            } catch (Exception ex) {
                ex.printStackTrace(); //TODO retry?
            }
            if (reader != null && saved > 0) {
                newQueue.offer(reader);
                newQueueSize++;
            }
            this.queue = newQueue;
            size.addAndGet(-s + newQueueSize);
        }
    }

    private long persistRemaining(MessagePersistentWriter ms, MessageOnStorage mOnS) {
        long saved = 0;
        while (true) {
            Message<?> m = mOnS.readNext();
            if (m == null) {
                break;
            } else {
                ms.save(m);
                ++saved;
            }
        }
        return saved;
    }

    @Override
    public Message<?> poll() {
        Message<?> m = peekForPoll();
        if (m instanceof MessageOnStorage) {
            return pollByReadNext((MessageOnStorage) m);
        } else {
            return queue.poll();
        }
    }

    protected Message<?> peekForPoll() {
        Message<?> m = queue.peek();
        if (m != null) {
            long n = size.decrementAndGet();
            if (n > sizeLimit && previousSize <= n) {
                persistLock.lock();
                try {
                    n = size.get();
                    persistLocked();
                    m = queue.peek();
                } finally {
                    persistLock.unlock();
                }
            }
            previousSize = n;
        }
        return m;
    }

    protected Message<?> pollByReadNext(MessageOnStorage mOnS) {
        Message<?> next = mOnS.readNext();
        if (next == null) {
            pollClose(mOnS);
            return poll();
        } else {
            return next;
        }
    }

    protected void pollClose(MessageOnStorage mOnS) {
        queue.poll();
    }

    ////// PersistentFile

    public static class PersistentFileManager {
        protected String path;
        protected long fileCount;

        protected KryoBuilder.SerializerFunction serializer;

        public PersistentFileManager(String path, KryoBuilder.SerializerFunction serializer) {
            this.path = path;
            this.serializer = serializer;
        }

        public synchronized PersistentFileWriter createWriter(String head) {
            try {
                return new PersistentFileWriter(createPath(head), this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public synchronized Path createPath(String head) {
            long c = fileCount;
            ++fileCount;
            Path p = Paths.get(path, String.format("%s-%05d", head, c));
            while (Files.exists(p)) {
                p = Paths.get(path, String.format("%s-%05d", head, c));
                ++fileCount;
                c = fileCount;
            }
            return p;
        }

        public KryoBuilder.SerializerFunction getSerializer() {
            return serializer;
        }
    }

    public static class PersistentFileEnd implements Serializable {}

    public static class PersistentFileWriter implements AutoCloseable {
        protected Path path;
        protected PersistentFileManager manager;
        protected Output output;
        protected KryoBuilder.SerializerFunction serializer;

        public PersistentFileWriter(Path path, PersistentFileManager manager) throws IOException  {
            this.path = path;
            this.manager = manager;
            Path dir = path.getParent();
            if (dir != null && !Files.exists(dir)) {
                Files.createDirectories(dir);
            }
            output = new Output(Files.newOutputStream(path));
            serializer = manager.getSerializer();
        }

        public long position() {
            return output.total();
        }

        public void write(Object obj) {
            serializer.write(output, obj);
        }

        public PersistentFileReaderSource createReaderSourceFromCurrentPosition() {
            return new PersistentFileReaderSource(path.toString(), position(), manager);
        }

        @Override
        public void close() {
            write(new PersistentFileEnd());
            output.close();
        }
    }

    public static class PersistentFileReaderSource implements Serializable {
        protected String path;
        protected long offset;
        protected transient PersistentFileManager manager;

        public PersistentFileReaderSource(String path, long offset, PersistentFileManager manager) {
            this.path = path;
            this.offset = offset;
            this.manager = manager;
        }

        public void setManager(PersistentFileManager manager) {
            this.manager = manager;
        }

        public PersistentFileManager getManager() {
            return manager;
        }

        public PersistentFileReader createReader() {
            try {
                return new PersistentFileReader(Paths.get(path), offset, manager);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        public PersistentFileReaderSource newSource(long offset) {
            return new PersistentFileReaderSource(path, offset, manager);
        }

        @Override
        public String toString() {
            return "source(" +
                    "path=" + path +
                    ", offset=" + offset +
                    ')';
        }
    }

    public static class PersistentFileReader implements AutoCloseable {
        protected Path path;
        protected PersistentFileManager manager;
        protected KryoBuilder.SerializerFunction serializer;
        protected Input input;
        protected long offset;

        public PersistentFileReader(Path path, long offset, PersistentFileManager manager) throws IOException {
            this.path = path;
            this.manager = manager;
            this.serializer = manager.getSerializer();
            InputStream in = new FileInputStream(path.toFile()); //Files.newInputStream(path).skip(n) is slow
            this.offset = offset;
            in.skip(offset);
            input = new Input(in);
        }

        public Object next() throws IOException {
            return serializer.read(input);
        }

        public long nextLong() {
            return input.readLong();
        }

        public long position() {
            return offset + input.position();
        }

        public void position(long newPosition) {
            input.skip(newPosition - position());
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        @Override
        public String toString() {
            return String.format("reader(path=%s,offset=%,d,pos=%,d)", path, offset, position());
        }
    }

    ////// MessagePersistentFile

    public static class MessagePersistentFile implements MessagePersistent {
        protected PersistentFileManager manager;

        public MessagePersistentFile(PersistentFileManager manager) {
            this.manager = manager;
        }

        @Override
        public synchronized MessagePersistentWriter get() {
            return new MessagePersistentFileWriter(manager.createWriter("mailbox"));
        }

        public PersistentFileManager getManager() {
            return manager;
        }
    }

    public static class MessagePersistentFileWriter implements MessagePersistentWriter {
        protected PersistentFileWriter writer;

        public MessagePersistentFileWriter(PersistentFileWriter writer) {
            this.writer = writer;
        }

        @Override
        public void save(Message<?> msg) {
            try {
                writer.write(msg);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public MessageOnStorage reader() {
            return new MessageOnStorageFile(writer.createReaderSourceFromCurrentPosition());
        }

        @Override
        public synchronized void close() {
            writer.close();
        }
    }

    public static class MessageOnStorageFile extends MessageOnStorage {
        protected PersistentFileReaderSource source;
        protected transient PersistentFileReader reader;
        protected transient MessageOnStorage currentMessage;

        public MessageOnStorageFile(PersistentFileReaderSource source) {
            this.source = source;
        }

        @Override
        public boolean isOpened() {
            return reader != null || currentMessage != null;
        }

        @Override
        public synchronized Message<?> readNext() {
            try {
                if (currentMessage != null) {
                    Message<?> m = currentMessage.readNext();
                    if (m != null) {
                        return m;
                    } else {
                        currentMessage = null;
                    }
                }
                if (reader == null) {
                    reader = source.createReader();
                }
                Object o = reader.next();
                if (o instanceof PersistentFileEnd) {
                    reader.close();
                    reader = null;
                    return null;
                } else {
                    Message<?> m = (Message<?>) o;
                    if (m instanceof MessageOnStorage) {
                        if (m instanceof MessageOnStorageFile) {
                            ((MessageOnStorageFile) m).source.setManager(source.getManager());
                        }
                        currentMessage = (MessageOnStorage) m;
                        return readNext();
                    } else {
                        return m;
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public String toStringContents() {
            return super.toStringContents() + (reader == null ? "" : ", " + reader);
        }
    }

    //////////////////

    protected static final Map<ActorSystem, PersistentFileManager> systemPersistent = new WeakHashMap<>();

    public static PersistentFileManager getPersistentFile(ActorSystem system, Supplier<String> path) {
        synchronized (systemPersistent) {
            return systemPersistent.computeIfAbsent(system, s -> createPersistentFile(path.get(), s));
        }
    }

    public static PersistentFileManager createPersistentFile(String path, ActorSystem system) {
        KryoBuilder.SerializerFunction serializer;
        if (system instanceof ActorSystemRemote) {
            serializer = ((ActorSystemRemote) system).getSerializer();
        } else {
            serializer = new KryoBuilder.SerializerPool(new Pool<Kryo>(true, false) {
                @Override
                protected Kryo create() {
                    return KryoBuilder.builder().apply(system);
                }
            });
        }
        return new PersistentFileManager(path, serializer);
    }

}
