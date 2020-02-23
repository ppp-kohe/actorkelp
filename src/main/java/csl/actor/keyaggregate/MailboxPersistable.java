package csl.actor.keyaggregate;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Pool;
import csl.actor.*;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class MailboxPersistable extends MailboxDefault implements Mailbox, Cloneable {
    protected AtomicLong size;
    protected volatile long previousSize;
    protected ReentrantLock persistLock;

    protected PersistentConditionMailbox condition;
    protected long onMemorySize;

    protected volatile MessagePersistent persistent;

    public interface MessagePersistent {
        MessagePersistentWriter get();
        KryoBuilder.SerializerFunction getSerializer();
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

    @FunctionalInterface
    public interface PersistentConditionMailbox {
        boolean needToPersist(MailboxPersistable mailbox, long size);

        default boolean needToPersistInPersistLock(MailboxPersistable mailbox, long size) {
            return needToPersist(mailbox, size);
        }

        default boolean needToPersistInOffer(MailboxPersistable mailbox, long size) {
            return needToPersist(mailbox, size);
        }

        default boolean needToPersistInPoll(MailboxPersistable mailbox, long size) {
            return needToPersist(mailbox, size);
        }
    }

    public MailboxPersistable(PersistentFileManager manager, long sizeLimit, long onMemorySize) {
        this(new MessagePersistentFile(manager), sizeLimit, onMemorySize);
    }

    public MailboxPersistable(PersistentFileManager manager, PersistentConditionMailbox condition, long onMemorySize) {
        this(new MessagePersistentFile(manager), condition, onMemorySize);
    }

    public MailboxPersistable(MessagePersistent persistent, long sizeLimit, long onMemorySize) {
        this(persistent, new PersistentConditionMailboxSizeLimit(sizeLimit), onMemorySize);
    }

    public MailboxPersistable(MessagePersistent persistent, PersistentConditionMailbox condition, long onMemorySize) {
        this.condition = condition;
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

    public long getOnMemorySize() {
        return onMemorySize;
    }

    /** @return implementation field getter */
    public PersistentConditionMailbox getCondition() {
        return condition;
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
        queue.offer(message);
        if (condition.needToPersistInOffer(this, s)) {
            persist();
        }
    }

    public void persist() {
        if (persistLock.tryLock()) {
            try {
                persistLocked();
            } finally {
                persistLock.unlock();
            }
        }
    }

    protected void persistLocked() {
        long s = size.get();
        if (condition.needToPersistInPersistLock(this, s)) {

            ConcurrentLinkedQueue<Message<?>> oldQueue = queue;
            ConcurrentLinkedQueue<Message<?>> newQueue = new ConcurrentLinkedQueue<>();
            long end = onMemorySize;

            long offered = 0;

            boolean hasOnMemory = false;
            for (int i = 0; i < end; ++i) {
                Message<?> m = oldQueue.poll();
                if (m == null) {
                    break;
                }
                hasOnMemory = true;
                newQueue.offer(m);
            }
            boolean top = true;
            long polled = 0;
            MessageOnStorage reader;
            try (MessagePersistentWriter ms = persistent.get()) {
                reader = ms.reader();
                if (reader != null) {
                    newQueue.offer(reader);
                    offered++;
                }
                queue = newQueue;
                //from here any other threads cannot touch the oldQueue
                while (true) {
                    Message<?> m = oldQueue.peek();
                    if (m == null) {
                        break;
                    }
                    if (top && !hasOnMemory && m instanceof MessageOnStorage) { //top item might be intermediate state
                        MessageOnStorage mOnS = (MessageOnStorage) m;
                        if (mOnS.isOpened()) {
                            top = !persistRemaining(ms, mOnS);
                        } else {
                            ms.save(m);
                            top = false;
                        }
                    } else {
                        ms.save(m);
                        top = false;
                    }
                    oldQueue.poll();
                    ++polled;
                }
            } catch (Exception ex) {
                ex.printStackTrace(); //TODO retry?
            }
            size.addAndGet(-polled + offered);
        }
    }

    private boolean persistRemaining(MessagePersistentWriter ms, MessageOnStorage mOnS) {
        boolean saved = false;
        while (true) {
            Message<?> m = mOnS.readNext();
            if (m == null) {
                break;
            } else {
                ms.save(m);
                saved = true;
            }
        }
        return saved;
    }

    @Override
    public Message<?> poll() {
        persistLock.lock();
        try {
            persistInPoll();
            Message<?> m = queue.peek();
            if (m instanceof MessageOnStorage) {
                return pollByReadNext((MessageOnStorage) m);
            } else if (m != null) {
                size.decrementAndGet();
                return queue.poll();
            } else {
                return null;
            }
        } finally {
            persistLock.unlock();
        }
    }

    protected void persistInPoll() {
        long n = size.get();
        if (condition.needToPersistInPoll(this, n)) {
            persist();
        }
        previousSize = n;
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
        size.decrementAndGet();
    }

    public static class PersistentConditionMailboxSizeLimit implements PersistentConditionMailbox {
        protected long sizeLimit;

        public PersistentConditionMailboxSizeLimit(long sizeLimit) {
            this.sizeLimit = sizeLimit;
        }

        @Override
        public boolean needToPersist(MailboxPersistable mailbox, long size) {
            return size > sizeLimit;
        }

        @Override
        public boolean needToPersistInOffer(MailboxPersistable mailbox, long size) {
            return size > sizeLimit && size > mailbox.getPreviousSize();
        }

        @Override
        public boolean needToPersistInPoll(MailboxPersistable mailbox, long size) {
            return size > sizeLimit && size > mailbox.getPreviousSize();
        }
    }

    public static class PersistentConditionMailboxSampling implements PersistentConditionMailbox {
        protected long sizeLimit;
        protected AtomicLong sampleTotal = new AtomicLong();
        protected AtomicLong sampleCount = new AtomicLong();
        protected AtomicInteger sampleTiming = new AtomicInteger();

        public PersistentConditionMailboxSampling(long sizeLimit) {
            this.sizeLimit = sizeLimit;
        }

        @Override
        public boolean needToPersist(MailboxPersistable mailbox, long size) {
            if (size > sizeLimit) {
                long currentSample;
                Message<?> msg;
                if (sampleTiming.getAndIncrement() % 100 == 0 &&
                        !((msg = mailbox.getQueue().peek()) instanceof MessageOnStorage)) {
                    currentSample = updateCurrentSample(mailbox, msg);
                } else {
                    currentSample = currentSample();
                }
                long totalSize = size * currentSample;
                Runtime rt = Runtime.getRuntime();
                long available = rt.maxMemory() - rt.totalMemory();
                return totalSize + sizeLimit * currentSample > available;
            }
            return false;
        }

        public long updateCurrentSample(MailboxPersistable mailbox, Message<?> msg) {
            long sampleSize;
            try (Output output = new Output(4096)) { //a lengthy message causes a buffer overflow error
                mailbox.getPersistent().getSerializer().write(output, msg);
                output.flush();
                sampleSize = output.total();
                sampleTiming.set(0);
                return sampleTotal.addAndGet(sampleSize) / sampleCount.incrementAndGet();
            } catch (Exception ex) {
                if (ex instanceof KryoException && ex.getMessage().contains("overflow")) {
                    return sampleTotal.addAndGet(4096) / sampleCount.incrementAndGet();
                } else {
                    //serialization failure
                    return currentSample();
                }
            }
        }

        public long currentSample() {
            long count = sampleCount.get();
            if (count == 0) {
                return 100;
            } else {
                return sampleTotal.get() / sampleCount.get();
            }
        }
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
            return String.format("source(path=%s,offset=%,d)", path, offset);
        }
    }

    public static class PersistentFileReader implements AutoCloseable {
        protected Path path;
        protected PersistentFileManager manager;
        protected KryoBuilder.SerializerFunction serializer;
        protected Input input;
        protected long offset;
        protected long position;
        protected InputStream inputStream;
        protected int bufferSize = 4096;

        public PersistentFileReader(Path path, long offset, PersistentFileManager manager) throws IOException {
            this.path = path;
            this.manager = manager;
            this.serializer = manager.getSerializer();
            inputStream = new FileInputStream(path.toFile()); //Files.newInputStream(path).skip(n) is slow
            this.offset = offset;
            inputStream.skip(offset);
            this.position = offset;
            input = new Input(inputStream, bufferSize);
        }

        public Object next() throws IOException {
            long prev = input.total();
            Object v = serializer.read(input);
            position += input.total() - prev;
            return v;
        }

        public long nextLong() {
            long prev = input.total();
            long v = input.readLong();
            position += input.total() - prev;
            return v;
        }

        public long position() {
            return position;
        }

        public void position(long newPosition) throws IOException {
            input.skip(newPosition - position());
            position = newPosition;
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

        @Override
        public KryoBuilder.SerializerFunction getSerializer() {
            return manager.getSerializer();
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
            return super.toStringContents() + (reader == null ? ", " + source : ", " + reader);
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
