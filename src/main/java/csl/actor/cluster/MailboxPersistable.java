package csl.actor.cluster;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
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

    public static boolean logPersist = System.getProperty("csl.actor.persist.log", "true").equals("true");
    public static boolean logDebugPersist = System.getProperty("csl.actor.persist.debug", "false").equals("true");
    public static int logColorPersist = ActorSystem.systemPropertyColor("csl.actor.persist.color", 94);

    public interface MessagePersistent {
        MessagePersistentWriter get();
        KryoBuilder.SerializerFunction getSerializer();
        ActorSystem.SystemLogger getLogger();
    }

    public interface MessagePersistentWriter extends AutoCloseable {
        void save(Message<?> msg);
        MessageOnStorage reader();
        @Override
        void close();
    }

    public static abstract class MessageOnStorage extends Message<Object> {
        public static final long serialVersionUID = 1L;
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
            MessageOnStorage reader = null;
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
            } catch (Exception ex) { //retry?
                persistent.getLogger().log(true, logColorPersist, ex, "mailbox persist: polled=%,d", polled);
            }
            persistent.getLogger().log(logDebugPersist, logColorPersist, "mailbox persisted: %s sizeDelta=%,d", reader, (-polled + offered));
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
        protected SampleTiming logTimingUpdate = new SampleTiming();
        protected SampleTiming logTimingNeedToPersist = new SampleTiming();
        protected ActorSystem.SystemLogger logger;

        public PersistentConditionMailboxSampling(long sizeLimit, ActorSystem.SystemLogger logger) {
            this.sizeLimit = sizeLimit;
            this.logger = logger;
        }

        public ActorSystem.SystemLogger getLogger() {
            return logger;
        }

        public int getSampleTiming() {
            return sampleTiming.get();
        }

        @Override
        public boolean needToPersist(MailboxPersistable mailbox, long size) {
            if (size > sizeLimit) {
                long currentSample = currentSampleWithUpdating(mailbox);
                return needToPersistRuntime(size, sizeLimit, currentSample);
            }
            return false;
        }

        public long currentSampleWithUpdating(MailboxPersistable mailbox) {
            Message<?> msg;
            if (mailbox != null &&
                    sampleTiming.getAndIncrement() % 1_000 == 0 &&
                    !((msg = mailbox.getQueue().peek()) instanceof MessageOnStorage) &&
                    msg != null && msg.getData() instanceof Serializable) {
                return updateCurrentSample(mailbox, msg);
            } else {
                return currentSample();
            }
        }

        public boolean needToPersistRuntime(long size, long sizeLimit, long currentSample) {
            boolean log = logTimingNeedToPersist.next();
            long free =  runtimeAvailableBytes();
            long estimated = (size + sizeLimit) * currentSample;
            boolean res = estimated > free;
            if (log || res) {
                logger.log(logPersist, logColorPersist,
                        "Mailbox needToPersist: size=%,d sizeLimit=%,d sample=%,d estimated=%,d free=%,d (%3.1f%%) -> %s",
                        size, sizeLimit, currentSample, estimated, free, free == 0 ? Double.POSITIVE_INFINITY : ((estimated / (double) free) * 100.0), res);
            }
            return res;
        }

        public long runtimeAvailableBytes() {
            Runtime rt = Runtime.getRuntime();
            return rt.maxMemory() - rt.totalMemory();
        }

        public long updateCurrentSample(MailboxPersistable mailbox, Message<?> msg) {
            int t = sampleTiming.get();
            if (t >= 1000_000_000) {
                sampleTiming.set(0);
            }
            boolean log = logTimingUpdate.next();
            try (Output output = new Output(4096)) { //a lengthy message causes a buffer overflow error
                mailbox.getPersistent().getSerializer().write(output, msg);
                output.flush();
                long sampleSize = output.total();
                long v = sampleTotal.addAndGet(sampleSize) / sampleCount.incrementAndGet();
                if (log) {
                    logger.log(logPersist, logColorPersist, "updateCurrentSample timing=%,d lastSample=[%,d] <%s> total=%,d count=%,d -> %,d",
                            t, sampleSize, logger.toStringLimit(msg), sampleTotal.get(), sampleCount.get(), v);
                }
                return v;
            } catch (Exception ex) {
                if (log) {
                    logger.log(logPersist, logColorPersist, "updateCurrentSample timing=%,d lastSample=<%s> total=%,d count=%,d error:%s",
                            t, logger.toStringLimit(msg), sampleTotal.get(), sampleCount.get(), ex);
                }
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
                return Math.max(16, sampleTotal.get() / sampleCount.get());
            }
        }
    }

    public static class SampleTiming {
        protected AtomicInteger count = new AtomicInteger();
        protected AtomicInteger next = new AtomicInteger();

        protected int firstShift; //0 -> 2^firstShift -> 2^(firstShift+2) -> ...
        protected int maxShift;
        protected int resetShift; //0 -> ... -> 2^maxShift -> 2^resetShift

        public SampleTiming() {
            this(14, 30, 21);
        }
        public SampleTiming(int firstShift, int maxShift, int resetShift) {
            this.firstShift = Math.min(30, firstShift);
            this.maxShift = Math.min(30, maxShift);
            this.resetShift = Math.min(30, resetShift);
        }

        public boolean check() {
            return count.get() >= next.get();
        }

        public boolean next() {
            int max = 1 << maxShift;
            int t = count.getAndIncrement();
            if (t > max) {
                count.set(0);
            }

            int nt = next.get();
            if (t >= nt) {
                int next = nt <= 0 ? (1 << firstShift) : (nt << 2);
                while (next > 0 && next < Math.min(max, t)) {
                    next <<= 2;
                }
                if (next < 0 || next >= max) {
                    this.next.set(1 << resetShift);
                } else {
                    this.next.set(next);
                }
                return true;
            } else {
                return false;
            }
        }
    }

    ////// PersistentFile

    public static class PersistentFileManager {
        protected String path;
        protected long fileCount;
        protected ConfigDeployment.PathModifier pathModifier;
        protected ActorSystem.SystemLogger logger;

        protected KryoBuilder.SerializerFunction serializer;

        public PersistentFileManager(String path, KryoBuilder.SerializerFunction serializer,
                                     ConfigDeployment.PathModifier pathModifier, ActorSystem.SystemLogger logger) {
            this.path = path;
            this.serializer = serializer;
            this.pathModifier = pathModifier;
            this.logger = logger;
        }

        public ConfigDeployment.PathModifier getPathModifier() {
            return pathModifier;
        }

        public synchronized PersistentFileWriter createWriter(String head) {
            try {
                String path = createPath(head);
                logger.log(logPersist, logColorPersist, "createWriter: %s", path);
                return new PersistentFileWriter(path, this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public synchronized String createPath(String head) {
            long c = fileCount;
            ++fileCount;
            String p = Paths.get(path, String.format("%s-%05d", head, c)).toString();
            while (Files.exists(pathModifier.get(p))) {
                p = Paths.get(path, String.format("%s-%05d", head, c)).toString();
                ++fileCount;
                c = fileCount;
            }
            return pathModifier.expandPath(p);
        }

        public Path getPath(String pathExpanded) {
            Path p = pathModifier.get(pathExpanded);
            logger.log(logDebugPersist, logColorPersist, "getPath: %s -> %s", pathExpanded, p);
            Path dir = p.getParent();
            if (dir != null && !Files.exists(dir)) {
                try {
                    Files.createDirectories(dir);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            return p;
        }

        public KryoBuilder.SerializerFunction getSerializer() {
            return serializer;
        }

        public ActorSystem.SystemLogger getLogger() {
            return logger;
        }

        public void openForWrite(Path path) { }

        public void close(Path path) { }
    }

    public static class PersistentFileEnd implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    public static class PersistentFileWriter implements AutoCloseable {
        protected String pathExpanded;
        protected Path filePath;
        protected PersistentFileManager manager;
        protected Output output;
        protected KryoBuilder.SerializerFunction serializer;

        public PersistentFileWriter(String pathExpanded, PersistentFileManager manager) throws IOException  {
            this.pathExpanded = pathExpanded;
            this.manager = manager;
            Path p = manager.getPath(pathExpanded);
            manager.openForWrite(p);
            this.filePath = p;
            output = new Output(Files.newOutputStream(p));
            serializer = manager.getSerializer();
        }

        public long position() {
            return output.total();
        }

        public void write(Object obj) {
            try {
                serializer.write(output, obj);
            } catch (Exception ex) {
                throw new RuntimeException(String.format("write: path=%s obj=%s",
                        pathExpanded, manager.getLogger().toStringLimit(obj)), ex);
            }
        }

        public PersistentFileReaderSource createReaderSourceFromCurrentPosition() {
            return new PersistentFileReaderSource(pathExpanded, position(), manager);
        }

        @Override
        public void close() {
            write(new PersistentFileEnd());
            output.close();
            manager.close(filePath);
        }

        @Override
        public String toString() {
            return String.format("writer(path=%s, pos=%,d)", pathExpanded, position());
        }
    }

    public static class PersistentFileReaderSource implements Serializable {
        public static final long serialVersionUID = 1L;
        protected String pathExpanded;
        protected long offset;
        protected transient PersistentFileManager manager;

        public PersistentFileReaderSource(String pathExpanded, long offset, PersistentFileManager manager) {
            this.pathExpanded = pathExpanded;
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
                manager.getLogger().log(logDebugPersist, logColorPersist, "open: %s", this);
                return new PersistentFileReader(pathExpanded, offset, manager);
            } catch (IOException ex) {
                throw new RuntimeException("createReader: " + toString(), ex);
            }
        }

        public PersistentFileReaderSource newSource(long offset) {
            return new PersistentFileReaderSource(pathExpanded, offset, manager);
        }

        @Override
        public String toString() {
            return String.format("source(path=%s,offset=%,d)", pathExpanded, offset);
        }
    }

    public static class PersistentFileReader implements AutoCloseable {
        protected String pathExpanded;
        protected PersistentFileManager manager;
        protected KryoBuilder.SerializerFunction serializer;
        protected Input input;
        protected long offset;
        protected long position;
        protected InputStream inputStream;
        protected int bufferSize = 4096;

        public PersistentFileReader(String pathExpanded, long offset, PersistentFileManager manager) throws IOException {
            this.pathExpanded = pathExpanded;
            this.manager = manager;
            this.serializer = manager.getSerializer();
            inputStream = new FileInputStream(manager.getPath(pathExpanded).toFile()); //Files.newInputStream(path).skip(n) is slow
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
            return String.format("reader(path=%s,offset=%,d,pos=%,d)", pathExpanded, offset, position());
        }

        public PersistentFileManager getManager() {
            return manager;
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

        @Override
        public ActorSystem.SystemLogger getLogger() {
            return manager.getLogger();
        }
    }

    public static class MessagePersistentFileWriter implements MessagePersistentWriter {
        protected PersistentFileWriter writer;

        public MessagePersistentFileWriter(PersistentFileWriter writer) {
            this.writer = writer;
        }

        @Override
        public void save(Message<?> msg) {
            writer.write(msg);
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
        public static final long serialVersionUID = 1L;
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
                    reader.getManager().getLogger().log(logDebugPersist, logColorPersist, "readNext finish: %s", reader);
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
                throw new RuntimeException(String.format("readNext: %s", source), ex);
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
        if (system instanceof PersistentFileManagerFactory) {
            return ((PersistentFileManagerFactory) system).createFileManager(path);
        } else {
            KryoBuilder.SerializerFunction serializer;
            if (system instanceof ActorSystemRemote) {
                serializer = ((ActorSystemRemote) system).getSerializer();
            } else {
                serializer = new KryoBuilder.SerializerPoolDefault(system);
            }
            return new PersistentFileManager(path, serializer, ConfigDeployment.getPathModifier(system),
                    system == null ? new ActorSystemDefault.SystemLoggerErr() : system.getLogger());
        }
    }

    public interface PersistentFileManagerFactory {
        PersistentFileManager createFileManager(String path);
    }
}
