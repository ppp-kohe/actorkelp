package csl.actor.persist;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystem;
import csl.actor.MailboxDefault;
import csl.actor.Message;
import csl.actor.persist.PersistentFileManager.PersistentFileWriter;
import csl.actor.remote.KryoBuilder;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class MailboxPersistableReplacement extends MailboxDefault implements MailboxManageable, Cloneable {
    protected AtomicLong size;
    protected volatile long previousSize;
    protected ReentrantLock persistLock;

    protected PersistentConditionMailbox condition;
    protected AtomicLong sizeOnMemory;
    protected long onMemorySizeLimit;

    protected volatile PersistentFileManager persistentManager;

    public static boolean logPersist = System.getProperty("csl.actor.persist.log", "true").equals("true");
    public static boolean logDebugPersist = System.getProperty("csl.actor.persist.debug", "false").equals("true");
    public static int logColorPersist = ActorSystem.systemPropertyColor("csl.actor.persist.color", 94);

    public MailboxPersistableReplacement(PersistentFileManager persistent, long sizeLimit, long onMemorySizeLimit) {
        this(persistent, new PersistentConditionMailbox.PersistentConditionMailboxSizeLimit(sizeLimit, persistent.getLogger()), onMemorySizeLimit);
    }

    public MailboxPersistableReplacement(PersistentFileManager persistent, PersistentConditionMailbox condition, long onMemorySizeLimit) {
        this.condition = condition;
        this.persistentManager = persistent;
        this.onMemorySizeLimit = onMemorySizeLimit;
        init();
    }

    @Override
    public KryoBuilder.SerializerFunction getSerializer() {
        return persistentManager.getSerializer();
    }

    public ActorSystem.SystemLogger getLogger() {
        return condition == null ? persistentManager.getLogger() : condition.getLogger();
    }

    public PersistentFileWriter createWriter() {
        return persistentManager.createWriterForHead("mailbox");
    }

    public ConcurrentLinkedQueue<Message<?>> getQueue() {
        return queue;
    }
    @Override
    public long getSize() {
        return size.get();
    }

    @Override
    public long getSizeOnMemory() {
        return sizeOnMemory.get();
    }

    /** @return implementation field getter */
    public long getPreviousSizeOnMemory() {
        return previousSize;
    }

    /** @return implementation field getter */
    public PersistentConditionMailbox getCondition() {
        return condition;
    }

    @Override
    public MailboxPersistableReplacement create() {
        try {
            MailboxPersistableReplacement p = (MailboxPersistableReplacement) super.clone();
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
        this.sizeOnMemory = new AtomicLong();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public void offer(Message<?> message) {
        long dataSize = dataSize(message);
        size.addAndGet(dataSize);
        long sizeOnMem = sizeOnMemory.addAndGet(MailboxManageable.messageSize(message)); //MOnS +1
        previousSize = sizeOnMem;
        queue.offer(message);
        if (condition.needToPersistInOffer(this, sizeOnMem)) {
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
        if (condition.needToPersistInPersistLock(this, sizeOnMemory.get())) {

            ConcurrentLinkedQueue<Message<?>> oldQueue = queue;
            ConcurrentLinkedQueue<Message<?>> newQueue = new ConcurrentLinkedQueue<>();

            long offeredOnMem = 0;

            boolean hasOnMemory = persistLockedHasOnMemory(newQueue); //just move, do not change size
            boolean top = true;
            long polledOnMem = 0;
            MessageOnStorage reader = null;
            try (PersistentFileWriter ms = createWriter()) {
                long saved = 0;
                reader = new MessageOnStorage(ms.createReaderSourceFromCurrentPosition(), 0);
                newQueue.offer(reader);
                offeredOnMem += MailboxManageable.messageSize(reader); //the size of the MessageOnStorage itself is 1
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
                            long mSaved = persistRemaining(ms, mOnS); //just move on storage
                            top = mSaved <= 0; //no messages, still top
                            saved += mSaved;
                        } else {
                            ms.write(mOnS);
                            top = false;
                            saved += mOnS.dataSizeOnStorage();
                        }
                    } else {
                        ms.write(m);
                        top = false;
                        saved += MailboxManageable.messageSize(m);
                    }
                    polledOnMem += MailboxManageable.messageSize(oldQueue.poll()); //MOnS size is also 1
                }
                reader.setDataSizeOnStorage(saved);
            } catch (Exception ex) { //retry?
                getLogger().log(true, logColorPersist, ex, "mailbox persist: polled=%,d", polledOnMem);
            }
            if (logDebugPersist) getLogger().log(logColorPersist, "mailbox persisted: %s sizeDelta=%,d", reader, (-polledOnMem + offeredOnMem));
            sizeOnMemory.addAndGet(-polledOnMem + offeredOnMem);
        }
    }

    private boolean persistLockedHasOnMemory(ConcurrentLinkedQueue<Message<?>> newQueue) {
        ConcurrentLinkedQueue<Message<?>> oldQueue = queue;
        long end = onMemorySizeLimit;
        boolean hasOnMemory = false;
        for (int i = 0; i < end; ) {
            Message<?> m = oldQueue.poll();
            if (m == null) {
                break;
            }
            hasOnMemory = true;
            i += MailboxManageable.messageSize(m);
            newQueue.offer(m);
        }
        return hasOnMemory;
    }

    private long dataSize(Message<?> m) {
        if (m instanceof MessageOnStorage) {
            return ((MessageOnStorage) m).dataSizeOnStorage();
        } else {
            return MailboxManageable.messageSize(m);
        }
    }

    private long persistRemaining(PersistentFileWriter ms, MessageOnStorage mOnS) {
        long saved = 0;
        while (true) {
            Message<?> m = mOnS.readNext();
            if (m == null) {
                break;
            } else {
                saved += dataSize(m);
                ms.write(m);
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
                int s = -MailboxManageable.messageSize(m);
                size.addAndGet(s); //m is not MOnS
                previousSize = sizeOnMemory.addAndGet(s);
                return queue.poll();
            } else {
                return null;
            }
        } finally {
            persistLock.unlock();
        }
    }

    protected void persistInPoll() {
        long n = sizeOnMemory.get();
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
            size.addAndGet(-dataSize(next));
            return next;
        }
    }

    protected void pollClose(MessageOnStorage mOnS) {
        queue.poll();
        previousSize = sizeOnMemory.addAndGet(-MailboxManageable.messageSize(mOnS));
        //size.addAndGet(dataSize(mOnS)) is 0
    }

    public static class MessageOnStorage extends Message<Object> implements KryoSerializable {
        public static final long serialVersionUID = 1L;
        public PersistentFileManager.PersistentFileReaderSource source;
        protected transient PersistentFileManager.PersistentFileReader reader;
        protected transient MessageOnStorage currentMessage;
        protected long dataSizeOnStorage;

        public MessageOnStorage() {
            super(null, null);
        }

        public MessageOnStorage(PersistentFileManager.PersistentFileReaderSource source, long dataSizeOnStorage) {
            this();
            this.source = source;
            this.dataSizeOnStorage = dataSizeOnStorage;
        }

        public void setDataSizeOnStorage(long dataSizeOnStorage) {
            this.dataSizeOnStorage = dataSizeOnStorage;
        }

        /**
         * @return the size excluding MessageOnStorage
         */
        public long dataSizeOnStorage() {
            return dataSizeOnStorage;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + toStringContents() + ")";
        }

        public String toStringContents() {
            return "open=" + isOpened() + (reader == null ? ", " + source : ", " + reader);
        }

        public boolean isOpened() {
            return reader != null || currentMessage != null;
        }

        public synchronized Message<?> readNext() {
            try {
                if (currentMessage != null) {
                    Message<?> m = currentMessage.readNext();
                    if (m != null) {
                        dataSizeOnStorage -= MailboxManageable.messageSize(m);
                        return m;
                    } else {
                        currentMessage = null;
                    }
                }
                if (reader == null) {
                    reader = source.createReader();
                }
                Object o = reader.next();
                if (o instanceof PersistentFileManager.PersistentFileEnd) {
                    if (logDebugPersist) reader.getManager().getLogger().log(logColorPersist, "readNext finish: %s", reader);
                    reader.close();
                    reader = null;
                    return null;
                } else {
                    Message<?> m = (Message<?>) o;
                    if (m instanceof MessageOnStorage) {
                        //Note: the restored message have a manager set by serializer
                        currentMessage = (MessageOnStorage) m;
                        return readNext();
                    } else {
                        dataSizeOnStorage -= MailboxManageable.messageSize(m);
                        return m;
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(String.format("readNext: %s", source), ex);
            }
        }

        @Override
        public void write(Kryo kryo, Output output) {
            kryo.writeClassAndObject(output, this.source);
            output.writeLong(dataSizeOnStorage);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            this.source = (PersistentFileManager.PersistentFileReaderSource) kryo.readClassAndObject(input);
            this.dataSizeOnStorage = input.readLong();
        }
    }
}
