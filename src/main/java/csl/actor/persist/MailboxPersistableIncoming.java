package csl.actor.persist;

import csl.actor.MailboxDefault;
import csl.actor.Message;
import csl.actor.remote.KryoBuilder;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * continuously persisting incoming messages when overflow
 */
public class MailboxPersistableIncoming extends MailboxDefault implements MailboxManageable {
    protected AtomicLong size;
    protected volatile long previousSizeOnMem;
    protected ReentrantLock persistLock;

    protected PersistentConditionMailbox condition;
    protected volatile PersistentFileManager.PersistentFileWriter writer;
    protected PersistentFileManager.PersistentFileReaderSource source;
    protected volatile PersistentFileManager.PersistentFileReader reader;
    protected PersistentFileManager manager;
    protected AtomicLong writeSize;

    public MailboxPersistableIncoming(PersistentFileManager manager, long onMemorySize) {
        this(manager, new PersistentConditionMailbox.PersistentConditionMailboxSizeLimit(onMemorySize, manager.getLogger()));
    }

    public MailboxPersistableIncoming(PersistentFileManager manager, PersistentConditionMailbox condition) {
        this.manager = manager;
        this.condition = condition;
        init();
    }

    @Override
    public MailboxPersistableIncoming create() {
        try {
            MailboxPersistableIncoming m = (MailboxPersistableIncoming) super.clone();
            m.init();
            return m;
        } catch (CloneNotSupportedException ce) {
            throw new RuntimeException(ce);
        }
    }

    protected void init() {
        queue = new ConcurrentLinkedQueue<>();
        size = new AtomicLong();
        writeSize = new AtomicLong();
        previousSizeOnMem = 0;
        persistLock = new ReentrantLock();
        writer = null;
        reader = null;
    }

    @Override
    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Override
    public long getSize() {
        return size.get(); //might be incorrect with non-atomic op
    }

    @Override
    public long getSizeOnMemory() {
        return size.get() - writeSize.get();
    }

    @Override
    public void offer(Message<?> message) {
        int dataSize = MailboxManageable.messageSize(message);
        long s = size.addAndGet(dataSize);
        long w = writeSize.get();
        boolean writing = w > 0;  //satisfy the condition, or already started writing
        if (condition.needToPersistInInComing(this, s - w, writing) || writing) {
            offerWrite(message, s, dataSize);
        } else {
            previousSizeOnMem = s - w;
            queue.offer(message);
        }
    }

    private void offerWrite(Message<?> message, long totalSize, int dataSize) {
        persistLock.lock();
        try {
            if (writer == null) {
                writer = manager.createWriterForHead("mbox");
                source = writer.createReaderSourceFromCurrentPosition();
            }
            writer.write(message);
            previousSizeOnMem = totalSize - writeSize.addAndGet(dataSize);
        } finally {
            persistLock.unlock();
        }
    }

    public static long storageClearSize = 1_000_000_000L;

    @Override
    public Message<?> poll() {
        persistLock.lock();
        try {
            Message<?> m = queue.peek();
            long writeRemaining = writeSize.get();
            if (m != null) {
                previousSizeOnMem = size.addAndGet(-MailboxManageable.messageSize(m)) - writeRemaining;
                return queue.poll();
            } else if (writeRemaining > 0) {
                try {
                    if (reader == null) {
                        reader = source.createReader();
                    }
                    long prevPos = reader.position();
                    if (writeRemaining < 100L) { //relating to serializer buffer
                        writer.flush();
                    }
                    Message<?> msg;
                    try {
                        msg = (Message<?>) reader.next();
                    } catch (Exception ex) {
                        writer.flush();
                        reader.close();
                        source = source.newSource(prevPos);
                        reader = source.createReader();
                        msg = (Message<?>) reader.next();
                    }
                    int dataSize = MailboxManageable.messageSize(msg);
                    long remain = writeSize.addAndGet(-dataSize);
                    previousSizeOnMem = size.addAndGet(-dataSize) - remain;
                    if (remain <= 0) {
                        long pos = reader.position();
                        reader.close();
                        reader = null;
                        if (pos >= storageClearSize) {//1G
                            if (writer != null) {
                                writer.flush();
                                writer.close();
                                writer = null;
                            }
                            source.delete();
                            source = null;
                        } else {
                            source = source.newSource(pos);
                        }

                    }
                    return msg;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                return null;
            }
        } finally {
            persistLock.unlock();
        }
    }

    public void delete() {
        persistLock.lock();
        try {
            if (writeSize.get() <= 0) {
                if (writer != null) {
                    writer.close();
                    writer = null;
                }
                if (reader != null) {
                    reader.close();
                    reader = null;
                }
                if (source != null) {
                    source.delete();
                    source = null;
                }
            }
        } finally {
            persistLock.unlock();
        }
    }

    @Override
    public KryoBuilder.SerializerFunction getSerializer() {
        return manager.getSerializer();
    }

    @Override
    public long getPreviousSizeOnMemory() {
        return previousSizeOnMem;
    }

    public PersistentConditionMailbox getCondition() {
        return condition;
    }
}
