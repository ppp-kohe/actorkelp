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
    protected volatile long previousSize;
    protected ReentrantLock persistLock;

    protected PersistentConditionMailbox condition;
    protected volatile PersistentFileManager.PersistentFileWriter writer;
    protected PersistentFileManager.PersistentFileReaderSource source;
    protected volatile PersistentFileManager.PersistentFileReader reader;
    protected PersistentFileManager manager;
    protected AtomicLong writeSize;

    public MailboxPersistableIncoming(PersistentFileManager manager, long onMemorySize) {
        this(manager, new PersistentConditionMailbox.PersistentConditionMailboxSizeLimit(onMemorySize));
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
        previousSize = 0;
        persistLock = new ReentrantLock();
        writer = null;
        reader = null;
    }

    @Override
    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Override
    public void offer(Message<?> message) {
        long s = size.incrementAndGet();
        if (condition.needToPersist(this, s) || writeSize.get() > 0) {
            persistLock.lock();
            try {
                if (writer == null) {
                    writer = manager.createWriter("mbox");
                    source = writer.createReaderSourceFromCurrentPosition();
                }
                writer.write(message);
                writeSize.incrementAndGet();
            } finally {
                persistLock.unlock();
            }
        } else {
            previousSize = s;
            queue.offer(message);
        }
    }

    @Override
    public Message<?> poll() {
        persistLock.lock();
        try {
            Message<?> m = queue.peek();
            long writeRemaining;
            if (m != null) {
                previousSize = size.decrementAndGet();
                return queue.poll();
            } else if ((writeRemaining = writeSize.get()) > 0) {
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
                    size.decrementAndGet();
                    long remain = writeSize.decrementAndGet();
                    if (remain <= 0) {
                        long pos = reader.position();
                        source = source.newSource(pos);

                        reader.close();
                        reader = null;
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
    public long getPreviousSize() {
        return previousSize;
    }

    public PersistentConditionMailbox getCondition() {
        return condition;
    }
}
