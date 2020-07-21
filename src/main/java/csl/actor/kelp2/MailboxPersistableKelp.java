package csl.actor.kelp2;

import csl.actor.MailboxDefault;
import csl.actor.Message;
import csl.actor.cluster.PersistentFileManager;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * continuously persisting incoming messages when overflow
 */
public class MailboxPersistableKelp extends MailboxDefault {
    protected AtomicLong size;
    protected volatile long previousSize;
    protected ReentrantLock persistLock;

    protected long onMemorySize;
    protected volatile PersistentFileManager.PersistentFileWriter writer;
    protected PersistentFileManager.PersistentFileReaderSource source;
    protected volatile PersistentFileManager.PersistentFileReader reader;
    protected PersistentFileManager manager;
    protected AtomicLong writeSize;

    public MailboxPersistableKelp(PersistentFileManager manager, long onMemorySize) {
        this.manager = manager;
        this.onMemorySize = onMemorySize;
        init();
    }

    @Override
    public MailboxPersistableKelp create() {
        try {
            MailboxPersistableKelp m = (MailboxPersistableKelp) super.clone();
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
        previousSize = s;
        if (s >= onMemorySize || writeSize.get() > 0) {
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
                size.decrementAndGet();
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
}
