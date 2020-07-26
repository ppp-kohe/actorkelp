package csl.actor.persist;

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
    protected long onMemorySize;

    protected volatile PersistentFileManager persistentManager;

    public static boolean logPersist = System.getProperty("csl.actor.persist.log", "true").equals("true");
    public static boolean logDebugPersist = System.getProperty("csl.actor.persist.debug", "false").equals("true");
    public static int logColorPersist = ActorSystem.systemPropertyColor("csl.actor.persist.color", 94);

    public MailboxPersistableReplacement(PersistentFileManager persistent, long sizeLimit, long onMemorySize) {
        this(persistent, new PersistentConditionMailbox.PersistentConditionMailboxSizeLimit(sizeLimit), onMemorySize);
    }

    public MailboxPersistableReplacement(PersistentFileManager persistent, PersistentConditionMailbox condition, long onMemorySize) {
        this.condition = condition;
        this.onMemorySize = onMemorySize;
        this.persistentManager = persistent;
        init();
    }

    @Override
    public KryoBuilder.SerializerFunction getSerializer() {
        return persistentManager.getSerializer();
    }

    public ActorSystem.SystemLogger getLogger() {
        return persistentManager.getLogger();
    }

    public PersistentFileWriter createWriter() {
        return persistentManager.createWriter("mailbox");
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
            try (PersistentFileWriter ms = createWriter()) {
                reader = new MessageOnStorage(ms.createReaderSourceFromCurrentPosition());
                newQueue.offer(reader);
                offered++;
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
                            ms.write(m);
                            top = false;
                        }
                    } else {
                        ms.write(m);
                        top = false;
                    }
                    oldQueue.poll();
                    ++polled;
                }
            } catch (Exception ex) { //retry?
                getLogger().log(true, logColorPersist, ex, "mailbox persist: polled=%,d", polled);
            }
            getLogger().log(logDebugPersist, logColorPersist, "mailbox persisted: %s sizeDelta=%,d", reader, (-polled + offered));
            size.addAndGet(-polled + offered);
        }
    }

    private boolean persistRemaining(PersistentFileWriter ms, MessageOnStorage mOnS) {
        boolean saved = false;
        while (true) {
            Message<?> m = mOnS.readNext();
            if (m == null) {
                break;
            } else {
                ms.write(m);
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

    public static class MessageOnStorage extends Message<Object> {
        public static final long serialVersionUID = 1L;
        protected PersistentFileManager.PersistentFileReaderSource source;
        protected transient PersistentFileManager.PersistentFileReader reader;
        protected transient MessageOnStorage currentMessage;

        public MessageOnStorage() {
            super(null, null, null);
        }

        public MessageOnStorage(PersistentFileManager.PersistentFileReaderSource source) {
            this();
            this.source = source;
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
                    reader.getManager().getLogger().log(logDebugPersist, logColorPersist, "readNext finish: %s", reader);
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
                        return m;
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(String.format("readNext: %s", source), ex);
            }
        }
    }
}
