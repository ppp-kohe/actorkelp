package csl.actor.msgassoc;

import csl.actor.Mailbox;
import csl.actor.Message;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class MailboxPersisted implements Mailbox, Cloneable {
    protected volatile ConcurrentLinkedQueue<Message<?>> queue = new ConcurrentLinkedQueue<>();
    protected AtomicLong size = new AtomicLong();
    protected volatile long previousSize;

    protected long sizeLimit = Integer.MAX_VALUE / 64;
    protected long onMemorySize = 100_000;

    protected volatile MessageSaving saving;

    public interface MessageSaving {
        MessageSavingSession get();
    }

    public interface MessageSavingSession extends AutoCloseable {
        void save(Message<?> msg);
        MessageOnStorage reader();
    }

    public static class MessageOnStorage extends Message<Object> {
        public MessageOnStorage() {
            super(null, null, null);
        }

        public Message<?> readNext() {
            return null;
        }
    }

    public MailboxPersisted setSaving(MessageSaving saving) {
        this.saving = saving;
        return this;
    }

    public MessageSaving getSaving() {
        return saving;
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
    public MailboxPersisted create() {
        try {
            MailboxPersisted p = (MailboxPersisted) super.clone();
            p.queue = new ConcurrentLinkedQueue<>();
            p.size = new AtomicLong();
            p.previousSize = 0;
            return p;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public void offer(Message<?> message) {
        long s = size.incrementAndGet();
        boolean increasing = s > previousSize;
        previousSize = s;
        queue.offer(message);
        if (s > sizeLimit && increasing && saving != null) {
            persist();
        }
    }

    protected synchronized void persist() {
        long s = size.get();
        if (s > sizeLimit && saving != null) {
            ConcurrentLinkedQueue<Message<?>> oldQueue = queue;
            ConcurrentLinkedQueue<Message<?>> newQueue = new ConcurrentLinkedQueue<>();
            this.queue = newQueue;
            size.addAndGet(-s);
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
            try (MessageSavingSession ms = saving.get()) {
                reader = ms.reader();
                while (true) {
                    Message<?> m = oldQueue.peek();
                    if (m == null) {
                        break;
                    }
                    ms.save(m);
                    ++saved;
                    oldQueue.poll();
                }
            } catch (Exception ex) {
                ex.printStackTrace(); //TODO retry?
            }
            if (reader != null && saved > 0) {
                newQueue.offer(reader);
            }
            size.addAndGet(newQueueSize);
        }
    }

    @Override
    public Message<?> poll() {
        Message<?> m = queue.peek();

        if (m instanceof MessageOnStorage) {
            Message<?> next = ((MessageOnStorage) m).readNext();
            if (next == null) {
                queue.poll();
            }
            return poll();
        } else {
            if (m != null) {
                size.getAndDecrement();
            }
            return queue.poll();
        }
    }

    public static class MessageSavingFile implements MessageSaving {
        protected String path;
        protected long count;
        protected Function<Message<?>, ByteBuffer> serializer;
        protected Function<ByteBuffer, Message<?>> deserializer;

        //TODO setting serializers

        @Override
        public synchronized MessageSavingSession get() {
            long c = count;
            ++count;
            Path p = Paths.get(path, String.format("mailbox-%05d", c));
            while (Files.exists(p)) {
                p = Paths.get(path, String.format("mailbox-%05d", c));
                ++count;
            }

            return new MessageSavingFileSession(this, p);
        }

        public Function<ByteBuffer, Message<?>> getDeserializer() {
            return deserializer;
        }

        public Function<Message<?>, ByteBuffer> getSerializer() {
            return serializer;
        }
    }

    public static class MessageSavingFileSession implements MessageSavingSession {
        protected Path path;
        protected BufferedOutputStream out;
        protected MessageSavingFile saving;

        protected ByteBuffer header = ByteBuffer.allocate(4);

        public MessageSavingFileSession(MessageSavingFile saving, Path path) {
            this.saving = saving;
            this.path = path;
        }

        @Override
        public void save(Message<?> msg) {
            try {
                if (out == null) {
                    out = new BufferedOutputStream(Files.newOutputStream(path));
                }
                ByteBuffer buf = saving.getSerializer().apply(msg);
                header.clear();
                header.order(ByteOrder.LITTLE_ENDIAN);
                header.putInt(buf.remaining());
                header.flip();
                out.write(header.array(), 0, header.remaining());
                out.write(buf.array(), buf.position(), buf.remaining());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public MessageOnStorage reader() {
            return new MessageOnStorageFile(saving, path.toString());
        }

        @Override
        public synchronized void close() throws Exception {
            if (out != null) {
                out.close();
            }
        }
    }

    public static class MessageOnStorageFile extends MessageOnStorage {
        protected transient MessageSaving saving;
        protected String path;
        protected transient InputStream in;
        protected transient ByteBuffer buffer;

        public MessageOnStorageFile(MessageSaving saving, String path) {
            this.saving = saving;
            this.path = path;
        }

        @Override
        public Message<?> readNext() {
            try {
                if (in == null) {
                    in = new BufferedInputStream(Files.newInputStream(Paths.get(path)));
                }
                if (buffer == null) {
                    buffer = ByteBuffer.allocate(8192);
                }
                int n = in.read(buffer.array());
                if (n > 0) {
                    buffer.position(0);
                    buffer.limit(n);
                    Message<?> m = ((MessageSavingFile) saving).getDeserializer().apply(buffer);
                    if (m instanceof MessageOnStorageFile) {
                        ((MessageOnStorageFile) m).saving = saving;
                    }
                    return m;
                } else {
                    in.close();
                    buffer = null;
                    return null;
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
