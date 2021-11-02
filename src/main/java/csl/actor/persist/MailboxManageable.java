package csl.actor.persist;

import csl.actor.Mailbox;
import csl.actor.MailboxDefault;
import csl.actor.Message;
import csl.actor.remote.KryoBuilder;

import java.util.concurrent.atomic.AtomicLong;

public interface MailboxManageable extends Mailbox {
    KryoBuilder.SerializerFunction getSerializer();
    /** @return a past mailbox size info. the value might be different from current size of the mailbox.
     *      used for determining saving the mailbox data */
    long getPreviousSizeOnMemory();

    /**
     * @return the total size including storage saved
     */
    long getSize();

    /**
     * @return the size on memory by atomic operation
     */
    long getSizeOnMemory();

    PersistentConditionMailbox getCondition();

    default MailboxManageable createManageable() {
        return (MailboxManageable) create();
    }

    static int messageSize(Message<?> m) {
        return m == null ? 0 : m.dataSize();
    }

    class MailboxDefaultManageable extends MailboxDefault implements MailboxManageable {
        protected PersistentFileManager manager;
        protected AtomicLong size;
        protected PersistentConditionMailbox condition;
        protected volatile long previousSize;

        public MailboxDefaultManageable(PersistentFileManager manager) {
            this(manager, new PersistentConditionMailbox.PersistentConditionMailboxNever());
        }

        public MailboxDefaultManageable(PersistentFileManager manager, PersistentConditionMailbox condition) {
            this.manager = manager;
            this.condition = condition;
        }

        @Override
        public long getSize() {
            return size.get();
        }

        @Override
        public long getSizeOnMemory() {
            return size.get();
        }

        @Override
        public void offer(Message<?> message) {
            previousSize = size.addAndGet(messageSize(message));
            queue.offer(message);
        }

        @Override
        public Message<?> poll() {
            Message<?> m = queue.poll();
            if (m != null) {
                previousSize = size.addAndGet(-messageSize(m));
            }
            return m;
        }

        @Override
        public KryoBuilder.SerializerFunction getSerializer() {
            return manager.getSerializer();
        }

        @Override
        public long getPreviousSizeOnMemory() {
            return previousSize;
        }

        @Override
        public PersistentConditionMailbox getCondition() {
            return condition;
        }
    }
}
