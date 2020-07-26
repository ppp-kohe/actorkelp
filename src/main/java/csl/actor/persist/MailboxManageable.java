package csl.actor.persist;

import csl.actor.Mailbox;
import csl.actor.MailboxDefault;
import csl.actor.Message;
import csl.actor.remote.KryoBuilder;

import java.util.concurrent.atomic.AtomicLong;

public interface MailboxManageable extends Mailbox {
    KryoBuilder.SerializerFunction getSerializer();
    long getPreviousSize();
    PersistentConditionMailbox getCondition();

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
        public void offer(Message<?> message) {
            previousSize = size.incrementAndGet();
            queue.offer(message);
        }

        @Override
        public Message<?> poll() {
            Message<?> m = queue.poll();
            if (m != null) {
                previousSize = size.decrementAndGet();
            }
            return m;
        }

        @Override
        public KryoBuilder.SerializerFunction getSerializer() {
            return manager.getSerializer();
        }

        @Override
        public long getPreviousSize() {
            return previousSize;
        }

        @Override
        public PersistentConditionMailbox getCondition() {
            return condition;
        }
    }
}
