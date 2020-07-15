package csl.actor.kelp2;

import csl.actor.Actor;
import csl.actor.ActorBehavior;
import csl.actor.ActorBehaviorBuilder;
import csl.actor.Message;
import csl.actor.util.FileSplitter;
import csl.actor.util.StagingActor;

public class ActorBehaviorBuilderKelp extends ActorBehaviorBuilder {
    @Override
    public ActorBehavior build() {
        with(new ActorBehaviorBundle());
        with(new ActorBehaviorKelpCompleted());
        with(new ActorBehaviorKelpFileSplit());
        return super.build();
    }

    public static class ActorBehaviorBundle implements ActorBehavior {
        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            if (message instanceof ActorKelp.MessageBundle) {
                ActorKelp.MessageBundle<Object> mb = (ActorKelp.MessageBundle<Object>) message;
                mb.getData().forEach(d ->
                        self.processMessage(new Message<>(self, mb.getSender(), d)));
                return true;
            }
            return false;
        }
    }

    public static class ActorBehaviorKelpCompleted implements ActorBehavior {
        @Override
        public boolean process(Actor self, Message<?> message) {
            if (message.getData() instanceof StagingActor.StagingCompleted &&
                    self instanceof StagingActor.StagingSupported) {
                ActorRefShuffle.flush(((StagingActor.StagingSupported) self).nextStageActor(), self);
                if (self.getMailbox() instanceof MailboxKelp) {
                    ((MailboxKelp) self.getMailbox()).delete();
                }
                ((StagingActor.StagingCompleted) message.getData()).accept(self);
                return true;
            }
            return false;
        }
    }

    public static class ActorBehaviorKelpFileSplit implements ActorBehavior {
        @Override
        public boolean process(Actor self, Message<?> message) {
            if (message.getData() instanceof FileSplitter.FileSplit &&
                self instanceof ActorKelp<?>) {
                FileSplitter.FileSplit split = (FileSplitter.FileSplit) message.getData();
                ((ActorKelp<?>) self).processFileSplit(split);
                return true;
            }
            return false;
        }
    }
}
