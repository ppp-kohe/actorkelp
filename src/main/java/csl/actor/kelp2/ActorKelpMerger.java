package csl.actor.kelp2;

import csl.actor.*;

import java.util.ArrayList;
import java.util.List;

public class ActorKelpMerger {
    public void merge(List<? extends ActorRef> members) {
        int size = members.size() / 2;
        MergingSplit s1 = new MergingSplit(null, null, members.subList(0, size));
        MergingSplit s2 = new MergingSplit(null, null, members.subList(size, members.size()));
        s1.go();
        s2.go();
    }

    public static class MergingSplit implements CallableMessage.CallableMessageConsumer<ActorKelp<?>> {
        protected ActorRef actor;
        protected MergingSplit parent;
        protected List<? extends ActorRef> members;

        public MergingSplit(ActorRef actor, MergingSplit parent, List<? extends ActorRef> members) {
            this.actor = actor;
            this.parent = parent;
            this.members = members;
        }

        public void go() {
            members.get(0).tell(this);
        }

        @Override
        public void accept(ActorKelp<?> self) {
            if (members.size() == 2) {
                ActorRef r = members.get(1);
                new DelegateActor(self.getSystem(), self.getName(), r); //invalidate left
                r.tell(new MergingSplitToRight(this, self.toSerializable()));
            } else if (members.size() <= 1) {
                //TODO

            } else {
                int size = members.size() / 2;
                MergingSplit s1 = new MergingSplit(self, this, members.subList(0, size));
                MergingSplit s2 = new MergingSplit(self, this, members.subList(size, members.size()));
                s1.go();
                s2.go();
            }
        }

        public MergingSplit getParent() {
            return parent;
        }
    }

    public static class DelegateActor extends Actor {
        protected ActorRef target;

        public DelegateActor(ActorSystem system, String name, ActorRef target) {
            super(system, name);
            this.target = target;
        }

        @Override
        public void processMessage(Message<?> message) {
            target.tellMessage(message);
        }
    }

    public static class MergingSplitToRight implements CallableMessage.CallableMessageConsumer<ActorKelp<?>> {
        protected MergingSplit split;
        protected ActorKelpSerializable<?> left;

        public MergingSplitToRight(MergingSplit split, ActorKelpSerializable<?> left) {
            this.split = split;
            this.left = left;
        }

        @Override
        public void accept(ActorKelp<?> self) {
            new MergingSplitCompleted()
        }
    }

    public static class MergingSplitCompleted implements CallableMessage.CallableMessageConsumer<ActorKelp<?>> {
        protected MergingSplit split;
        protected ActorRef mergedActor;

        public MergingSplitCompleted(MergingSplit split, ActorRef mergedActor) {
            this.split = split;
            this.mergedActor = mergedActor;
        }

        @Override
        public void accept(ActorKelp<?> self) {
            //boolean end = self.addCompleted(split)
            boolean end = false;
            List<ActorRef> leftAndRight = new ArrayList<>();
            if (end) {
                new MergingSplit(split, leftAndRight).go();
            }
        }
    }
}
