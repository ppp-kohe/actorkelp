package csl.actor.example.kelp2;

import csl.actor.*;
import csl.actor.kelp2.*;
import csl.actor.util.ResponsiveCalls;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Random;

public class ExampleMerge {
    public static void main(String[] args) throws Exception {
        ActorSystemDefault system = new ActorSystemKelp.ActorSystemDefaultForKelp();

        MyActor a = new MyActor(system);
        KelpStage<MyActor> as = a.shuffle();
        ActorRef ref = ActorRefLocalNamed.get(system, ((Actor) as.getMemberActors().get(as.getMemberActors().size() - 1)).getName());
        system.getLogger().log("finish merge: %s", as.merge());

        system.getNamedActorMap().forEach((n, ac) ->
                system.getLogger().log(" actor: %s -> %s", n, ac));

        system.getLogger().log("ref: %s", ref);
        ResponsiveCalls.sendTaskConsumer(system, ref, (self) -> {
            self.getSystem().getLogger().log("after merge: %s", self);
        }).get();
        system.close();
    }

    public static class MyActor extends ActorKelp<MyActor> {
        @TransferredState(mergeFunc = MyMergerFunction.class) int state;
        int independentState;
        @TransferredState(mergeOnly = true) MyState mergerState;

        public MyActor(ActorSystem system) {
            super(system);
            state = new Random().nextInt(1000);
            independentState = new Random().nextInt(1000);
            mergerState = new MyState();
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder().build();
        }

        @Override
        public void setInternalState(Serializable data) {
            super.setInternalState(data);
            logState("init");
        }

        private void logState(String str) {
            getSystem().getLogger().log("%s %s", str, toStringContents());
        }

        @Override
        public void mergeInternalState(ActorKelpSerializable<MyActor> s, ActorKelpSerializable.MergingContext context, Serializable data) {
            String msg = String.format("%2d + %2d : %s ", getShuffleIndex(), s.shuffleIndex, context);

            getSystem().getLogger().log("merge: %s", msg);
            super.mergeInternalState(context, data);
            logState("merge");
            try {
                Thread.sleep(500);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            getSystem().getLogger().log("merge: %s End", msg);
        }

        @Override
        public String toStringContents() {
            return super.toStringContents() +
                    String.format(" state=%,4d, iState=%,4d sIdx=%,d merger=%s mergedCount=%,d",
                            state, independentState, getShuffleIndex(), mergerState, getMergedCount());
        }
    }

    public static class MyMergerFunction implements ActorKelpMergerFunctions.MergerFunction<Integer> {
        public static MyMergerFunction get(ActorKelp.MergerOpType op, Type type) {
            return new MyMergerFunction();
        }
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Integer l, Integer r) {
            System.err.println("[merge: " + l + " , " + r + "] : " + (l + r));
            return l + r;
        }
    }

    public static class MyState implements ActorKelpMergerFunctions.Mergeable<MyState> {
        public static final long serialVersionUID = -1;
        int n;
        public MyState() {
            this(new Random().nextInt(100));
        }

        public MyState(int n) {
            this.n = n;
        }

        @Override
        public MyState merge(ActorKelpSerializable.MergingContext context, MyState another) {
            MyState s= new MyState(n + another.n);
            System.err.println("[merge: " + this + " , " + another + "] -> " + s);
            return s;
        }

        @Override
        public String toString() {
            return "MyState(" + n + ")";
        }
    }
}
