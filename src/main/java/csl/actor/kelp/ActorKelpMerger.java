package csl.actor.kelp;

import csl.actor.*;
import csl.actor.util.ResponsiveCalls;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ActorKelpMerger<ActorType extends ActorKelp<ActorType>> implements AutoCloseable {
    protected ActorSystem system;
    protected ExecutorService executor;
    protected ConfigKelp config;

    public ActorKelpMerger(ActorSystem system, ConfigKelp config) {
        this.system = system;
        this.config = config;
        executor = Executors.newCachedThreadPool();
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    @SuppressWarnings("unchecked")
    public ActorType mergeToLocalSync(List<? extends ActorRef> mebers) {
        ActorRef ref = mergeSync(mebers);
        if (ref instanceof Actor) {
            return (ActorType) ref;
        } else {
            try {
                ActorKelpSerializable<?> k = toState(system, null, ref, false);
                return (ActorType) (k == null ? null : k.restore(system, -1, config));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public ActorRef mergeSync(List<? extends ActorRef> members) {
        try {
            return mergeAsync(members).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public Future<ActorRef> mergeAsync(List<? extends ActorRef> members) {
        if (members.isEmpty()) {
            return null;
        } else if (members.size() == 1) {
            return CompletableFuture.completedFuture(members.get(0));
        } else if (members.size() == 2) {
            return mergeAsync(
                    CompletableFuture.completedFuture(members.get(0)),
                    CompletableFuture.completedFuture(members.get(1)));
        } else {
            int hs = members.size() / 2;
            return mergeAsync(
                    mergeAsync(members.subList(0, hs)),
                    mergeAsync(members.subList(hs, members.size())));
        }
    }

    public Future<ActorRef> mergeAsync(Future<ActorRef> l, Future<ActorRef> r) {
        if (l == null) {
            return r;
        } else if (r == null) {
            return l;
        } else {
            try {
                return executor.submit(() -> merge(l.get(), r.get()));
            } catch (Exception ex) {
                config.log(ex, "error: %s : ", l, r);
                return l;
            }
        }
    }

    public ActorRef merge(ActorRef l, ActorRef r) {
        try {
            return ResponsiveCalls.sendTask(system, l, new MergeTask(l, r)).get();
        } catch (Exception ex) {
            config.log(ex, "error: %s : ", l, r);
            return l;
        }
    }

    public static class MergeTask implements Serializable, CallableMessage<Actor, ActorRef> {
        public static final long serialVersionUID = -1;

        protected ActorRef l;
        protected ActorRef r;

        public MergeTask() {}

        public MergeTask(ActorRef l, ActorRef r) {
            this.l = l;
            this.r = r;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public ActorRef call(Actor self) {
            try {
                ActorKelpSerializable<?> anotherState = toState(self.getSystem(), l, r, true);
                ((ActorKelp<?>) self).merge((ActorKelpSerializable) anotherState);
                return self;
            } catch (Exception ex) {
                self.getSystem().getLogger().log(true, 0, ex, "error: %s", self);
                return self;
            }
        }
    }

    public static ActorKelpSerializable<?> toState(ActorSystem system, ActorRef l, ActorRef r, boolean disable) {
        try {
            return ResponsiveCalls.sendTask(system, r, new ToStateTask(l, disable)).get();
        } catch (Exception ex) {
            system.getLogger().log(true, 0, ex, "error: %s", r);
            return null;
        }
    }


    public static class ToStateTask implements Serializable, CallableMessage<Actor, ActorKelpSerializable<?>> {
        public static final long serialVersionUID = -1;
        protected ActorRef l;
        protected boolean disable;

        public ToStateTask() {}

        public ToStateTask(ActorRef l, boolean disable) {
            this.l = l;
            this.disable = disable;
        }

        @Override
        public ActorKelpSerializable<?> call(Actor another) {
            if (disable) {
                new DelegateActor(another.getSystem(), another.getName(), l);

                //already merged actors
                ((ActorKelp<?>) another).getMergedActorNames().forEach(n ->
                        new DelegateActor(another.getSystem(), n, l));
            }
            ActorKelpSerializable<?> s = ((ActorKelp<?>) another).toSerializable();
            s.internalStateUsed = true;
            return s;
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
            target.tellMessage(message.renewTarget(target));
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + name + " -> " + target + ")";
        }
    }
}
