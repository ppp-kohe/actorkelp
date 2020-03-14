package csl.actor.cluster;

import csl.actor.*;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class PhaseShift implements CallableMessage.CallableMessageConsumer<Actor> {
    protected Instant startTime;
    protected Object key;
    protected ActorRef target;
    protected int count;

    public static CompletableFuture<PhaseCompleted> start(ActorSystem system, ActorRef target) {
        return start(UUID.randomUUID(), system, target);
    }

    public static CompletableFuture<PhaseCompleted> start(Object key, ActorSystem system, ActorRef target) {
        return new PhaseTerminalActor(system, false).start(key, target);
    }

    public PhaseShift(Object key, ActorRef target) {
        this.key = key;
        this.target = target;
        startTime = Instant.now();
    }

    public PhaseShift(Object key) {
        this(key, null);
    }

    public Object getKey() {
        return key;
    }

    public ActorRef getTarget() {
        return target;
    }

    @Override
    public void accept(Actor self) {
        accept(self, null);
    }

    @Override
    public void accept(Actor actor, ActorRef sender) {
        if (!actor.isEmptyMailbox()) {
            count = 0;
            retry(actor, sender);
        } else if (count < 3) {
            retry(actor, sender);
        } else {
            completed(actor, sender);
        }
    }

    public int getCount() {
        return count;
    }

    public void retry(Actor actor, ActorRef sender) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException ie) {
            //ignore
        }
        ++count;
        actor.tell(this, sender);
    }

    public PhaseCompleted createCompleted(Actor actor) {
        return new PhaseCompleted(key, actor, this);
    }

    public void completed(Actor router, ActorRef sender) {
        count = 0;
        PhaseCompleted c = createCompleted(router);
        log(router, "#phase    completed: %s %s : %s", key, c.getElapsedTime(), router);
        router.tell(c, router);
    }

    public void log(Actor router, String str, Object... args) {
        if (router instanceof StageSupported) {
            ((StageSupported) router).logPhase(str, args);
        } else {
            router.getSystem().getLogger().log(str, args);
        }
    }

    public Instant getStartTime() {
        return startTime;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this)) +
                "(key=" + key + ", target=" + target + ", count=" + count + ", start=" + startTime + ")";
    }

    public PhaseShiftIntermediate createIntermediate(ActorRef actor, PhaseShiftIntermediateType type) {
        return new PhaseShiftIntermediate(key, actor, type);
    }

    public enum PhaseShiftIntermediateType {
        PhaseIntermediateRouterStart,
        PhaseIntermediateFinishCanceled,
        PhaseIntermediateFinishLeaf
    }

    public interface StageSupported {
        ActorRef nextStage();
        void logPhase(String str, Object... args);
    }

    public static class PhaseShiftIntermediate implements CallableMessageConsumer<Actor> {
        protected Object key;
        protected ActorRef actor;
        protected PhaseShiftIntermediateType type;

        protected int count;

        public PhaseShiftIntermediate(Object key, ActorRef actor, PhaseShiftIntermediateType type) {
            this.key = key;
            this.actor = actor;
            this.type = type;
        }

        public Object getKey() {
            return key;
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" + type +
                    ", key=" + key +
                    ", actor=" + actor +
                    '}';
        }

        public PhaseShiftIntermediateType getType() {
            return type;
        }

        @Override
        public void accept(Actor self, ActorRef sender) {
            accept(self, sender, sender);
        }

        @Override
        public void accept(Actor self) {
            throw new RuntimeException("illegal: " + this + " : " + self);
        }

        public void accept(Actor self, ActorRef router, ActorRef sender) {
            if (!self.isEmptyMailbox() || count < 3) {
                retry(self, sender);
            } else {
                router.tell(this, self);
                count = 0;
            }
        }

        protected void retry(Actor actor, ActorRef sender) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                //ignore
            }
            ++count;
            actor.tell(this, sender);
        }
    }

    public static class PhaseCompleted implements Serializable, CallableMessageConsumer<Actor> {
        protected Object key;
        protected ActorRef actor;
        protected PhaseShift origin;
        protected Instant time;

        public PhaseCompleted(Object key, ActorRef actor, PhaseShift origin) {
            this.key = key;
            this.actor = actor;
            this.origin = origin;
            this.time = Instant.now();
        }

        public Object getKey() {
            return key;
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "key=" + key +
                    ", actor=" + actor +
                    '}';
        }

        @Override
        public void accept(Actor self) {
            if (self instanceof StageSupported) {
                ActorRef next = ((StageSupported) self).nextStage();
                if (next != null) {
                    redirectTo(next);
                } else {
                    sendToTarget();
                }
            } else {
                sendToTarget();
            }
        }

        public void redirectTo(ActorRef nextRouter) {
            log("#phase   redirectTo: %s %s : %s -> %s", key, getElapsedTime(), actor, nextRouter);
            nextRouter.tell(origin);
        }


        public void log(String str, Object... args) {
            if (actor instanceof StageSupported) {
                ((StageSupported) actor).logPhase(str, args);
            } else if (actor instanceof Actor) {
                ((Actor) actor).getSystem().getLogger().log(str, args);
            } else {
                new ActorSystemDefault.SystemLoggerErr().log(str, args);
            }
        }

        public void sendToTarget() {
            log("#phase sendToTarget: %s %s : %s -> %s", key, getElapsedTime(), actor, origin.getTarget());
            if (origin.getTarget() != null) {
                origin.getTarget().tell(this);
            }
        }

        public PhaseShift getOrigin() {
            return origin;
        }

        public Instant getTime() {
            return time;
        }

        public Duration getElapsedTime() {
            return Duration.between(origin.getStartTime(), time);
        }
    }

    public static class PhaseTerminalActor extends ActorDefault {
        protected boolean closeSystem;
        protected BiConsumer<ActorSystem, PhaseCompleted> handler;

        protected Map<Object, PhaseTerminalEntry> completed = new ConcurrentHashMap<>();

        public PhaseTerminalActor(ActorSystem system, boolean closeSystem, BiConsumer<ActorSystem, PhaseCompleted> handler) {
            super(system);
            this.closeSystem = closeSystem;
            this.handler = handler;
            this.name = getClass().getName() + "#" + UUID.randomUUID().toString();
            system.register(this);
        }

        public PhaseTerminalActor(ActorSystem system, boolean closeSystem) {
            this(system, closeSystem, null);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(PhaseCompleted.class, this::completed)
                    .build();
        }

        public void completed(PhaseCompleted c) {
            PhaseTerminalEntry e = completed.computeIfAbsent(c.getKey(), PhaseTerminalEntry::new);
            int n = e.getNextCount();
            c.log("#phase       finish: %s %s : %s count=%,d", c.getKey(), c.getElapsedTime(), c.getTime(), n);
            e.complete(c);
            if (handler != null) {
                handler.accept(getSystem(), c);
            }
            if (closeSystem) {
                getSystem().close();
            }
        }

        public CompletableFuture<PhaseCompleted> start(Object key, ActorRef initialTarget) {
            PhaseTerminalEntry e = completed.computeIfAbsent(key, PhaseTerminalEntry::new);
            CompletableFuture<PhaseCompleted> c = e.start();
            PhaseShift shift = createPhaseShift(key);
            e.setStartTime(shift.getStartTime());
            initialTarget.tell(shift, this);
            return c;
        }

        protected PhaseShift createPhaseShift(Object key) {
            return new PhaseShift(key, this);
        }

        public Map<Object, PhaseTerminalEntry> getCompleted() {
            return new HashMap<>(completed);
        }

        public int getCompletedCount(Object key) {
            PhaseTerminalEntry e = completed.get(key);
            if (e == null) {
                return 0;
            } else {
                return e.getCount();
            }
        }

        @Override
        public String toStringContents() {
            return "";
        }
    }

    public static class PhaseTerminalEntry {
        protected Object key;
        protected int count;
        protected Instant startTime;
        protected CompletableFuture<PhaseCompleted> future;

        public PhaseTerminalEntry(Object key) {
            this.key = key;
        }

        public Object getKey() {
            return key;
        }

        public void setStartTime(Instant startTime) {
            this.startTime = startTime;
        }

        public Instant getStartTime() {
            return startTime;
        }

        public synchronized int getNextCount() {
            ++count;
            return count;
        }

        public synchronized int getCount() {
            return count;
        }

        public synchronized CompletableFuture<PhaseCompleted> future() {
            if (future == null) {
                future = new CompletableFuture<>();
            }
            return future;
        }

        public synchronized CompletableFuture<PhaseCompleted> start() {
            if (future == null || future.isDone()) {
                startTime = Instant.now();
                future = new CompletableFuture<>();
            }
            return future;
        }

        public synchronized void complete(PhaseCompleted c) {
            future().complete(c);
        }
    }

}
