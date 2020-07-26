package csl.actor.kelp_old;

import csl.actor.*;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

@Deprecated
public class PhaseShift implements CallableMessage.CallableMessageConsumer<Actor>, Cloneable {
    public static final long serialVersionUID = 1L;
    protected Instant startTime;
    protected Object key;
    protected ActorRef target;
    protected int count;

    public static CompletableFuture<PhaseCompleted> start(ActorSystem system, ActorRef target) {
        return start(system, target, Instant.now());
    }

    public static CompletableFuture<PhaseCompleted> start(Object key, ActorSystem system, ActorRef target) {
        return start(key, system, target, Instant.now());
    }

    public static CompletableFuture<PhaseCompleted> start(ActorSystem system, ActorRef target, Instant startTime) {
        return start(UUID.randomUUID(), system, target, startTime);
    }

    public static CompletableFuture<PhaseCompleted> start(Object key, ActorSystem system, ActorRef target, Instant startTime) {
        return new PhaseTerminalActor(system, false).start(key, target, startTime);
    }

    public static CompletableFuture<PhaseCompleted> startActors(ActorSystem system, Iterable<? extends ActorRef> target) {
        return startActors(system, target, Instant.now());
    }

    public static CompletableFuture<PhaseCompleted> startActors(ActorSystem system, Iterable<? extends ActorRef> target, Instant startTime) {
        return startActors(UUID.randomUUID(), system, target, startTime);
    }

    public static CompletableFuture<PhaseCompleted> startActors(Object key, ActorSystem system, Iterable<? extends ActorRef> target, Instant startTime) {
        return new PhaseTerminalActor(system, false).startActors(key, target, startTime);
    }

    public PhaseShift(Object key, ActorRef target) {
        this(key, target, Instant.now());
    }

    public PhaseShift(Object key, ActorRef target, Instant startTime) {
        this.key = key;
        this.target = target;
        this.startTime = startTime;
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
        actor.getDelayedMailbox().offer(new Message<>(actor, sender, this));
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

    public PhaseShift copy() {
        try {
            return (PhaseShift) super.clone();
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
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

        /**
         * To enable multiple next-stage actors:
         * <pre>
         *     ActorRef nextStage() { return null; }
         *     Iterable&lt;? extends ActoRef&gt; nextStages() { return nextList; }
         * </pre>
         * @return the completion tries to call the method if {@link #nextStage()} returns null
         */
        default Iterable<? extends ActorRef> nextStages() {
            return null;
        }
    }

    public static class PhaseShiftIntermediate implements CallableMessageConsumer<Actor> {
        public static final long serialVersionUID = 1L;
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
            actor.getDelayedMailbox().offer(new Message<>(actor, sender, this));
        }
    }

    public static class PhaseCompleted implements Serializable, CallableMessageConsumer<Actor> {
        public static final long serialVersionUID = 1L;
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
                    Iterable<? extends ActorRef> nextList = ((StageSupported) self).nextStages();
                    if (nextList != null) {
                        redirectTo(nextList, true);
                    } else {
                        sendToTarget();
                    }
                }
            } else {
                sendToTarget();
            }
        }

        public void redirectTo(ActorRef nextRouter) {
            redirectTo(nextRouter, true);
        }

        public void redirectTo(ActorRef nextRouter, boolean withCompleteThis) {
            log("#phase   redirectTo: %s %s : %s -> %s", key, getElapsedTime(), actor, nextRouter);
            if (origin.getTarget() != null) {
                origin.getTarget().tell(new PhaseRedirection(key, true)); //first, count-up
                if (withCompleteThis) {
                    origin.getTarget().tell(new PhaseRedirection(key, false)); //next, count-down
                }
            }
            nextRouter.tell(origin.copy());
        }

        public void redirectTo(Iterable<? extends ActorRef> nextActors) {
            redirectTo(nextActors, true);
        }

        public void redirectTo(Iterable<? extends ActorRef> nextActors, boolean withCompleteThis) {
            ActorRef target = origin.getTarget();

            PhaseRedirection rs = new PhaseRedirection(key, true);
            List<ActorRef> buf = new ArrayList<>();
            for (ActorRef a : nextActors) {
                if (target != null) {
                    target.tell(rs);
                }
                buf.add(a);
            }
            if (target != null && withCompleteThis) {
                target.tell(new PhaseRedirection(key, false));
            }
            if (!buf.isEmpty()) {
                log("#phase   redirectTo: %s %s : %s -> %,d actors [%s, ...]", key, getElapsedTime(), actor, buf.size(), buf.get(0));
                buf.forEach(a ->
                        a.tell(origin.copy()));
            }
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
                    .match(PhaseRedirection.class, this::redirection)
                    .match(PhaseCompleted.class, this::completed)
                    .build();
        }

        public void completed(PhaseCompleted c) {
            PhaseTerminalEntry e = completed.computeIfAbsent(c.getKey(), PhaseTerminalEntry::new);
            int n = e.getNextCount();
            int r = e.getRemainingCount();
            c.log("#phase       finish: %s %s : %s count=%,d/%,d", c.getKey(), c.getElapsedTime(), ActorSystem.timeForLog(c.getTime()), n, r);
            e.complete(c);
            if (handler != null) {
                handler.accept(getSystem(), c);
            }
            if (closeSystem) {
                getSystem().close();
            }
        }

        public CompletableFuture<PhaseCompleted> start(Object key, ActorRef initialTarget, Instant startTime) {
            PhaseTerminalEntry e = completed.computeIfAbsent(key, PhaseTerminalEntry::new);
            CompletableFuture<PhaseCompleted> c = e.start();
            PhaseShift shift = createPhaseShift(key, startTime);
            e.setStartTime(shift.getStartTime());
            initialTarget.tell(shift, this);
            return c;
        }

        public CompletableFuture<PhaseCompleted> startActors(Object key, Iterable<? extends ActorRef> initialActors, Instant startTime) {
            PhaseTerminalEntry e = completed.computeIfAbsent(key, _k -> new PhaseTerminalEntry(_k, 0));
            CompletableFuture<PhaseCompleted> c = e.start();
            e.setStartTime(startTime);

            List<ActorRef> buf = new ArrayList<>();
            for (ActorRef a : initialActors) {
                e.getNextRemainingCount();
                buf.add(a);
            }
            buf.forEach(a ->
                    a.tell(createPhaseShift(key, startTime), this));
            return c;
        }

        protected PhaseShift createPhaseShift(Object key, Instant startTime) {
            return new PhaseShift(key, this, startTime);
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

        public void redirection(PhaseRedirection s) {
            PhaseTerminalEntry e = completed.computeIfAbsent(s.getKey(), PhaseTerminalEntry::new);
            if (s.isStart()) {
                e.getNextRemainingCount();
            } else {
                e.getNextCount(); //intermediate completion
            }
        }
    }

    public static class PhaseTerminalEntry {
        protected Object key;
        protected int count;
        protected Instant startTime;
        protected CompletableFuture<PhaseCompleted> future;
        protected int remainingCount;

        public PhaseTerminalEntry(Object key) {
            this(key, 1);
        }

        public PhaseTerminalEntry(Object key, int remainingCount) {
            this.key = key;
            this.remainingCount = remainingCount;
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
            if (count >= remainingCount) {
                future().complete(c);
            }
        }

        public synchronized int getRemainingCount() {
            return remainingCount;
        }

        public synchronized int getNextRemainingCount() {
            ++remainingCount;
            return remainingCount;
        }
    }

    public static class PhaseRedirection implements Serializable, CallableMessageConsumer<Actor> {
        public static final long serialVersionUID = 1L;

        protected Object key;
        protected boolean start;

        public PhaseRedirection(Object key, boolean start) {
            this.key = key;
            this.start = start;
        }

        public Object getKey() {
            return key;
        }

        public boolean isStart() {
            return start;
        }

        @Override
        public void accept(Actor self) {
            if (self instanceof PhaseTerminalActor) {
                ((PhaseTerminalActor) self).redirection(this);
            }
        }
    }

}
