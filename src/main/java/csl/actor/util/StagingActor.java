package csl.actor.util;

import csl.actor.*;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * The actor managing computation stages of actor processing
 */
public class StagingActor extends ActorDefault {
    protected UUID id;
    protected StagingEntry entry;
    protected BiConsumer<StagingActor, StagingCompleted> handler;
    protected boolean systemClose;

    protected Instant logLastTime;
    protected Duration logDuration;

    public StagingActor(ActorSystem system) {
        super(system);
        this.id = UUID.randomUUID();
        this.name = createName();
        this.logLastTime = Instant.EPOCH;
        this.logDuration = Duration.ofSeconds(3);
        entry = new StagingEntry(new StagingTask(Instant.now(), id, this));
        system.register(this);
    }

    public String createName() {
        return getClass().getName() + "#" + id.toString();
    }

    ////configure

    public static StagingActor staging(ActorSystem system) {
        return new StagingActor(system);
    }

    public StagingActor withStartTime(Instant time) {
        entry.getTask().setStartTime(time);
        return this;
    }

    public StagingActor withId(Object id) {
        entry.getTask().setKey(id);
        return this;
    }

    public StagingActor withHandler(BiConsumer<StagingActor, StagingCompleted> handler) {
        this.handler = handler;
        return this;
    }

    public StagingActor withSystemClose(boolean close) {
        systemClose = close;
        return this;
    }

    public StagingActor withLogDuration(Duration d) {
        this.logDuration = d;
        return this;
    }

    public StagingActor withWatcherSleepTimeMs(long watcherSleepTimeMs) {
        entry.getTask().setWatcherSleepTimeMs(watcherSleepTimeMs);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <ActorType extends Actor> StagingActor withHandler(Class<ActorType> actorType,
                                                                      CallableMessage.CallableMessageConsumer<ActorType> handler) {
        participantsHandler.add(new CompletionHandlerForActor(actorType, (CallableMessage.CallableMessageConsumer<Actor>) handler));
        return this;
    }

    public <ActorType> StagingActor withHandlerNonActor(Class<ActorType> actorType,
                                                              CallConsumerI<ActorType> handler) {
        participantsHandler.add(new CompletionHandlerForActor(actorType, (self) -> {
            handler.accept(actorType.cast(self));
        }));
        return this;
    }

    public interface CallConsumerI<ActorType> extends Serializable {
        void accept(ActorType type);
    }

    ////starting

    public CompletableFuture<StagingCompleted> start(ActorRef target) {
        return startActors(Collections.singletonList(target));
    }

    public CompletableFuture<StagingCompleted> startActors(Iterable<? extends ActorRef> targets) {
        //create temporary completed
        new StagingCompleted(entry.getTask(), this, this)
                .redirectTo(this, targets, false);
        return entry.future();
    }

    ////

    @Override
    protected ActorBehavior initBehavior() {
        return behaviorBuilder()
                .match(StagingNotification.class, this::notified)
                .match(StagingCompleted.class, this::complete)
                .build();
    }

    @Override
    public String toStringContents() {
        return String.format("%s", ActorSystem.timeForLog(this.entry.getTask().getStartTime()));
    }

    /**
     * the staging task info.
     */
    public static class StagingTask implements Serializable {
        public static final long serialVersionUID = 1L;
        protected Instant startTime;
        protected Object key;
        protected ActorRef terminalActor; //stagingActor
        protected long watcherSleepTimeMs;

        public StagingTask(Instant startTime, Object key, ActorRef terminalActor) {
            this.startTime = startTime;
            this.key = key;
            this.terminalActor = terminalActor;
        }

        public void setStartTime(Instant startTime) {
            this.startTime = startTime;
        }

        public void setKey(Object key) {
            this.key = key;
        }

        public void setWatcherSleepTimeMs(long watcherSleepTimeMs) {
            this.watcherSleepTimeMs = watcherSleepTimeMs;
        }

        public long getWatcherSleepTimeMs() {
            return watcherSleepTimeMs;
        }

        public Instant getStartTime() {
            return startTime;
        }

        public ActorRef getTerminalActor() {
            return terminalActor;
        }

        public Object getKey() {
            return key;
        }

        @Override
        public String toString() {
            return String.format("stage %s: %-11s", key, getElapsedTime());
        }

        public Duration getElapsedTime() {
            return getElapsedTime(Instant.now());
        }

        public Duration getElapsedTime(Instant time) {
            return Duration.between(startTime, time);
        }
    }

    /**
     * the task observing actor's mailbox;
     *   if no messages on the box, it sends {@link StagingCompleted} to the actor
     */
    public static class StagingWatcher implements CallableMessage.CallableMessageConsumer<Actor>, Cloneable {
        public static final long serialVersionUID = 1L;
        protected StagingTask task;
        protected Object sender;
        protected int count;

        public StagingWatcher(StagingTask task, Object sender) {
            this.task = task;
            this.sender = sender;
        }

        public StagingTask getTask() {
            return task;
        }

        public Object getSender() {
            return sender;
        }

        public int getCount() {
            return count;
        }

        @Override
        public void accept(Actor self) {
            accept(self, null);
        }

        @Override
        public void accept(Actor self, ActorRef sender) {
            if (!self.isEmptyMailbox()) {
                count = 0;
                retry(self, sender);
            } else if (count < 3) {
                retry(self, sender);
            } else {
                completed(self);
            }
        }

        public void retry(Actor actor, ActorRef sender) {
            long s = task.getWatcherSleepTimeMs();
            if (s > 0) {
                try {
                    Thread.sleep(s);
                } catch (InterruptedException ie) {
                    //ignore
                }
            }
            ++count;
            actor.getDelayedMailbox().offer(new Message<>(actor, sender, this));
        }

        public void completed(Actor actor) {
            count = 0;
            StagingCompleted c = createCompleted(actor, this.sender);
            actor.tell(c, actor);
        }

        public StagingCompleted createCompleted(Actor actor, Object sender) {
            return new StagingCompleted(task, actor, sender);
        }

        public StagingWatcher copy() {
            try {
                return (StagingWatcher) super.clone();
            } catch (CloneNotSupportedException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this)) +
                    "(task=" + task +
                    ", count=" + count +
                    ", sender=" + sender +
                    ")";
        }
    }

    /**
     * the completion handling actor.
     *  An actor can handle this message manually for doing special actions at stage-completion.
     *  Then, the actor needs to explicitly call {@link #accept(Actor)} to the completion.
     * <p>
     *  This message might be called multiple times for a same actor
     *    because of multiple actors specifying the same actor as the next stage.
     *    Then, the actor can check completion by using {@link StagingTask#getKey()}.
     *    <pre>
     *      class A extends ActorDefault {
     *         Object lastCompleted;
     *        ...
     *          .match(StageCompleted.class, c -> {
     *              if (!Objects.equals(lastCompleted, c.getTask().getKey())) {
     *                  lastCompleted = c.getTask().getKey();
     *                  //doing completion task...
     *              }
     *              c.accept(this);
     *          })
     *    </pre>
     */
    public static class StagingCompleted implements CallableMessage.CallableMessageConsumer<Actor>, Serializable {
        public static final long serialVersionUID = 1L;
        protected StagingTask task;
        protected Object sender;
        protected ActorRef completedActor;
        protected volatile Instant completedTime;

        public StagingCompleted(StagingTask task, ActorRef completedActor, Object sender) {
            this.task = task;
            this.completedActor = completedActor;
            this.sender = sender;
            completedTime = Instant.EPOCH;
        }

        public void setCompletedTime(Instant completedTime) {
            this.completedTime = completedTime;
        }

        public Instant getCompletedTime() {
            return completedTime;
        }

        public Duration getElapsedTime() {
            Instant start = task.getStartTime();
            if (completedTime.equals(Instant.EPOCH)) {
                return Duration.between(start, Instant.now());
            } else {
                return Duration.between(start, completedTime);
            }
        }

        public StagingTask getTask() {
            return task;
        }

        public ActorRef getCompletedActor() {
            return completedActor;
        }

        @Override
        public void accept(Actor self) {
            if (self instanceof StagingSupported) {
                redirectTo(self, ((StagingSupported) self).nextStageActors(), true);
            } else {
                sendToTerminal();
            }
        }

        /**
         * Calls of the method can be bundled by withCompleteThis=false
         * <pre>
         *     c.redirectTo(sender, actors1, false);
         *     c.redirectTo(sender, actors2, false);
         *     c.sendCompleteThisToTerminal()
         * </pre>
         *
         * @param sender the sender object: grouping starting tasks
         * @param nextActors actors observed by {@link StagingWatcher}
         * @param withCompleteThis if true, it calls {@link #sendCompleteThisToTerminal()}
         */
        public void redirectTo(Object sender, Iterable<? extends ActorRef> nextActors, boolean withCompleteThis) {
            ArrayList<ActorRef> refs = new ArrayList<>();
            for (ActorRef a : nextActors) {
                if (a != null) {
                    refs.add(a);
                }
            }
            //first, notify start of N actors: the task is started by self
            task.getTerminalActor().tell(new StagingNotification(task, sender, true, refs.size(), completedActor));

            if (withCompleteThis) {
                sendCompleteThisToTerminal(); //second, finish this task
            }
            //actually starts next stage
            if (!refs.isEmpty()) {
                refs.forEach(a ->
                        a.tell(new StagingWatcher(task, sender)));
            } else {
                task.getTerminalActor().tell(this);
            }
        }

        public void sendCompleteThisToTerminal() {
            task.getTerminalActor().tell(new StagingNotification(task, sender, false, 1, completedActor));
        }

        public void sendToTerminal() {
            sendCompleteThisToTerminal();
            task.getTerminalActor().tell(this);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" +
                    "task=" + task +
                    ", sender=" + sender +
                    ", completedActor=" + completedActor +
                    ")";
        }
    }

    /**
     * interface for actors
     */
    public interface StagingSupported {
        default Iterable<? extends ActorRef> nextStageActors() {
            ActorRef ref = nextStageActor();
            if (ref == null) {
                return Collections.emptyList();
            } else {
                return Collections.singletonList(ref);
            }
        }

        default ActorRef nextStageActor() {
            return null;
        }

        /**
         * optional operation
         * @param ref the next stage reference
         */
        default void setNextStage(ActorRef ref) { }
    }

    public static class StagingNotification implements Serializable {
        public static final long serialVersionUID = 1L;
        protected StagingTask task;
        protected Object sender;
        protected boolean start; //or complete
        protected int taskCount;
        protected ActorRef actor;

        public StagingNotification(StagingTask task, Object sender, boolean start, int taskCount, ActorRef actor) {
            this.task = task;
            this.sender = sender;
            this.start = start;
            this.taskCount = taskCount;
            this.actor = actor;
        }

        public Object getSender() {
            return sender;
        }

        public StagingTask getTask() {
            return task;
        }

        public boolean isStart() {
            return start;
        }

        public int getTaskCount() {
            return taskCount;
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" +
                    "task=" + task +
                    ", sender=" + sender +
                    ", " + (start ? "started" : "completed") +
                    ", taskCount=" + taskCount +
                    ')';
        }
    }
    ///////////

    public static class StagingEntry {
        protected StagingTask task;
        protected AtomicLong started;
        protected AtomicLong finished;
        protected Instant completedTime;
        protected CompletableFuture<StagingCompleted> future;

        protected Set<ActorRef> completedActors;

        public StagingEntry(StagingTask task) {
            this.task = task;
            this.started = new AtomicLong();
            this.finished = new AtomicLong();
            this.completedTime = Instant.EPOCH;
        }

        public StagingTask getTask() {
            return task;
        }

        public long addStarted(int n) {
            return started.addAndGet(n);
        }

        public long addFinished(int n) {
            return finished.addAndGet(n);
        }

        public long getFinished() {
            return finished.get();
        }

        public long getStarted() {
            return started.get();
        }

        public synchronized void setCompletedTime(Instant time) {
            this.completedTime = time;
        }

        public synchronized void complete(StagingCompleted c) {
            future().complete(c);
        }

        public synchronized CompletableFuture<StagingCompleted> future() {
            if (future == null) {
                future = new CompletableFuture<>();
            }
            return future;
        }

        public Instant getCompletedTime() {
            return completedTime;
        }

        public synchronized void addCompletedActor(ActorRef actor) {
            if (completedActors == null) {
                completedActors = new HashSet<>();
            }
            completedActors.add(actor);
        }

        public synchronized Set<ActorRef> getCompletedActors() {
            return new HashSet<>(completedActors);
        }
    }

    public void notified(StagingNotification notification) {
        StagingEntry e = getEntry(notification.getTask());
        if (e == null) {
            return;
        }
        long started;
        long finished;
        int added = notification.getTaskCount();
        if (notification.isStart()) {
            started = e.addStarted(added);
            finished = e.getFinished();
        } else {
            started = e.getStarted();
            finished = e.addFinished(added);
        }
        if (isRecordCompletedActors()) {
            e.addCompletedActor(notification.getActor());
        }
        if (added != 0) {
            notified(notification, started, finished);
        }
    }

    public void notified(StagingNotification notification, long started, long finished) {
        StagingTask task = notification.getTask();
        if (isLogging(task, started, finished)) {
            log(task, started, finished, "" + notification.getSender());
        }
    }

    public boolean isLogging(StagingTask task, long started, long finished) {
        Instant now = Instant.now();
        Duration d = Duration.between(logLastTime, now);
        if (d.compareTo(logDuration) >= 0) {
            logLastTime = now;
            return true;
        } else {
            return false;
        }
    }

    public void log(StagingTask task, long started, long finished, String msg) {
        double d = 100 * (started == 0 ? 0 : (finished / (double) started));
        log(String.format("%s [%,3d/%,3d (%3.0f%%)] : %s", task, finished, started, d, msg));
    }

    protected void log(String str) {
        getSystem().getLogger().log("%s", str);
    }

    public StagingEntry getEntry(StagingTask task) {
        if (entry.getTask().getKey().equals(task.getKey())) {
            return entry;
        } else {
            return null;
        }
    }

    public void complete(StagingCompleted completed) {
        StagingEntry e = getEntry(completed.getTask());
        if (e == null) {
            return;
        }
        if (e.getFinished() >= e.getStarted()) {
            completedActually(e, completed);
        }
    }

    public void completedActually(StagingEntry e, StagingCompleted completed) {
        e.setCompletedTime(Instant.now());
        completed.setCompletedTime(e.getCompletedTime());

        log(e.getTask(), e.getStarted(), e.getFinished(),
                String.format("FINISH %-11s (%s)", completed.getElapsedTime(), ActorSystem.timeForLog(completed.getCompletedTime())));
        if (handler != null) {
            handler.accept(this, completed);
        }
        if (isRecordCompletedActors()) {
            runParticipantsHandlers(e.getCompletedActors());
        }
        e.complete(completed);
        if (systemClose) {
            getSystem().close();
        }
    }

    //// completion handler for each participants

    protected List<CompletionHandlerForActor> participantsHandler = new ArrayList<>();

    public boolean isRecordCompletedActors() {
        return !participantsHandler.isEmpty();
    }

    public void runParticipantsHandlers(Set<ActorRef> completed) {
        CompletionHandlerTask task = new CompletionHandlerTask(participantsHandler);
        List<CompletableFuture<?>> fs = new ArrayList<>(completed.size());
        for (ActorRef a : completed) {
            fs.add(ResponsiveCalls.sendTaskConsumer(system, a, task));
        }
        try {
            CompletableFuture.allOf(fs.toArray(new CompletableFuture<?>[0])).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class CompletionHandlerTask implements CallableMessage.CallableMessageConsumer<Actor> {
        public static final long serialVersionUID = 1L;
        protected List<CompletionHandlerForActor> handlers;

        public CompletionHandlerTask(List<CompletionHandlerForActor> handlers) {
            this.handlers = handlers;
        }

        @Override
        public void accept(Actor self, ActorRef sender) {
            for (CompletionHandlerForActor h : handlers) {
                if (h.handle(self, sender)) {
                    break;
                }
            }
        }

        @Override
        public void accept(Actor self) {
            accept(self, null);
        }
    }

    public static class CompletionHandlerForActor implements Serializable {
        public static final long serialVersionUID = 1L;
        protected Class<?> actorType;
        protected CallableMessage.CallableMessageConsumer<Actor> handler;

        public CompletionHandlerForActor(Class<?> actorType, CallableMessage.CallableMessageConsumer<Actor> handler) {
            this.actorType = actorType;
            this.handler = handler;
        }

        public boolean handle(Actor target, ActorRef sender) {
            if (actorType.isInstance(target)) {
                handler.accept(target, sender);
                return true;
            } else {
                return false;
            }
        }
    }
}
