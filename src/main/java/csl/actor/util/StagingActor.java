package csl.actor.util;

import csl.actor.*;
import csl.actor.remote.ActorSystemRemote;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * The actor managing computation stages of actor processing
 * <pre>
 *     Duration d = StagingActor.staging(system)
 *         .withStartTime(instant)
 *         .withId(filePath)
 *         .withHandler(MyActor.class, (self) -> completionTask)
 *         .start(topActor)
 *         .get().getElapsedTime();
 * </pre>
 *
 * <pre>
 * Example:
 * actor stages:
 * *  a1 -&gt; {a2, a3} -&gt;  a4
 *    * a1.nextStageActors() : {a2,a3}
 *    * a2.nextStageActors() : {a4}
 *    * a3.nextStageActors() : {a4}
 *
 * StagingActor s:
 *    start(a1)
 *       -&gt; StagingCompleted(...).redirectTo(...,{a1},false)
 *          -&gt; s.tell(StagingNotification(...,true, 1, null)) //started=1
 *             a1.tell(StagingWatcher(...))
 *
 * a1:
 *     receive StagingWatcher w1
 *       -&gt; w1.accept(a1)
 *            -&gt; w1.retry(a1,...) ... //offered to the delayed mailbox of a1
 *            -&gt; if the mailbox of a1 becomes empty,
 *                w1.completed(a1)
 *                 -&gt; a1.tell(StagingCompleted(...,a1,...))
 *                     -&gt; receive StagingCompleted c1
 *                          -&gt; c1.redirectTo(...,{a2,a3},true)
 *                             -&gt; s.tell(StagingNotification(...,true,2,a1)) //start of a2,a3 : started=3
 *                                s.tell(StagingNotification(...,false,1,a1)) //end of a1     : finishd=1
 *                                a2.tell(StagingWatcher(...))
 *                                a3.tell(StagingWatcher(...))
 *
 * a2,a3:
 *    //almost same as a1
 *      ... s.tell(StagingNotification(...,true,1,a4))...
 *      ... s.tell(StagingNotification(...,true,1,a4))... //started=5, finished=3
 *
 *    //2 StagingWatchers are sent to a4
 * a4:
 *     recieve StagingWatcher w4_1, w4_2
 *        ... -&gt; w4_1.completed(a4)
 *               w4_2.completed(a4)
 *                 ... -&gt; receive StagingCompleted c4_1  //started=5, finished=4
 *                         -&gt; s.tell(StagingNotification(...,false,1,a4))
 *                            s.tell(c4_1)
 *                              -&gt; s.completed(c4_1)
 *                                  -&gt; finshed:4 < started:5, nothing happen
 *                 ... -&gt; receive StagingCompleted c4_2  //started=5, finished=5
 *                         -&gt; s.tell(StagingNotification(...,false,1,a4))
 *                            s.tell(c4_2)
 *                              -&gt; s.completed(c4_2)
 *                                  -&gt; finshed:5 &gt;= started:5,
 *                                     -&gt; launchComplete():true
 *                                         -&gt; s.completedActually(...,c4_2)
 *                                          -&gt; s.runParticipantsHandlers(...,c4_2)
 *                                           -&gt;  a1.tell(CompletionHandlerTask(...s,0,c4_2))
 *                                               a2.tell(CompletionHandlerTask(...s,1,c4_2))
 *                                               a3.tell(CompletionHandlerTask(...s,2,c4_2))
 *                                               a4.tell(CompletionHandlerTask(...s,3,c4_2))
 * a1,...,a4:
 *   receive CompletionHandlerTask h1,...,h4
 *     -&gt;  hN.accept(aN)
 *       -&gt; executes handler
 *          s.tell(StagingHandlerCompleted(...,c4_2))
 *            -&gt; s.completeHandler(StagingHandlerCompleted(...))
 *              ...
 *               -&gt; completedHandlers &gt;= completedActorSize,
 *                   s.completedActuallyHandler(c4_2)
 *                    -&gt; future.complete(c4_2)
 *
 *
 *
 * </pre>
 */
public class StagingActor extends ActorDefault {
    protected UUID id;
    protected StagingEntry entry;
    protected BiConsumer<StagingActor, StagingCompleted> handler;
    protected List<CompletionHandlerForActor> participantsHandler = new ArrayList<>();
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
        participantsHandler.add(new CompletionHandlerForActor(actorType,
                (self) -> handler.accept(actorType.cast(self))));
        return this;
    }

    @FunctionalInterface
    public interface CallConsumerI<ActorType> extends Serializable {
        void accept(ActorType type);
    }

    ////starting

    public CompletableFuture<StagingCompleted> start(ActorRef target) {
        return startActors(Collections.singletonList(target));
    }

    public CompletableFuture<StagingCompleted> startActors(Iterable<? extends ActorRef> targets) {
        //create temporary completed
        new StagingCompleted(entry.getTask(), null, this)
                .redirectTo(this, targets, false);
        return entry.future();
    }

    ////

    @Override
    protected ActorBehavior initBehavior() {
        return behaviorBuilder()
                .match(StagingNotification.class, this::notified)
                .match(StagingCompleted.class, this::complete)
                .match(StagingHandlerCompleted.class, this::completeHandler)
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

        public StagingTask() {}

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
    public static class StagingWatcher implements CallableMessage.CallableMessageConsumer<Actor>, Cloneable, ActorSystemRemote.SpecialMessage {
        public static final long serialVersionUID = 1L;
        protected StagingTask task;
        protected Object sender;
        protected int count;

        public StagingWatcher() {}

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
     * the completion handling message.
     *  An actor can handle this message manually for doing special actions at stage-completion.
     *  Then, the actor needs to explicitly call {@link #accept(Actor)} to the completion.
     *  (For completion task, {@link StagingActor#withHandler(Class, CallableMessageConsumer)} is suitable)
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
     *
     */
    public static class StagingCompleted implements CallableMessage.CallableMessageConsumer<Actor>, Serializable, ActorSystemRemote.SpecialMessage {
        public static final long serialVersionUID = 1L;
        protected StagingTask task;
        protected Object sender;
        protected ActorRef completedActor;
        protected volatile Instant completedTime;

        public StagingCompleted() {}

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

    /**
     * The notification message sent back to {@link StagingActor}
     *   from each participants.
     */
    public static class StagingNotification implements Serializable, ActorSystemRemote.SpecialMessage {
        public static final long serialVersionUID = 1L;
        protected StagingTask task;
        protected Object sender;
        protected boolean start; //or complete
        protected int taskCount;
        protected ActorRef actor;

        public StagingNotification() { }

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
        protected AtomicBoolean completedLaunched = new AtomicBoolean();

        protected Set<ActorRef> completedActors;
        protected AtomicInteger completedActorSize;
        protected AtomicInteger completedHandlers;

        public StagingEntry(StagingTask task) {
            this.task = task;
            this.started = new AtomicLong();
            this.finished = new AtomicLong();
            this.completedTime = Instant.EPOCH;
            this.completedActorSize = new AtomicInteger();
            this.completedHandlers = new AtomicInteger();
        }

        public StagingTask getTask() {
            return task;
        }

        /**
         * @param n the number of stated tasks
         * @return total started tasks including the n
         */
        public long addStarted(int n) {
            return started.addAndGet(n);
        }

        /**
         * @param n the number of finished tasks
         * @return total finished tasks including the n
         */
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

        /**
         * complete the {@link #future()} object
         * @param c the completion object
         */
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

        /**
         * add an participant actor to the completed actors set.
         *  it increments {@link #getCompletedActorSize()}
         * @param actor the completed participant
         */
        public synchronized void addCompletedActor(ActorRef actor) {
            if (completedActors == null) {
                completedActors = new HashSet<>();
            }
            if (completedActors.add(actor)) {
                completedActorSize.incrementAndGet();
            }
        }

        public synchronized Set<ActorRef> getCompletedActors() {
            return new HashSet<>(completedActors);
        }

        public int getCompletedActorSize() {
            return completedActorSize.get();
        }

        /**
         * checks the launch of completion handlers
         * @return true if successfully launched
         */
        public boolean launchComplete() {
            return completedLaunched.compareAndSet(false, true);
        }

        /**
         * increments the number of handler completed actors
         * @return the total number of completed handlers
         */
        public int addCompletedHandler() {
            return completedHandlers.incrementAndGet();
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
        if (needsToRecordCompletedActors() && notification.getActor() != null) {
            e.addCompletedActor(notification.getActor()); //note: the actor might not be the completed actor
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
            if (e.launchComplete()) {
                completedActually(e, completed);
            }
        }
    }

    public void completedActually(StagingEntry e, StagingCompleted completed) {
        e.setCompletedTime(Instant.now());
        completed.setCompletedTime(e.getCompletedTime());

        if (needsToRecordCompletedActors()) { //i.e. it has participants handlers
            runParticipantsHandlers(e, completed);
        } else {
            completedActuallyHandler(completed);
        }
    }

    public void completeHandler(StagingHandlerCompleted completed) {
        StagingEntry e = getEntry(completed.getCompleted().getTask());
        int finished = e.addCompletedHandler();
        int started = e.getCompletedActorSize();
        StagingTask task = completed.getCompleted().getTask();
        if (finished >= started) {
            log(task, started, finished, String.format("completed handler : [%,d] %s", completed.getHandlerIndex(), completed.getTarget()));
            completedActuallyHandler(completed.getCompleted());
        } else {
            if (isLogging(task, started, finished)) {
                log(task, started, finished, String.format("completed handler : [%,d] %s", completed.getHandlerIndex(), completed.getTarget()));
            }
        }
    }

    public void completedActuallyHandler(StagingCompleted completed) {
        StagingEntry e = getEntry(completed.getTask());
        log(e.getTask(), e.getStarted(), e.getFinished(),
                String.format("FINISH %-11s (%s)", completed.getElapsedTime(), ActorSystem.timeForLog(completed.getCompletedTime())));
        if (handler != null) {
            handler.accept(this, completed);
        }
        e.complete(completed);
        if (systemClose) {
            getSystem().close();
        }
    }

    public static class StagingHandlerCompleted implements Serializable, ActorSystemRemote.SpecialMessage {
        public static final long serialVersionUID = 1L;
        protected ActorRef target;
        protected int handlerIndex;
        protected StagingCompleted completed;

        public StagingHandlerCompleted() {}

        public StagingHandlerCompleted(ActorRef target, int handlerIndex, StagingCompleted completed) {
            this.target = target;
            this.handlerIndex = handlerIndex;
            this.completed = completed;
        }

        public ActorRef getTarget() {
            return target;
        }

        public int getHandlerIndex() {
            return handlerIndex;
        }

        public StagingCompleted getCompleted() {
            return completed;
        }
    }

    //// completion handler for each participants


    public boolean needsToRecordCompletedActors() {
        return !participantsHandler.isEmpty();
    }

    public void runParticipantsHandlers(StagingEntry e, StagingCompleted completed) {
        Set<ActorRef> completedActors = e.getCompletedActors();
        if (completedActors.isEmpty()) {
            completedActuallyHandler(completed);
        } else {
            log(completed.getTask(), e.getStarted(), e.getFinished(), String.format("start handlers for %,d actors", completedActors.size()));
            int i = 0;
            for (ActorRef a : completedActors) {
                a.tell(new CompletionHandlerTask(this.participantsHandler, this, i, completed));
                ++i;
            }
        }
    }

    public static class CompletionHandlerTask implements CallableMessage.CallableMessageConsumer<Actor>, ActorSystemRemote.SpecialMessage {
        public static final long serialVersionUID = 1L;
        protected ActorRef stagingActor;
        protected int index;
        protected StagingCompleted completed;
        protected List<CompletionHandlerForActor> handlers;

        public CompletionHandlerTask() {}

        public CompletionHandlerTask(List<CompletionHandlerForActor> handlers,
                                     ActorRef stagingActor, int index, StagingCompleted completed) {
            this.handlers = handlers;
            this.stagingActor = stagingActor;
            this.index = index;
            this.completed = completed;
        }

        @Override
        public void accept(Actor self, ActorRef sender) {
            try {
                for (CompletionHandlerForActor h : handlers) {
                    if (h.handle(self, sender)) {
                        break;
                    }
                }
            } finally {
                stagingActor.tell(new StagingHandlerCompleted(self, index, completed));
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

        public CompletionHandlerForActor() {}

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
