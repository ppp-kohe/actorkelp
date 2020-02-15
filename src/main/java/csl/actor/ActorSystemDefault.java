package csl.actor;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ActorSystemDefault implements ActorSystem {
    protected PollTime time;
    protected boolean shutdown;
    protected int threads;
    protected ExecutorService executorService;
    protected volatile ScheduledExecutorService scheduledExecutor;
    protected AtomicInteger processingCount;
    protected int throughput;
    protected Map<String, Actor> namedActorMap;

    public ActorSystemDefault() {
        initSystem();
    }

    @Override
    public int getThreads() {
        return threads;
    }

    public int getThroughput() {
        return throughput;
    }

    public PollTime getTime() {
        return time;
    }

    @Override
    public String toString() {
        return toStringSystemName() + "@" + Integer.toHexString(System.identityHashCode(this));
    }

    public String toStringSystemName() {
        return "system";
    }

    protected void initSystem() {
        shutdown = false;
        initThroughput();
        initSystemTime();
        initSystemThreads();
        initSystemExecutorService();
        initSystemProcessingCount();
        initNamedActorMap();
    }
    protected void initThroughput() {
        this.throughput = 5;
    }
    protected void initSystemTime() {
        time = new PollTime(1, TimeUnit.SECONDS);
    }
    protected void initSystemThreads() {
        threads = Runtime.getRuntime().availableProcessors();
    }
    protected void initSystemExecutorService() {
        executorService = Executors.newFixedThreadPool(threads);
    }
    protected void initSystemProcessingCount() {
        processingCount = new AtomicInteger();
    }

    protected void initNamedActorMap() {
        this.namedActorMap = new ConcurrentHashMap<>();
    }

    @Override
    public void send(Message<?> message) {
        Actor targetActor = resolveActor(message.getTarget());
        if (targetActor != null) {
            startProcessMessageSubsequently(targetActor, message);
        } else {
            sendDeadLetter(message);
        }
    }

    public void sendDeadLetter(Message<?> message) {
        if (message.getSender() != null) {
            message.getSender().tell(toDeadLetter(message));
        } else {
            System.err.println("DEAD-LETTER " + message);
        }
    }

    protected Object toDeadLetter(Message<?> message) {
        return new DeadLetter(message);
    }

    public void startProcessMessageSubsequently(Actor target, Message<?> message) {
        if (!(message instanceof Message.MessageNone)) {
            target.offer(message);
        }
        if (!target.processMessageLocked()) { //the guard is experimentally inserted for reducing executorService's backlogs
            // it needs to isEmptyMailbox() at (B) for the (A)->(B) & remainingMessages=false case
            try {
                execute(() -> processMessageSubsequently(target));
            } catch (RejectedExecutionException re) {
                processMessageRejected(target);
            }
        }
    }

    protected void processMessageSubsequently(Actor actor) {
        if (actor.processMessageBefore()) {
            processingCount.incrementAndGet();
            boolean remainingMessages = false;
            try {
                for (int i = 0; isProcessContinue(i); ++i) {
                    remainingMessages = actor.processMessageNext();
                    if (!remainingMessages) {
                        break;
                    }
                }
            } finally { //(A)
                actor.processMessageAfter();
                if (remainingMessages || !actor.isEmptyMailbox()) { //(B)
                    try {
                        execute(() -> processMessageSubsequently(actor));
                    } catch (RejectedExecutionException re) { //shutdown
                        processMessageRejected(actor);
                    }
                }
                processingCount.decrementAndGet();
            }
        }
    }

    protected void processMessageRejected(Actor actor) {
        if (!actor.isEmptyMailbox()) {
            System.err.println(String.format("remaining-messages for actor after shut-down: %s", actor));
        }
    }

    protected boolean isProcessContinue(int messageCount) {
        return !shutdown && messageCount < throughput;
    }

    public Actor resolveActor(ActorRef ref) {
        if (ref instanceof Actor) {
            return (Actor) ref;
        } else if (ref instanceof ActorRefLocalNamed) {
            return resolveActorLocalNamed((ActorRefLocalNamed) ref);

        } else {
            return null; //TODO
        }
    }

    public void register(Actor actor) {
        String name = actor.getName();
        if (name != null) {
            namedActorMap.put(name, actor);
        }
    }

    public void unregister(String actorName) {
        namedActorMap.remove(actorName);
    }

    public Actor resolveActorLocalNamed(ActorRefLocalNamed ref) {
        if (ref instanceof ActorRefLocalNamed.ActorRefLocalNamedNoName) {
            System.err.println(String.format("resolveActorLocalNamed error: %s", ref));
            return null;
        } else {
            return namedActorMap.get(ref.getName());
        }
    }

    /** @return implementation field getter */
    public Map<String, Actor> getNamedActorMap() {
        return namedActorMap;
    }

    /** @return implementation field getter */
    public ExecutorService getExecutorService() {
        return executorService;
    }

    /** @return implementation field getter */
    public AtomicInteger getProcessingCount() {
        return processingCount;
    }

    @Override
    public void execute(Runnable task) {
        executorService.execute(task);
    }

    @Override
    public void close() {
        shutdown = true;
        executorService.shutdownNow();
        ScheduledExecutorService se = scheduledExecutor;
        if (se != null) {
            se.shutdownNow();
        }
    }

    public ScheduledExecutorService getScheduledExecutor() {
        ScheduledExecutorService se = scheduledExecutor;
        if (se == null) {
            synchronized (this) {
                if (scheduledExecutor == null) {
                    scheduledExecutor = Executors.newScheduledThreadPool(getScheduledExecutorThreads());
                }
                se = scheduledExecutor;
            }
        }
        return se;
    }

    protected int getScheduledExecutorThreads() {
        return Math.max(1, getThreads() / 2);
    }

    public static class PollTime {
        protected long value;
        protected TimeUnit unit;

        public PollTime(long value, TimeUnit unit) {
            this.value = value;
            this.unit = unit;
        }

        public long getValue() {
            return value;
        }

        public TimeUnit getUnit() {
            return unit;
        }

        @Override
        public String toString() {
            return "time(" + value + ", " + unit + ")";
        }
    }

    public static class DeadLetter implements Serializable {
        protected Message<?> message;

        public DeadLetter(Message<?> message) {
            this.message = message;
        }

        public Message<?> getMessage() {
            return message;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + '(' + message + ')';
        }
    }
}
