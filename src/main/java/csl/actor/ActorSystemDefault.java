package csl.actor;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ActorSystemDefault implements ActorSystem {
    protected boolean shutdown;
    protected int threads;
    protected ExecutorService executorService;
    protected volatile ScheduledExecutorService scheduledExecutor;
    protected AtomicInteger processingCount;
    protected int throughput;
    protected Map<String, Actor> namedActorMap;
    protected SystemLogger logger;

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

    @Override
    public String toString() {
        return toStringSystemName() + "@" + Integer.toHexString(System.identityHashCode(this));
    }

    public String toStringSystemName() {
        return "system";
    }

    protected void initSystem() {
        shutdown = false;
        initLogger();
        initThroughput();
        initSystemThreads();
        initSystemExecutorService();
        initSystemProcessingCount();
        initNamedActorMap();
    }
    protected void initLogger() {
        this.logger = new SystemLoggerErr();
    }
    protected void initThroughput() {
        this.throughput = 5;
    }
    protected void initSystemThreads() {
        threads = Runtime.getRuntime().availableProcessors();
    }
    protected void initSystemExecutorService() {
        executorService = createThreadPoolFixed(threads);
    }

    public static ExecutorService createThreadPoolUnlimited(int threads) {
        return new ThreadPoolExecutor(threads, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<>());
    }

    public static ExecutorService createThreadPoolFixed(int threads) {
        return Executors.newFixedThreadPool(threads * 6);
    }

    public static class ActorSystemDefaultUnlimited extends ActorSystemDefault {
        @Override
        protected void initSystemExecutorService() {
            executorService = ActorSystemDefault.createThreadPoolUnlimited(threads);
        }
    }

    protected void initSystemProcessingCount() {
        processingCount = new AtomicInteger();
    }

    protected void initNamedActorMap() {
        this.namedActorMap = new ConcurrentHashMap<>();
    }

    @Override
    public SystemLogger getLogger() {
        return logger;
    }

    public void setLogger(SystemLogger logger) {
        this.logger = logger;
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
            getLogger().log("DEAD-LETTER %s", message);
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
            getLogger().log("remaining-messages for actor after shut-down: %s", actor);
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

        } else if (ref != null) {
            return ref.asLocal();
        } else {
            return null;
        }
    }

    @Override
    public void register(Actor actor) {
        String name = actor.getName();
        if (name != null) {
            namedActorMap.put(name, actor);
        }
    }

    @Override
    public void unregister(Actor actor) {
        namedActorMap.remove(actor.getName(), actor);
    }

    @Override
    public Actor resolveActorLocalNamed(ActorRefLocalNamed ref) {
        if (ref instanceof ActorRefLocalNamed.ActorRefLocalNamedNoName) {
            getLogger().log("resolveActorLocalNamed error: %s", ref);
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

    @Override
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

    @Override
    public void awaitClose(long time, TimeUnit unit) throws InterruptedException {
        executorService.awaitTermination(time, unit);
    }

    public static class DeadLetter implements Serializable {
        public static final long serialVersionUID = 1L;
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

    public static class SystemLoggerErr implements SystemLogger {
        @Override
        public void log(boolean flag, int color, String fmt, Object... args) {
            if (flag) {
                System.err.println(toColorLine(color, String.format(fmt, args)));
            }
        }

        public String toColorLine(int color, String line) {
            if (color >= 0) {
                return String.format("\033[38;5;%dm%s\033[0m", color, line);
            } else {
                return line;
            }
        }

        @Override
        public void log(boolean flag, int color, Throwable ex, String fmt, Object... args) {
            if (flag) {
                StringWriter sw = new StringWriter();
                PrintWriter w = new PrintWriter(sw);
                ex.printStackTrace(w);
                w.close();
                String line = sw.getBuffer().toString();
                log(true, color, "%s %s", String.format(fmt, args), line);
            }
        }

    }
}
