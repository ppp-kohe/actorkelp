package csl.actor;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Arrays;
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
        Object data = message.getData();
        ActorRef sender;
        if (data instanceof Message.MessageDataPacket<?> &&
                (sender = ((Message.MessageDataPacket<?>) data).getSender()) != null) {
            sender.tell(toDeadLetter(message));
        } else {
            getLogger().log("DEAD-LETTER %s", message);
        }
    }

    protected Object toDeadLetter(Message<?> message) {
        return new DeadLetter(message);
    }

    public void startProcessMessageSubsequently(Actor target, Message<?> message) {
        if (message instanceof Message.MessageNone) { //launch both
            startProcessMessageSubsequently(createProcessMessageSubsequently(target, true, message), message);
            startProcessMessageSubsequently(createProcessMessageSubsequently(target, false, message), message);
        } else {
            boolean special = isSpecialMessage(message);
            startProcessMessageSubsequently(createProcessMessageSubsequently(target, special, message), message);
        }
    }

    protected ProcessMessageSubsequently createProcessMessageSubsequently(Actor target, boolean special, Message<?> msg) {
        return new ProcessMessageSubsequently(this, target, special);
    }

    public void startProcessMessageSubsequently(ProcessMessageSubsequently task, Message<?> message) {
        task.actor.activate(task.isSpecial());
        if (!(message instanceof Message.MessageNone)) {
            task.actor.offer(message);
        }
        if (task.isSpecial() ?
                !task.actor.processMessageLockedSpecial() :
                !task.actor.processMessageLocked()) { //the guard is experimentally inserted for reducing executorService's backlogs
            // it needs to isEmptyMailbox() at (B) for the (A)->(B) & remainingMessages=false case
            try {
                task.submit();
            } catch (RejectedExecutionException re) {
                processMessageRejected(task.actor);
            }
        }
    }

    protected void processMessageSubsequently(ProcessMessageSubsequently task) {
        Actor actor = task.actor;
        if (task.isSpecial() ?
                actor.processMessageBeforeSpecial() :
                actor.processMessageBefore()) {
            processingCount.incrementAndGet();
            boolean remainingMessages = false;
            try {
                for (int i = 0; isProcessContinue(i); ++i) {
                    remainingMessages = task.isSpecial() ?
                            actor.processMessageNextSpecial() :
                            actor.processMessageNext();
                    if (!remainingMessages) {
                        break;
                    }
                }
            } finally { //(A)
                if (task.isSpecial()) {
                    actor.processMessageAfterSpecial();
                } else {
                    actor.processMessageAfter();
                }
                if (remainingMessages || !actor.isEmptyMailboxAll()) { //(B)
                    try {
                        task.submit();
                    } catch (RejectedExecutionException re) { //shutdown
                        processMessageRejected(actor);
                    }
                } else {
                    actor.deactivate(task.isSpecial());
                }
                processingCount.decrementAndGet();
            }
        }
    }

    public static class ProcessMessageSubsequently implements Runnable {
        public ActorSystemDefault system;
        public Actor actor;
        protected boolean special;

        public ProcessMessageSubsequently(ActorSystemDefault system, Actor actor, boolean special) {
            this.system = system;
            this.actor = actor;
            this.special = special;
        }

        public boolean isSpecial() {
            return special;
        }

        @Override
        public void run() {
            try {
                system.processMessageSubsequently(this);
            } catch (Throwable ex) {
                ex.printStackTrace();
            }
        }

        public void submit() {
            system.execute(this);
        }
    }

    protected void processMessageRejected(Actor actor) {
        if (!actor.isEmptyMailboxAll()) {
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
    public boolean register(Actor actor) {
        String name = actor.getName();
        if (name != null) {
            return namedActorMap.put(name, actor) == null;
        } else {
            return false;
        }
    }

    @Override
    public boolean unregister(Actor actor) {
        return namedActorMap.remove(actor.getName(), actor);
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
                    scheduledExecutor = initScheduledExecutor();
                }
                se = scheduledExecutor;
            }
        }
        return se;
    }

    protected ScheduledExecutorService initScheduledExecutor() {
        return Executors.newScheduledThreadPool(getScheduledExecutorThreads());
    }

    protected int getScheduledExecutorThreads() {
        return Math.max(1, getThreads() / 2);
    }

    @Override
    public void awaitClose(long time, TimeUnit unit) throws InterruptedException {
        executorService.awaitTermination(time, unit);
    }

    public static class DeadLetter implements Serializable, Message.MessageData {
        public static final long serialVersionUID = 1L;
        public Message<?> message;

        public DeadLetter() {}

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
        public void log(String fmt, Object... args) {
            System.err.printf(fmt + "%n", args);
        }

        @Override
        public void log(int color, String fmt, Object... args) {
            System.err.println(toColorFormat(color, fmt, args));
        }

        @Override
        public void log(boolean flag, int color, String fmt, Object... args) {
            if (flag) {
                log(color, fmt, args);
            }
        }

        public String toColorFormat(int color, String fmt, Object... args) {
            if (color >= 0) {
                return String.format("\033[38;5;" + color + "m" + fmt + "\033[0m", args);
            } else {
                return String.format(fmt, args);
            }
        }

        public String toColorLine(int color, String line) {
            return toColorFormat(color, "%s", line);
        }

        @Override
        public void log(boolean flag, int color, Throwable ex, String fmt, Object... args) {
            if (flag) {
                StringWriter sw = new StringWriter();
                PrintWriter w = new PrintWriter(sw);
                ex.printStackTrace(w);
                w.close();
                String line = sw.getBuffer().toString();
                log(color, "%s %s", String.format(fmt, args), line);
            }
        }

    }
}
