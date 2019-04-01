package csl.actor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ActorSystemDefault implements ActorSystem {
    protected PollTime time;
    protected boolean shutdown;
    protected int threads;
    protected ExecutorService executorService;
    protected AtomicInteger processingCount;
    protected int throughput;
    protected Map<String, Actor> namedActorMap;

    public ActorSystemDefault() {
        initSystem();
    }

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
        return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this));
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
        System.err.println("DEAD-LETTER " + message);
    }

    public void startProcessMessageSubsequently(Actor target, Message<?> message) {
        target.getMailbox().offer(message);
        execute(() -> processMessageSubsequently(target));
    }

    protected void processMessageSubsequently(Actor actor) {
        processMessageSubsequently(actor, null);
    }

    protected void processMessageSubsequently(Actor actor, Runnable postProcess) {
        try {
            processingCount.incrementAndGet();
            for (int i = 0; isProcessContinue(i); ++i) {
                Message<?> message = actor.getMailbox().poll();
                if (message != null) {
                    startProcessMessage(actor, message);
                } else {
                    break;
                }
            }
            if (postProcess != null) {
                postProcess.run();
            }
        } finally {
            processingCount.decrementAndGet();
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

    public Actor resolveActorLocalNamed(ActorRefLocalNamed ref) {
        return namedActorMap.get(ref.getName());
    }

    public void startProcessMessage(Actor targetActor, Message<?> message) {
        execute(() -> targetActor.offerAndProcess(message));
    }

    @Override
    public void execute(Runnable task) {
        executorService.execute(task);
    }

    public void stop() {
        shutdown = true;
        executorService.shutdownNow();
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
}
