package csl.actor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ActorSystemDefault implements ActorSystem {
    protected PollTime time;
    protected boolean shutdown;
    protected int threads;
    protected ExecutorService executorService;
    protected AtomicInteger processingCount;
    protected int throughput;

    public ActorSystemDefault() {
        initSystem();
    }

    protected void initSystem() {
        shutdown = false;
        initThroughput();
        initSystemTime();
        initSystemThreads();
        initSystemExecutorService();
        initSystemProcessingCount();
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

    @Override
    public void send(Message<?> message) {
        Actor targetActor = resolveActor(message.getTarget());
        if (targetActor != null) {
            startProcessMessageSubsequently(targetActor, message);
        } else {
            //TODO dead letter
        }
    }

    public void startProcessMessageSubsequently(Actor target, Message<?> message) {
        target.getMailbox().offer(message);
        executorService.submit(() -> processMessageSubsequently(target));
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
        } else {
            return null; //TODO
        }
    }

    public void startProcessMessage(Actor targetActor, Message<?> message) {
        executorService.submit(() -> targetActor.offerAndProcess(message));
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
