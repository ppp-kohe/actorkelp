package csl.actor.kelp;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.*;
import csl.actor.cluster.ActorSystemCluster;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.kelp.behavior.MailboxKelp;
import csl.actor.kelp.persist.PersistentConditionActor;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.util.ConfigBase;
import csl.actor.util.SampleTiming;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ActorSystemKelp extends ActorSystemRemote implements ActorKelpBuilder {
    protected Map<ActorAddress, ConnectionHost> hostMap;
    protected volatile ConfigKelp config = ConfigKelp.CONFIG_DEFAULT;

    protected ActorKelpInternalFactory internalFactory;

    @Override
    public ActorSystem system() {
        return this;
    }

    @Override
    public ConfigKelp config() {
        return config;
    }

    @Override
    public ActorKelpInternalFactory internalFactory() {
        return internalFactory;
    }

    public static ActorSystemKelp create(ConfigDeployment configDeployment) {
        Function<ActorSystem, Kryo> kryoFactory = configDeployment.kryoBuilder(defaultBuilderType());
        return create(configDeployment, kryoFactory);
    }

    public static ActorSystemKelp create(ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory) {
        return new ActorSystemKelp(new ActorSystemDefaultForKelp(configDeployment, kryoFactory), kryoFactory);
    }

    public static ActorSystemKelp create(ConfigDeployment configDeployment, ActorKelpInternalFactory internalFactory) {
        Function<ActorSystem, Kryo> kryoFactory = configDeployment.kryoBuilder(defaultBuilderType());
        return create(configDeployment, kryoFactory, internalFactory);
    }

    public static ActorSystemKelp create(ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory, ActorKelpInternalFactory internalFactory) {
        return new ActorSystemKelp(new ActorSystemDefaultForKelp(configDeployment, kryoFactory, internalFactory), kryoFactory);
    }

    public static ActorSystemKelp create(ConfigKelp config, ConfigDeployment configDeployment) {
        Function<ActorSystem, Kryo> kryoFactory = configDeployment.kryoBuilder(defaultBuilderType());
        return create(config, configDeployment, kryoFactory);
    }

    public static ActorSystemKelp create(ConfigKelp config, ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory) {
        return new ActorSystemKelp(new ActorSystemDefaultForKelp(config, configDeployment, kryoFactory), kryoFactory);
    }

    public static ActorSystemKelp create(ConfigKelp config, ConfigDeployment configDeployment, ActorKelpInternalFactory internalFactory) {
        Function<ActorSystem, Kryo> kryoFactory = configDeployment.kryoBuilder(defaultBuilderType());
        return create(config, configDeployment, kryoFactory, internalFactory);
    }

    public static ActorSystemKelp create(ConfigKelp config, ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory, ActorKelpInternalFactory internalFactory) {
        return new ActorSystemKelp(new ActorSystemDefaultForKelp(config, configDeployment, kryoFactory, internalFactory), kryoFactory);
    }

    //////////////////////

    public static ActorSystemDefaultForKelp createLocal() {
        return createLocal(new ConfigDeployment());
    }

    public static ActorSystemDefaultForKelp createLocal(ConfigDeployment configDeployment) {
        Function<ActorSystem, Kryo> kryoFactory = configDeployment.kryoBuilder(defaultBuilderType());
        return createLocal(configDeployment, kryoFactory);
    }

    public static ActorSystemDefaultForKelp createLocal(ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory) {
        return new ActorSystemDefaultForKelp(configDeployment, kryoFactory);
    }

    public static ActorSystemDefaultForKelp createLocal(ActorKelpInternalFactory internalFactory) {
        return createLocal(new ConfigDeployment(), internalFactory);
    }

    public static ActorSystemDefaultForKelp createLocal(ConfigDeployment configDeployment, ActorKelpInternalFactory internalFactory) {
        Function<ActorSystem, Kryo> kryoFactory = configDeployment.kryoBuilder(defaultBuilderType());
        return createLocal(configDeployment, kryoFactory, internalFactory);
    }

    public static ActorSystemDefaultForKelp createLocal(ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory, ActorKelpInternalFactory internalFactory) {
        return new ActorSystemDefaultForKelp(configDeployment, kryoFactory, internalFactory);
    }

    public static ActorSystemDefaultForKelp createLocal(ConfigKelp config) {
        return createLocal(config, new ConfigDeployment());
    }

    public static ActorSystemDefaultForKelp createLocal(ConfigKelp config, ConfigDeployment configDeployment) {
        Function<ActorSystem, Kryo> kryoFactory = configDeployment.kryoBuilder(defaultBuilderType());
        return createLocal(config, configDeployment, kryoFactory);
    }

    public static ActorSystemDefaultForKelp createLocal(ConfigKelp config, ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory) {
        return new ActorSystemDefaultForKelp(config, configDeployment, kryoFactory);
    }

    public static ActorSystemDefaultForKelp createLocal(ConfigKelp config, ActorKelpInternalFactory internalFactory) {
        return createLocal(config, new ConfigDeployment(), internalFactory);
    }

    public static ActorSystemDefaultForKelp createLocal(ConfigKelp config, ConfigDeployment configDeployment, ActorKelpInternalFactory internalFactory) {
        Function<ActorSystem, Kryo> kryoFactory = configDeployment.kryoBuilder(defaultBuilderType());
        return createLocal(config, configDeployment, kryoFactory, internalFactory);
    }

    public static ActorSystemDefaultForKelp createLocal(ConfigKelp config, ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory, ActorKelpInternalFactory internalFactory) {
        return new ActorSystemDefaultForKelp(config, configDeployment, kryoFactory, internalFactory);
    }

    //////////////////////

    public static Class<KryoBuilderKelp> defaultBuilderType() {
        return KryoBuilderKelp.class;
    }

    public ActorSystemKelp() {
        this(new ConfigDeployment());
    }

    public ActorSystemKelp(ConfigDeployment configDeployment) {
        this(new ActorSystemDefaultForKelp(new ConfigDeployment()), configDeployment.kryoBuilder(defaultBuilderType()));
    }

    public ActorSystemKelp(ActorSystemDefaultForKelp localSystem, Function<ActorSystem, Kryo> kryoFactory) {
        super(localSystem, kryoFactory);
        this.internalFactory = localSystem.internalFactory();
        this.config = localSystem.config();
    }

    public ExecutorService getMergerExecutors() {
        return ((ActorSystemDefaultForKelp) localSystem).getMergerExecutors();
    }

    public static ExecutorService getMergerExecutors(ActorSystem system) {
        if (system instanceof ActorSystemDefaultForKelp) {
            return ((ActorSystemDefaultForKelp) system).getMergerExecutors();
        } else if (system instanceof ActorSystemKelp) {
            return ((ActorSystemKelp) system).getMergerExecutors();
        } else {
            return null;
        }
    }

    public static class SystemLoggerKelp extends ConfigBase.SystemLoggerHeader {
        protected ConfigKelp configKelp;
        public SystemLoggerKelp(SystemLogger logger, ConfigBase configDeployment, ConfigKelp configKelp) {
            super(logger, configDeployment);
            this.configKelp = configKelp;
        }
        @Override
        protected ConfigBase.FormatAndArgs format(String fmt, Object... args) {
            return config.logMessageHeader().append(configKelp.logMessageHeaderCustom()).append(new ConfigBase.FormatAndArgs(fmt, args));
        }

        @Override
        public String toString() {
            return String.format("%s@%h(dep=<%s> kelp=<%s>, %s)",
                    getClass().getSimpleName(),
                    System.identityHashCode(this),
                    config.logMessageHeader().format(),
                    configKelp.logMessageHeaderCustom().format(),
                    logger);
        }
    }

    public static class ActorSystemDefaultForKelp extends ActorSystemCluster.ActorSystemDefaultForCluster
        implements ActorKelpBuilder {
        protected ConfigDeployment configDeployment;
        protected ExecutorService mergerExecutors;
        protected ConfigKelp config;
        protected ActorKelpInternalFactory internalFactory;
        protected AtomicReference<Double> memoryManagedActors = new AtomicReference<>(0.0);
        protected SystemLogger originalLogger;

        protected int updateTiming;
        protected AtomicLong updateCount = new AtomicLong();

        @Override
        public ActorSystem system() {
            return this;
        }

        @Override
        public ConfigKelp config() {
            if (config == null) {
                withConfig((ConfigKelp) configDeployment.createAppConfig(ConfigKelp.class));
            }
            return config;
        }

        @Override
        public ActorKelpInternalFactory internalFactory() {
            return internalFactory;
        }

        public ActorSystemDefaultForKelp() {
            this(new ConfigDeployment(), KryoBuilderKelp.builder());
        }

        public ActorSystemDefaultForKelp(ConfigDeployment configDeployment) {
            this(configDeployment, configDeployment.kryoBuilder(defaultBuilderType()));
        }

        public ActorSystemDefaultForKelp(ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory) {
            this(configDeployment, kryoFactory, ActorKelpInternalFactory.createFromConfig(configDeployment));
        }

        public ActorSystemDefaultForKelp(ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory, ActorKelpInternalFactory internalFactory) {
            this(null, configDeployment, kryoFactory, internalFactory);
        }

        public ActorSystemDefaultForKelp(ConfigKelp config) {
            this(config, new ConfigDeployment(), KryoBuilderKelp.builder());
        }

        public ActorSystemDefaultForKelp(ConfigKelp config, ConfigDeployment configDeployment) {
            this(config, configDeployment, configDeployment.kryoBuilder(defaultBuilderType()));
        }

        public ActorSystemDefaultForKelp(ConfigKelp config, ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory) {
            this(config, configDeployment, kryoFactory, ActorKelpInternalFactory.createFromConfig(configDeployment));
        }

        public ActorSystemDefaultForKelp(ConfigKelp config, ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory, ActorKelpInternalFactory internalFactory) {
            super(kryoFactory);
            this.configDeployment = configDeployment;
            this.internalFactory = internalFactory;
            super.initSystem();
            initMergeConfig(config);
        }

        @Override
        protected void initLogger() {
            super.initLogger();
            originalLogger = logger;
        }

        protected void initMergeConfig(ConfigKelp config) {
            Class<? extends ConfigBase> confType = ConfigKelp.class;
            if (config != null) {
                confType = config.getClass();
            }
            ConfigKelp configFromDep = (ConfigKelp) configDeployment.createAppConfig(confType);
            if (config != null) {
                configFromDep.mergeChangedFields(config);
            }
            withConfig(configFromDep);
        }

        public ActorSystemDefaultForKelp withConfig(ConfigKelp config) {
            this.config = config;
            setLogger(new SystemLoggerKelp(originalLogger, configDeployment, config));
            this.updateTiming = awaitTiming(config.systemPendingMessageSize);
            return this;
        }

        @Override
        protected void initSystem() {
            //delay
        }

        @Override
        protected void initSystemExecutorService() {
//            executorService = Executors.newFixedThreadPool(
//                    Math.max(1, (int) (threads * configDeployment.systemThreadFactor)));
            executorService = Executors.newCachedThreadPool();
            mergerExecutors = Executors.newCachedThreadPool(new SpecialThreadFactory());
        }

        @Override
        protected ScheduledExecutorService initScheduledExecutor() {
            return Executors.newScheduledThreadPool(getScheduledExecutorThreads(), new ScheduledThreadFactory());
        }

        public ExecutorService getMergerExecutors() {
            return mergerExecutors;
        }

        @Override
        protected void initThroughput() {
            throughput = configDeployment.systemThroughput;
        }

        public int getServerLeaderThreads() {
            return configDeployment.systemServerLeaderThreads;
        }

        public int getServerWorkerThreads() {
            return Math.max(1, (int) (getThreads() * configDeployment.systemServerWorkerThreadsFactor));
        }

        public int getClientThreads() {
            return Math.max(1, (int) (getThreads() * configDeployment.systemClientThreadsFactor));
        }

        @Override
        public void close() {
            super.close();
            mergerExecutors.shutdownNow();
        }

        @Override
        protected ProcessMessage getProcessMessageSubsequently(Actor target, boolean special, Message<?> msg) {
            if (special && !(msg instanceof Message.MessageNone) &&
                    !target.isDelayedMessage(msg)) { //DelayedMessage are not awaited
                updateAndWait(target);
            }
            return super.getProcessMessageSubsequently(target, special, msg);
        }

        @Override
        public ProcessMessageSubsequently createProcessMessageSubsequently(Actor target, boolean special) {
            return special ?
                    new ProcessMessageSubsequentlyKelpSpecial(this, target) :
                    new ProcessMessageSubsequently(this, target);
        }

        public void updateAndWait(Actor target) {
            if (updateCount.incrementAndGet() % updateTiming == 0) {
                Mailbox mb = target.getMailbox();
                if (mb instanceof MailboxKelp && !isSpecialThread()) {
                    MailboxKelp kmb = (MailboxKelp) mb;
                    long size = kmb.getMailbox().getPreviousSizeOnMemory();
                    long pendingLimit = config().systemMailboxPendingMessageSize;
                    double factor = config().systemMailboxWaitMsFactor;
                    await(new ActorAwaitLogHandler(this, kmb, pendingLimit, target),
                            kmb.getHistory(), size, pendingLimit, factor);
                }
            }
        }

        @Override
        public boolean register(Actor actor) {
            boolean r = super.register(actor);
            if (r && actor instanceof PersistentConditionActor.MemoryManagedActor) {
                double p = ((PersistentConditionActor.MemoryManagedActor) actor).memoryManageRatioTotalActor();
                memoryManagedActors.getAndUpdate(v -> v + p);
            }
            return r;
        }

        @Override
        public boolean unregister(Actor actor) {
            boolean r = super.unregister(actor);
            if (r && actor instanceof PersistentConditionActor.MemoryManagedActor) {
                double p = ((PersistentConditionActor.MemoryManagedActor) actor).memoryManageRatioTotalActor();
                memoryManagedActors.getAndUpdate(v -> v - p);
            }
            return r;
        }

        public double getMemoryManagedActors() {
            return memoryManagedActors.get();
        }
    }

    public static class ActorAwaitLogHandler implements AwaitLogHandler {
        protected ActorSystem system;
        protected Actor actor;
        protected MailboxKelp mbox;
        protected long pendingLimit;

        public ActorAwaitLogHandler(ActorSystem system, MailboxKelp mbox, long pendingLimit, Actor actor) {
            this.system = system;
            this.mbox = mbox;
            this.pendingLimit = pendingLimit;
            this.actor = actor;
        }

        @Override
        public void log(long waitMs, Stream<Long> pendingSize, float sizeDecreasesRate, long averagePendingSize, long pendingLimit) {
            log( "wait %,d ms, pendingSize=%s, decreasesRate=%.2f, averagePendingSize=%,d/%,d",
                    waitMs,
                    pendingSize.map(l -> String.format("%,d", l))
                            .collect(Collectors.joining(", ")),
                    sizeDecreasesRate, averagePendingSize, pendingLimit);
        }


        public void log(String fmt, Object... args) {
            ConfigBase.FormatAndArgs fa = new ConfigBase.FormatAndArgs("ActorSystemDefaultForKelp ")
                    .append(new ConfigBase.FormatAndArgs(fmt, args)
                            .append(new ConfigBase.FormatAndArgs(" @ %s %s", actor, Thread.currentThread())));
            system.getLogger().log(debugLogColor, fa.format, fa.args);
        }

        @Override
        public void logError(Throwable ex) {
            system.getLogger().log(true, debugLogColor, ex, "ConnectionHost: wait error %s", actor);
        }

        @Override
        public void logAfter(long waitMs, long mboxSize) {
            long afterSize = mbox.getMailbox().getPreviousSizeOnMemory();
            log( "after wait %,d ms: pendingSize %,7d -> %,7d (%+,d)",
                    waitMs, mboxSize, afterSize, (afterSize - mboxSize));
        }
    }

    public static boolean isSpecialThread() {
        return Thread.currentThread() instanceof SpecialThread;
    }

    public static class SpecialThreadFactory implements ThreadFactory {
        protected AtomicInteger count = new AtomicInteger();
        protected static AtomicInteger totalCount = new AtomicInteger();
        protected int factoryCount;
        protected ThreadGroup group;
        protected int priority;
        public SpecialThreadFactory() {
            factoryCount = totalCount.getAndIncrement();
            group = Thread.currentThread().getThreadGroup();
            priority = Math.max(Thread.MIN_PRIORITY,
                    Math.min(Thread.MAX_PRIORITY,
                    initPriority(group.getMaxPriority(), Thread.currentThread().getPriority())));
        }
        protected int initPriority(int max, int current) {
            return (int) (((max - current) * 0.8) + current);
        }
        protected String name() {
            return "kelp-special";
        }
        @Override
        public Thread newThread(Runnable r) {
            return new SpecialThread(group, r, String.format("%s-%d-%d", name(), factoryCount, count.getAndIncrement()), priority);
        }
    }

    public static class ScheduledThreadFactory extends SpecialThreadFactory {
        @Override
        protected int initPriority(int max, int current) {
            return (int) (((max - current) * 0.6) + current);
        }

        @Override
        protected String name() {
            return "kelp-schedule";
        }
    }

    public static class SpecialThread extends Thread {
        public SpecialThread(ThreadGroup group, Runnable r, String name, int priority) {
            super(group, r, name);
            setDaemon(false);
            setPriority(priority);
        }
    }

    public static class ProcessMessageSubsequentlyKelpSpecial extends ActorSystemDefault.ProcessMessageSubsequentlySpecial {
        public ProcessMessageSubsequentlyKelpSpecial(ActorSystemDefaultForKelp system, Actor actor) {
            super(system, actor);
        }

        @Override
        public void submit() {
            //if other actors are busily processing, then it might be awaiting in all executor's threads.
            // the special message needs to be immediately processed on a free thread
            ((ActorSystemDefaultForKelp) system).getMergerExecutors().execute(this);
        }
    }


    public void setConfig(ConfigKelp config) {
        this.config = config;
        ((ActorSystemDefaultForKelp) localSystem).withConfig(config);
    }

    public ConfigKelp getConfig() {
        return config;
    }

    @Override
    protected void init() {
        super.init();
        hostMap = new ConcurrentHashMap<>();
    }

    @Override
    protected ConnectionActor createConnection(ActorAddress addr) {
        if (addr instanceof ActorAddress.ActorAddressRemote) {
            try {
                return new ConnectionActorKelp(localSystem, this,
                        hostMap.computeIfAbsent(addr.getHostAddress(), this::createConnectionHost),
                        (ActorAddress.ActorAddressRemote) addr);
            } catch (InterruptedException ex) {
                getLogger().log(true, debugLogColor, ex, "createConnection: %s", addr);
                return null;
            }
        } else {
            return null;
        }
    }

    protected ConnectionHost createConnectionHost(ActorAddress hostAddr) {
        return new ConnectionHost(this, hostAddr);
    }

    public static class ConnectionHost {
        protected ActorSystem system;
        protected ActorAddress address;
        protected AtomicLong size = new AtomicLong();
        protected AtomicLong transferredBytes = new AtomicLong();
        protected volatile Instant previousLogTime = Instant.now();
        protected long totalTransferredBytes;

        public ConnectionHost(ActorSystem system, ActorAddress address) {
            this.system = system;
            this.address = address;
        }

        public void addSize(int n) {
            size.addAndGet(n);
        }

        public long size() {
            return size.get();
        }

        public void addBytes(long n) {
            transferredBytes.addAndGet(n);
        }

        public long getTransferredBytes() {
            return transferredBytes.get();
        }

        public void clearTransferredBytes() {
            totalTransferredBytes += transferredBytes.getAndSet(0);
        }

        public long getTotalTransferredBytes() {
            return totalTransferredBytes + getTransferredBytes();
        }

        protected ConfigKelp getConfig() {
            return ((ActorKelpBuilder) system).config();
        }

        public double getWaitMsFactor() {
            return getConfig().systemWaitMsFactor;
        }

        public Duration getHostUpdateTime() {
            return Duration.ofMillis(getConfig().systemHostUpdateMs);
        }

        public int getMaxBundle() {
            return getConfig().systemMaxBundle;
        }

        public long getPendingMessageSize() {
            return getConfig().systemPendingMessageSize;
        }

        public long getPendingMessageLimit() {
            return getConfig().systemPendingMessageLimit;
        }

        public boolean checkLogTime() {
            Instant now = Instant.now();
            Duration d = Duration.between(previousLogTime, now);
            if (d.compareTo(getHostUpdateTime()) > 0) {
                synchronized (this) {
                    this.previousLogTime = now;
                }
                return true;
            } else {
                return false;
            }
        }
    }

    public interface AwaitLogHandler {
        void log(long waitMs, Stream<Long> pendingSize, float sizeDecreasesRate, long averagePendingSize, long pendingLimit);
        void logError(Throwable ex);
        void logAfter(long waitMs, long mboxSize);
    }

    public static int awaitTiming(long pendingSize) {
        return (int) Math.max(0, Math.min(Integer.MAX_VALUE, pendingSize / 2));
    }

    public static <HistType extends History<HistType>> void await(AwaitLogHandler log, HistType history, long mboxSize,
                                                                  long pendingMessageLimit, double waitMsFactor) {
        List<HistType> hs = history.toList(HISTORY_SIZE_OLDEST);
        float sdr = history.sizeDecreasesRate(hs);
        long avg = history.averagePendingSize(hs);
        if (sdr == 0 || avg > pendingMessageLimit) {
            long ms = (long) (mboxSize * waitMsFactor);
            log.log(ms, hs.stream().map(h -> h.size), sdr, avg, pendingMessageLimit);
            try {
                Thread.sleep(ms);
            } catch (Exception ex) {
                log.logError(ex);
            }
            log.logAfter(ms, mboxSize);
        }
    }

    public static class ConnectionActorKelp extends ConnectionActor implements AwaitLogHandler {
        protected ConnectionHost host;

        protected volatile Instant previousUpdateTime = Instant.now();
        protected volatile ConnectionHostHistory history;

        protected int updateTiming;
        protected AtomicLong updateCount = new AtomicLong();

        public ConnectionActorKelp(ActorSystem system, ActorSystemRemote remoteSystem, ConnectionHost host, ActorAddress.ActorAddressRemote address) throws InterruptedException {
            super(system, remoteSystem, address);
            this.host = host;
            this.updateTiming = awaitTiming(host.getPendingMessageSize());
            this.mailbox = new MailboxForConnection(host);
            initHistory();
        }

        protected void initHistory() {
            this.history = ConnectionHostHistory.create(HISTORY_SIZE);
        }

        @Override
        protected Mailbox initMailbox() {
            return null; //for delay init
        }

        @Override
        public void tell(Object data) {
            super.tell(data);
            update();
        }

        public void log(String fmt, Object... args) {
            ConfigBase.FormatAndArgs fa = new ConfigBase.FormatAndArgs("ConnectionActorKelp [%s->%s] : ",
                    getRemoteSystem().getServerAddress(), address)
                    .append(new ConfigBase.FormatAndArgs(fmt, args));
            system.getLogger().log(debugLogColor, fa.format, fa.args);
        }

        public void update() {
            if (updateCount.incrementAndGet() % updateTiming == 0) {
                Instant now = Instant.now();
                Duration d = Duration.between(previousUpdateTime, now);
                if (d.compareTo(host.getHostUpdateTime()) > 0) {
                    updateAndWait();
                }
            }
        }

        protected synchronized void updateAndWait() {
            Instant now = Instant.now();
            Instant prev = previousUpdateTime;
            Duration time = Duration.between(prev, now);
            if (time.compareTo(host.getHostUpdateTime()) <= 0) {
                return; //multiple entering from update() and the first one already done
            }
            previousUpdateTime = now;

            long bs = host.getTransferredBytes();
            long mbox = host.size();
            host.clearTransferredBytes();
            long totalBs = host.getTotalTransferredBytes();

            log("tells=%,7d, transBytes=%,14d, totalBytes=%,14d, pendingSize=%,7d, elapsed=%s",
                    updateCount.get(), bs, totalBs, mbox, time);

            updateCount.set(1);

            ConnectionHostHistory newest = history;
            history = history.set(mbox, totalBs, time);
            //the current history has the oldest values
            if (newest.size > host.getPendingMessageSize() && !isSpecialThread()) {
                ActorSystemKelp.await(this, history, mbox, host.getPendingMessageLimit(), host.getWaitMsFactor());
            }
        }

        @Override
        public void log(long waitMs, Stream<Long> pendingSize, float sizeDecreasesRate, long averagePendingSize, long pendingLimit) {
            log( "wait %,d ms, pendingSize=%s, bytesPerSec=%,.2f, decreasesRate=%.2f, averagePendingSize=%,d/%,d",
                    waitMs,
                    pendingSize.map(l -> String.format("%,d", l))
                            .collect(Collectors.joining(", ")),
                    history.bytesPerSec(history.toList(HISTORY_SIZE_OLDEST)),
                    sizeDecreasesRate, averagePendingSize, host.getPendingMessageLimit());
        }

        @Override
        public void logError(Throwable ex) {
            system.getLogger().log(true, debugLogColor, ex, "ConnectionHost: wait error");
        }

        @Override
        public void logAfter(long waitMs, long mboxSize) {
            long afterSize = host.size();
            log( "after wait %,d ms: pendingSize %,7d -> %,7d (%+,d)",
                    waitMs, mboxSize, afterSize, (afterSize - mboxSize));
        }

        @Override
        public int getMaxBundle() {
            return host.getMaxBundle();
        }

        @Override
        protected void writeSingleMessage(Message.MessageDataClock<Message<?>> message) {
            if (debugLogMsg) logMsg("%s write %s", this, message);
            long pre = connection.getRecordSendBytes();
            try {
                connection.getChannel().writeAndFlush(new MessageDataTransferred(currentCount(), remoteSystem.getServerAddress(), message)).sync();
            } catch (Exception ex) {
                throw new ConnectionSendException(ex, message);
            }
            ++count;
            ++recordSendMessages;
            long pos = connection.getRecordSendBytes();
            long len = pos - pre;
            host.addBytes(len);
        }

        @Override
        protected void writeNonEmpty(List<? extends Message.MessageDataClock<Message<?>>> messageBundle) {
            if (debugLogMsg) logMsg("%s write %,d messages: %s,...", this, messageBundle.size(), messageBundle.get(0));
            long pre = connection.getRecordSendBytes();
            try {
                connection.getChannel().writeAndFlush(new MessageDataTransferred(currentCount(), remoteSystem.getServerAddress(), messageBundle)).sync();
            } catch (Exception ex) {
                throw new ConnectionSendException(ex, messageBundle);
            }
            ++count;
            recordSendMessages += messageBundle.size();
            long pos = connection.getRecordSendBytes();
            long len = pos - pre;
            host.addBytes(len);
        }
    }

    public static class MailboxForConnection extends MailboxDefault {
        protected ConnectionHost host;

        public MailboxForConnection(ConnectionHost host) {
            this.host = host;
        }

        @Override
        public void offer(Message<?> message) {
            host.addSize(1);
            super.offer(message);
        }

        @Override
        public Message<?> poll() {
            Message<?> m = super.poll();
            if (m != null) {
                host.addSize(-1);
            }
            return m;
        }
    }

    public static int HISTORY_SIZE = 100;
    public static int HISTORY_SIZE_OLDEST = 7;

    public static abstract class History<SelfType extends History<SelfType>> {
        public long size;
        public SelfType next;

        public static <HistoryType extends History<HistoryType>> HistoryType create(int size, Supplier<HistoryType> cons) {
            HistoryType tail = cons.get();
            HistoryType top = tail;
            for (int i = 0; i < size; ++i) {
                HistoryType n = cons.get();
                n.next = top;
            }
            tail.next = top;
            return top;
        }

        @SuppressWarnings("unchecked")
        public List<SelfType> toList(int size) {
            SelfType h = this.next;
            List<SelfType> hs = new ArrayList<>(size);
            hs.add((SelfType) this);
            while (h != this && hs.size() < size) {
                hs.add(h);
                h = h.next;
            }
            return hs;
        }

        public float sizeDecreasesRate(List<SelfType> hs) {
            SelfType pre = null;
            int count = 0;
            int max = 0;
            for (SelfType h : hs) {
                if (pre == null) {
                    pre = h;
                } else {
                    if (pre.size > h.size) {
                        ++count;
                    }
                    ++max;
                }
            }
            return count / (float)max;
        }

        public long averagePendingSize(List<SelfType> hs) {
            long s = 0;
            for (SelfType h : hs) {
                s += h.size;
            }
            return (long) (s / (double) hs.size());
        }

        public SelfType set(long size) {
            this.size = size;
            return next;
        }
    }

    public static class KelpHistory extends History<KelpHistory> {
        public static KelpHistory create(int size) {
            return create(size, KelpHistory::new);
        }
    }

    /**
     * a looped chain for recent message transfer statistics.
     * the default size of the chain is {@link #HISTORY_SIZE}.
     * <ul>
     *     <li>{@link #size}: the mail box message size of all {@link ConnectionActorKelp}s of a host.
     *          it means there are the "size" pending messages on the queue</li>
     *     <li>{@link #transferredBytes}: the total bytes of transferred messages of all {@link ConnectionActorKelp}s of a host</li>
     *     <li>{@link #time}: time spent for sending the message data</li>
     * </ul>
     * The chain is created for each {@link ConnectionActorKelp} but the stats are shared for same host.
     */
    public static class ConnectionHostHistory extends History<ConnectionHostHistory> {
        public long transferredBytes;
        public Duration time = Duration.ofSeconds(1); //use 1 as default

        public ConnectionHostHistory next;

        public static ConnectionHostHistory create(int size) {
            return create(size, ConnectionHostHistory::new);
        }

        public ConnectionHostHistory set(long size, long transferredBytes, Duration time) {
            this.transferredBytes = transferredBytes;
            this.time = time;
            return set(size);
        }

        public double bytesPerSec(List<ConnectionHostHistory> hs) {
            Duration time = Duration.ZERO;
            long bytes = 0;
            for (ConnectionHostHistory h : hs) {
                time = time.plus(h.time);
                bytes += h.transferredBytes;
            }
            return bytes / seconds(time);
        }

        public double seconds(Duration d) {
            long n = d.getSeconds();
            double nn = d.getNano() / 1_000_000_000.0;
            return n + nn;
        }

    }
}
