package csl.actor.cluster;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.*;
import csl.actor.persist.PersistentFileManager;
import csl.actor.remote.*;
import csl.actor.util.ConfigBase;
import csl.actor.util.PathModifier;
import csl.actor.util.ResponsiveCalls;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;

public class ActorSystemCluster extends ActorSystemRemote implements PersistentFileManager.PersistentFileManagerFactory {
    protected Map<ActorAddress, UnitStatus> units;

    public ActorSystemCluster() {
        super(new ActorSystemDefaultForCluster(KryoBuilderCluster.builder()), KryoBuilderCluster.builder());
    }

    public ActorSystemCluster(ActorSystemDefault localSystem, Function<ActorSystem, Kryo> kryoFactory) {
        super(localSystem, kryoFactory);
    }

    public static ActorSystemCluster createWithKryoBuilderThrottle(Function<ActorSystem, Kryo> kryoFactory) {
        return new ActorSystemCluster(new ActorSystemDefaultForClusterThrottle(kryoFactory), kryoFactory);
    }

    public static ActorSystemCluster createWithKryoBuilder(Function<ActorSystem, Kryo> kryoFactory) {
        return new ActorSystemCluster(new ActorSystemDefaultForCluster(kryoFactory), kryoFactory);
    }

    public static class ActorSystemDefaultForCluster extends ActorSystemDefaultForRemote implements KryoBuilder.SerializerFactory {
        protected Function<ActorSystem, Kryo> kryoBuilder;

        public ActorSystemDefaultForCluster(Function<ActorSystem, Kryo> kryoBuilder) {
            this.kryoBuilder = kryoBuilder;
        }

        @Override
        protected void initSystemExecutorService() {
            executorService = Executors.newFixedThreadPool((threads * 5));
        }

        @Override
        protected void initThroughput() {
            throughput = 256;
        }

        @Override
        public void sendDeadLetter(Message<?> message) {
            if (message.getData() instanceof ActorPlacement.LeaveEntry) {
                //the target might be already left
            } else {
                super.sendDeadLetter(message);
            }
        }

        @Override
        public Kryo createSerializer() {
            return kryoBuilder.apply(this);
        }
    }

    public static class ActorSystemDefaultForClusterThrottle extends ActorSystemDefaultForRemote implements KryoBuilder.SerializerFactory {
        protected Function<ActorSystem, Kryo> kryoBuilder;

        public ActorSystemDefaultForClusterThrottle(Function<ActorSystem, Kryo> kryoBuilder) {
            this.kryoBuilder = kryoBuilder;
        }

        @Override
        protected void initSystemExecutorService() {
            executorService = createThreadPoolUnlimited(threads);
        }

        @Override
        protected void initThroughput() {
            threads = 256;
        }

        @Override
        public void sendDeadLetter(Message<?> message) {
            if (message.getData() instanceof ActorPlacement.LeaveEntry) {
                //the target might be already left
            } else {
                super.sendDeadLetter(message);
            }
        }

        @Override
        public Kryo createSerializer() {
            return kryoBuilder.apply(this);
        }
    }

    @Override
    protected void init() {
        super.init();
        initUnits();
    }

    protected void initUnits() {
        units = new ConcurrentHashMap<>();
    }

    @Override
    protected ObjectMessageClient initObjectMessageClient() {
        return new ObjectMessageClientThrottle(this);
    }

    @Override
    public boolean isSpecialMessageData(Object data) {
        return super.isSpecialMessageData(data)||
                data instanceof ActorPlacement.AddressList ||
                data instanceof ClusterDeployment.ConfigSet;
    }

    @Override
    public void send(Message<?> message) {
        awaits(message, ConfigBase.lazyToString(() ->
                "\n  send " + message.toString(getLogger()::toStringLimit)));
        super.send(message);
    }

    public UnitStatus getUnitStatus(ActorRef target) {
        if (target instanceof ActorRefRemote) {
            return getUnitStatus(((ActorRefRemote) target).getAddress().getHostAddress());
        } else {
            return units.get(getServerAddress());
        }
    }

    public UnitStatus getUnitStatus(ActorAddress host) {
        return units.get(host);
    }

    public void updateUnitStatus(ActorAddress host, boolean start, Instant time) {
        units.computeIfAbsent(host, UnitStatus::new)
                .updateTime(this, start, time);
    }

    public void awaits(Message<?> message, Object info) {
        if (!isSpecialMessage(message)) {
            ActorRef target = message.getTarget();
            UnitStatus status = getUnitStatus(target);
            if (status != null) {
                status.awaits(this, info);
            }
        }
    }

    public static Duration max = Duration.ofMillis(Long.MAX_VALUE);

    public static boolean logThrottle = System.getProperty("csl.actor.throttle", "true").equals("true");
    public static int logColorThrottle = ActorSystem.systemPropertyColor("csl.actor.throttle.color", 54);

    public static class UnitStatus {
        protected ActorAddress address;
        protected volatile Instant time = Instant.now();
        protected Set<Thread> waitingThreads = new HashSet<>();
        protected int runningCount;

        public UnitStatus(ActorAddress address) {
            this.address = address;
        }

        public synchronized void updateTime(ActorSystemCluster system, boolean start, Instant time) {
            if (!start) {
                --runningCount;
                if (runningCount < 0) {
                    runningCount = 0;
                }
            } else {
                ++runningCount;
            }
            system.getLogger().log(logThrottle, logColorThrottle, "updateTime: %s %s %s -> %s (%s) waiting=%,d running=%,d",
                    address, (start ? "start" : "finish"), ActorSystem.timeForLog(this.time), ActorSystem.timeForLog(time),
                    Duration.between(Instant.now(), time), waitingThreads.size(), runningCount);
            if (time.compareTo(this.time) < 0) { //shorter time
                if (runningCount <= 0) {
                    this.time = time;
                    waitingThreads.forEach(Thread::interrupt);
                }
            } else {
                this.time = time;
            }
        }

        public void awaits(ActorSystemCluster system, Object info) {
            int n = 0;
            Instant start = null;
            while (true) {
                Instant now = Instant.now();
                if (start == null) {
                    start = now;
                }
                Duration t = Duration.between(now, time);
                if (max.compareTo(t) < 0) {
                    t = max;
                }
                Thread th = Thread.currentThread();
                if (!t.isNegative()) {
                    system.getLogger().log(logThrottle, logColorThrottle, "awaits: %s %s (%s) thread=%s %s",
                            address, ActorSystem.timeForLog(time), t, th, info);
                    synchronized (this) {
                        waitingThreads.add(th);
                    }
                    try {
                        TimeUnit.MILLISECONDS.sleep(t.toMillis());
                    } catch (InterruptedException ie) {
                        //
                    }
                } else {
                    synchronized (this) {
                        waitingThreads.remove(th);
                        Thread.interrupted();
                    }
                    if (n > 0) {
                        try {
                            system.getLogger().log(logThrottle, logColorThrottle, "awaits finish: %s total %s before %s thread=%s loop=%,d",
                                    address, Duration.between(start, now), t, th, n);
                        } catch (Exception ie) {}
                    }
                    break;
                }
                ++n;
            }
        }
    }

    @Override
    public PersistentFileManagerThrottle createFileManager(String path) {
        getLogger().log(logThrottle, logColorThrottle, "throttle enabled: %s",
                this);
        return new PersistentFileManagerThrottle(path, this);
    }

    public static class PersistentFileManagerThrottle extends PersistentFileManager {
        protected ActorSystemCluster system;

        public PersistentFileManagerThrottle(String path, ActorSystemCluster system) {
            this(path, system.getSerializer(), PathModifier.getPathModifier(system), system.getLogger(), system);
        }

        public PersistentFileManagerThrottle(String path, KryoBuilder.SerializerFunction serializer, PathModifier pathModifier, SystemLogger logger,
                                             ActorSystemCluster system) {
            super(path, serializer, pathModifier, logger);
            this.system = system;
        }

        @Override
        public void openForWrite(Path path) {
            Instant future = Instant.now().plus(30, ChronoUnit.MINUTES);
            sendUpdate(true, future);
        }

        public void sendUpdate(boolean start, Instant future) {
            ActorAddress.ActorAddressRemote addr = system.getServerAddress();
            ClusterDeployment.ActorPlacementForCluster<?> p = getPlacement();
            updateStatus(addr, start, future, p);
            p.getCluster()
                    .forEach(a -> ResponsiveCalls.sendTaskConsumer(system, a.getPlacementActor(),
                            (ap) -> updateStatus(addr, start, future, ap)));

        }

        public static void updateStatus(ActorAddress addr, boolean start, Instant future, Actor ap) {
            if (ap.getSystem() instanceof ActorSystemCluster) {
                ActorSystemCluster system = (ActorSystemCluster) ap.getSystem();
                system.updateUnitStatus(addr, start, future);
            }
        }

        @Override
        public void close(Path path) {
            sendUpdate(false, Instant.now());
        }

        public ActorSystem getSystem() {
            return system;
        }

        public ClusterDeployment.ActorPlacementForCluster<?> getPlacement() {
            Actor placement = getSystem().resolveActorLocalNamed(
                    ActorRefLocalNamed.get(getSystem(), ActorPlacement.PLACEMENT_NAME));
            if (placement instanceof ClusterDeployment.ActorPlacementForCluster<?>) {
                return (ClusterDeployment.ActorPlacementForCluster<?>) placement;
            } else {
                return null;
            }
        }
    }

    public static class ObjectMessageClientThrottle extends ObjectMessageClient {
        protected ActorSystemCluster system;
        public ObjectMessageClientThrottle(ActorSystemCluster system) {
            super(system.getLogger());
            this.system = system;
        }

        public ActorSystemCluster getSystem() {
            return system;
        }

        @Override
        protected ObjectMessageConnection initConnection() {
            return new ObjectMessageConnectionThrottle(this);
        }

        public boolean isSpecial(Object msg) {
            return system.isSpecialMessageData(msg);
        }
    }

    public static class ObjectMessageConnectionThrottle extends ObjectMessageClient.ObjectMessageConnection {
        protected ActorAddress targetAddress;
        public ObjectMessageConnectionThrottle(ObjectMessageClientThrottle client) {
            super(client);
        }

        @Override
        public ObjectMessageClient.ObjectMessageConnection open() throws InterruptedException {
            targetAddress = ActorAddress.get(host, port);
            return super.open();
        }

        @Override
        public ObjectMessageClient.ObjectMessageConnection write(Object msg, int retryCount) {
            if (!isSpecial(msg)) {
                ActorSystemCluster system = ((ObjectMessageClientThrottle) getClient()).getSystem();
                UnitStatus status = system.getUnitStatus(targetAddress);
                if (status != null) {
                    status.awaits(system, ConfigBase.lazyToString(() ->
                            "\n  connection write " + system.getLogger().toStringLimit(msg)));
                } else {
                    if (retryCount > 0) {
                        system.getLogger().log(logThrottle, logColorThrottle,
                                "connection write: no status for %s with retryCount=%,d", targetAddress, retryCount);
                    }
                }
            }
            return super.write(msg, retryCount);
        }

        protected boolean isSpecial(Object msg) {
            if (msg instanceof TransferredMessage) {
                return isSpecial(((TransferredMessage) msg).body);
            } else if (msg instanceof Message<?>) {
                return isSpecial(((Message<?>) msg).getData());
            } else {
                return ((ObjectMessageClientThrottle) getClient()).isSpecial(msg);
            }
        }
    }
}
