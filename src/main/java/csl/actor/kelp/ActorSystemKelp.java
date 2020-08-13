package csl.actor.kelp;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.*;
import csl.actor.cluster.ActorSystemCluster;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.util.ConfigBase;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ActorSystemKelp extends ActorSystemRemote {
    protected Map<ActorAddress, ConnectionHost> connectionHostMap;
    protected volatile ConfigKelp config = ConfigKelp.CONFIG_DEFAULT;

    public static ActorSystemKelp create(ConfigDeployment configDeployment) {
        Function<ActorSystem, Kryo> kryoFactory = configDeployment.kryoBuilder(defaultBuilderType());
        return create(configDeployment, kryoFactory);
    }

    public static ActorSystemKelp create(ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory) {
        return new ActorSystemKelp(new ActorSystemDefaultForKelp(configDeployment, kryoFactory), kryoFactory);
    }

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

    public static Class<KryoBuilderKelp> defaultBuilderType() {
        return KryoBuilderKelp.class;
    }

    public ActorSystemKelp() {
        this(new ConfigDeployment());
    }

    public ActorSystemKelp(ConfigDeployment configDeployment) {
        this(new ActorSystemDefaultForKelp(configDeployment), configDeployment.kryoBuilder(defaultBuilderType()));
    }

    public ActorSystemKelp(ActorSystemDefaultForKelp localSystem, Function<ActorSystem, Kryo> kryoFactory) {
        super(localSystem, kryoFactory);
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

    public static class ActorSystemDefaultForKelp extends ActorSystemCluster.ActorSystemDefaultForCluster {
        protected ConfigDeployment configDeployment;
        protected ExecutorService mergerExecutors;

        public ActorSystemDefaultForKelp() {
            this(new ConfigDeployment(), KryoBuilderKelp.builder());
        }

        public ActorSystemDefaultForKelp(ConfigDeployment configDeployment) {
            this(configDeployment, configDeployment.kryoBuilder(defaultBuilderType()));
        }

        public ActorSystemDefaultForKelp(ConfigDeployment configDeployment, Function<ActorSystem, Kryo> kryoFactory) {
            super(kryoFactory);
            this.configDeployment = configDeployment;
            super.initSystem();
        }

        @Override
        protected void initSystem() {
            //delay
        }

        @Override
        protected void initSystemExecutorService() {
            executorService = Executors.newFixedThreadPool(
                    Math.max(1, (int) (threads * configDeployment.systemThreadFactor)));
            mergerExecutors = Executors.newCachedThreadPool();
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
    }

    public void setConfig(ConfigKelp config) {
        this.config = config;
    }

    public ConfigKelp getConfig() {
        return config;
    }

    @Override
    protected void init() {
        super.init();
        connectionHostMap = new ConcurrentHashMap<>();
    }

    @Override
    protected ConnectionActor createConnection(ActorAddress addr) {
        if (addr instanceof ActorAddress.ActorAddressRemote) {
            try {
                return new ConnectionActorKelp(localSystem, this,
                        connectionHostMap.computeIfAbsent(addr.getHostAddress(), this::createConnectionHost),
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
        protected ActorSystemKelp system;
        protected ActorAddress address;
        protected AtomicLong size = new AtomicLong();
        protected AtomicLong transferredBytes = new AtomicLong();
        protected volatile Instant previousLogTime = Instant.now();

        public ConnectionHost(ActorSystemKelp system, ActorAddress address) {
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


        public double getWaitMsFactor() {
            return system.getConfig().systemWaitMsFactor;
        }

        public Duration getHostUpdateTime() {
            return Duration.ofMillis(system.getConfig().systemHostUpdateMs);
        }

        public int getMaxBundle() {
            return system.getConfig().systemMaxBundle;
        }

        public long getPendingMessageSize() {
            return system.getConfig().systemPendingMessageSize;
        }

        public long getPendingMessageLimit() {
            return system.getConfig().systemPendingMessageLimit;
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

    public static class ConnectionActorKelp extends ConnectionActor {
        protected ConnectionHost host;

        protected volatile Instant previousLogTime = Instant.now();
        protected volatile ConnectionHostHistory history;

        public ConnectionActorKelp(ActorSystem system, ActorSystemRemote remoteSystem, ConnectionHost host, ActorAddress.ActorAddressRemote address) throws InterruptedException {
            super(system, remoteSystem, address);
            this.host = host;
            this.mailbox = new MailboxForConnection(host);
            initHistory();
        }

        protected void initHistory() {
            ConnectionHostHistory tail = new ConnectionHostHistory();
            ConnectionHostHistory top = tail;
            for (int i = 0; i < HISTORY_SIZE - 1; ++i) {
                ConnectionHostHistory n = new ConnectionHostHistory();
                n.next = top;
                top = n;
            }
            tail.next = top;
            this.history = top;
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
            ConfigBase.FormatAndArgs fa = new ConfigBase.FormatAndArgs("ConnectionActorKelp ")
                    .append(new ConfigBase.FormatAndArgs(fmt, args)
                    .append(new ConfigBase.FormatAndArgs(" @ %s", address)));
            system.getLogger().log(true, debugLogColor, fa.format, fa.args);
        }

        public void update() {
            Instant now = Instant.now();
            Duration d = Duration.between(previousLogTime, now);
            if (d.compareTo(host.getHostUpdateTime()) > 0) {
                updateAndWait();
            }
        }

        protected synchronized void updateAndWait() {
            Instant now = Instant.now();
            Instant prev = previousLogTime;
            Duration time = Duration.between(prev, now);
            if (time.compareTo(host.getHostUpdateTime()) <= 0) {
                return; //multiple entering from update() and the first one already done
            }
            previousLogTime = now;

            long bs = host.getTransferredBytes();
            long mbox = host.size();

            if (host.checkLogTime()) { // in order to avoid log flooding
                log("transferredBytes=%,14d, pendingSize=%,7d, elapsed=%s", bs, mbox, time);
            }
            ConnectionHostHistory newest = history;
            history = history.set(mbox, bs, time);
            //the current history has oldest values
            if (newest.size > host.getPendingMessageSize()) {
                List<ConnectionHostHistory> hsAll = history.toList();
                List<ConnectionHostHistory> hs = hsAll.subList(hsAll.size() - 7, hsAll.size()); //only recent items
                float sdr = history.sizeDecreasesRate(hs);
                long avg = history.averagePendingSize(hs);
                if (sdr == 0 || avg > host.getPendingMessageLimit()) {
                    long ms = (long) (mbox * host.getWaitMsFactor());
                    log( "wait %,d ms, pendingSize=%s, bytesPerSec=%,.2f, decreasesRate=%.2f, averagePendingSize=%,d/%,d",
                            ms,
                            hs.stream().map(h -> h.size)
                                    .map(l -> String.format("%,d", l))
                                    .collect(Collectors.joining(", ")),
                            history.bytesPerSec(hs),
                            sdr, avg, host.getPendingMessageLimit());

                    try {
                        Thread.sleep(ms);
                    } catch (Exception ex) {
                        system.getLogger().log(true, debugLogColor, ex, "ConnectionHost: wait error");
                    }

                    long afterSize = host.size();
                    log( "after wait %,d ms: pendingSize %,7d -> %,7d (%+,d)",
                            ms, mbox, afterSize, (afterSize - mbox));
                }
            }
        }

        @Override
        public int getMaxBundle() {
            return host.getMaxBundle();
        }

        @Override
        protected void writeSingleMessage(Message<?> message) {
            writeObject(message);
        }

        @Override
        protected void writeNonEmpty(List<Object> messageBundle) {
            writeObject(messageBundle);
        }

        protected void writeObject(Object msg) {
            long pre = connection.getRecordSendBytes();
            try {
                connection.getChannel().writeAndFlush(new TransferredMessage(count, msg)).sync();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
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

    public static class ConnectionHostHistory {
        public long size;
        public long transferredBytes;
        public Duration time = Duration.ofSeconds(1); //use 1 as default

        public ConnectionHostHistory next;

        public ConnectionHostHistory set(long size, long transferredBytes, Duration time) {
            this.size = size;
            this.transferredBytes = transferredBytes;
            this.time = time;
            return next;
        }

        public List<ConnectionHostHistory> toList() {
            ConnectionHostHistory h = this.next;
            List<ConnectionHostHistory> hs = new ArrayList<>(HISTORY_SIZE);
            hs.add(this);
            while (h != this) {
                hs.add(h);
                h = h.next;
            }
            return hs;
        }

        public float sizeDecreasesRate(List<ConnectionHostHistory> hs) {
            ConnectionHostHistory pre = null;
            int count = 0;
            int max = 0;
            for (ConnectionHostHistory h : hs) {
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

        public long averagePendingSize(List<ConnectionHostHistory> hs) {
            long s = 0;
            for (ConnectionHostHistory h : hs) {
                s += h.size;
            }
            return (long) (s / (double) hs.size());
        }
    }
}
