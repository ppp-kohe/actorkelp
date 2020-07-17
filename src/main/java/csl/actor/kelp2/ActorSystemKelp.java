package csl.actor.kelp2;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.*;
import csl.actor.cluster.ActorSystemCluster;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;
import csl.actor.util.ConfigBase;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ActorSystemKelp extends ActorSystemRemote {
    protected Map<ActorAddress, ConnectionHost> connectionHostMap;
    protected volatile ConfigKelp config = ConfigKelp.CONFIG_DEFAULT;

    public ActorSystemKelp() {
        this(new ActorSystemCluster.ActorSystemDefaultForCluster(), KryoBuilder.builder());
    }

    public ActorSystemKelp(ActorSystemDefault localSystem, Function<ActorSystem, Kryo> kryoFactory) {
        super(localSystem, kryoFactory);
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

        protected volatile ConnectionHostHistory history;

        public ConnectionHost(ActorSystemKelp system, ActorAddress address) {
            this.system = system;
            this.address = address;
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

        public void update() {
            Instant now = Instant.now();
            Duration d = Duration.between(previousLogTime, now);
            if (d.compareTo(getHostUpdateTime()) > 0) {
                updateAndWait();
            }
        }

        protected synchronized void updateAndWait() {
            Instant now = Instant.now();
            Instant prev = previousLogTime;
            previousLogTime = now;
            Duration time = Duration.between(prev, now);
            long bs = getTransferredBytes();
            long mbox = size();

            log("transferredBytes=%,14d pendingSize=%,7d elapsed=%s", bs, mbox, time);
            ConnectionHostHistory newest = history;
            history = history.set(mbox, bs, time);
            //the current history has oldest values
            if (newest.size > getPendingMessageSize()) {
                List<ConnectionHostHistory> hsAll = history.toList();
                List<ConnectionHostHistory> hs = hsAll.subList(hsAll.size() - 7, hsAll.size()); //only recent items
                float sdr = history.sizeDecreasesRate(hs);
                if (sdr > 0) {
                    long ms = (long) (mbox * getWaitMsFactor());
                    log( "wait %,d ms pendingSize=%s bytesPerSec=%,.2f",
                        ms,
                        hs.stream().map(h -> h.size)
                                .map(l -> String.format("%,d", l))
                                .collect(Collectors.joining(", ")),
                        history.bytesPerSec(hs));

                    try {
                        Thread.sleep(ms);
                    } catch (Exception ex) {
                        system.getLogger().log(true, debugLogColor, ex, "ConnectionHost: wait error");
                    }

                    long afterSize = size();
                    log( "after wait %,d ms: pendingSize %,7d -> %,7d (%+,d)",
                            ms, mbox, afterSize, (afterSize - mbox));
                }
            }
        }

        public void log(String fmt, Object... args) {
            ConfigBase.FormatAndArgs fa = new ConfigBase.FormatAndArgs("ConnectionHost: %s ", address).append(new ConfigBase.FormatAndArgs(fmt, args));
            system.getLogger().log(true, debugLogColor, fa.format, fa.args);
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
    }

    public static class ConnectionActorKelp extends ConnectionActor {
        protected ConnectionHost host;

        public ConnectionActorKelp(ActorSystem system, ActorSystemRemote remoteSystem, ConnectionHost host, ActorAddress.ActorAddressRemote address) throws InterruptedException {
            super(system, remoteSystem, address);
            this.host = host;
            this.mailbox = new MailboxForConnection(host);
        }

        @Override
        protected Mailbox initMailbox() {
            return null; //for delay init
        }

        @Override
        public void tell(Object data) {
            super.tell(data);
            host.update();
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
    }
}
