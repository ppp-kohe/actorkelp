package csl.actor.cluster;

import csl.actor.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.util.ConfigBase;
import csl.actor.util.ResponsiveCalls;
import csl.actor.util.ToJson;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface ActorPlacement {
    String PLACEMENT_NAME = ActorPlacement.class.getName() + ".placement";

    ActorRef place(Actor a);

    abstract class ActorPlacementDefault extends ActorDefault implements ActorPlacement {
        protected List<AddressListEntry> cluster = new ArrayList<>(); //without self entry
        protected PlacementStrategy strategy;
        protected volatile int totalThreads;
        protected AtomicLong createdActors = new AtomicLong();
        protected long createdActorsNextLog = 1;

        protected long shutdownWaitMsAfterAllMembersLeft = -1L; //minus value: never
        protected volatile ScheduledFuture<?> shutdownAfterWait;

        public static boolean debugLog = System.getProperty("csl.actor.debug", "false").equals("true");

        public static int logColor = ActorSystem.systemPropertyColor("csl.actor.logColor", 33);

        protected ConfigBase logger = new ConfigBase();

        public void logDebug(String fmt, Object... args) {
            if (debugLog) {
                logger.log(logColor, fmt, args);
            }
        }

        public void log(String msg, Object... args) {
            logger.log(logColor, "%s %s", this, ConfigBase.lazyToString(() -> String.format(msg, args)));
        }

        public ActorPlacementDefault(ActorSystem system, String name) {
            super(system, name);
            strategy = initStrategy();
            totalThreads = system.getThreads();
            logger.setLogger(system.getLogger());
            ResponsiveCalls.initCallableTarget(system);
        }

        public ActorPlacementDefault(ActorSystem system, String name, PlacementStrategy strategy) {
            super(system, name);
            this.strategy = strategy;
            totalThreads = system.getThreads();
            logger.setLogger(system.getLogger());
            ResponsiveCalls.initCallableTarget(system);
        }

        public static void setLogColor(int logColor) {
            ActorPlacementDefault.logColor = logColor;
        }

        public void setLogger(ConfigBase logger) {
            this.logger = logger;
        }

        protected abstract PlacementStrategy initStrategy();

        public synchronized List<AddressListEntry> getCluster() {
            return new ArrayList<>(cluster);
        }

        public synchronized List<AddressListEntry> getClusterWithSelf() {
            ArrayList<AddressListEntry> es = new ArrayList<>();
            es.add(getSelfEntry());
            es.addAll(cluster);
            return es;
        }

        public ActorPlacementDefault(ActorSystem system) {
            this(system, PLACEMENT_NAME);
        }

        public ActorPlacementDefault(ActorSystem system, PlacementStrategy strategy) {
            this(system, PLACEMENT_NAME, strategy);
        }


        @Override
        protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilder builder) {
            return builder.matchWithSender(AddressList.class, this::receive)
                    .matchWithSender(ActorCreationRequest.class, this::create)
                    .match(LeaveEntry.class, this::receiveLeave);
        }

        public void join(ActorAddress.ActorAddressRemote primary) {
            ActorAddress.ActorAddressRemoteActor primaryActor;
            if (primary instanceof ActorAddress.ActorAddressRemoteActor) {
                primaryActor = (ActorAddress.ActorAddressRemoteActor) primary;
            } else {
                primaryActor = primary.getActor(getName()); //same name
            }
            try {
                int primaryThreads = ResponsiveCalls.sendHostTask(getSystem(), primaryActor, new CallablePrimaryThreads())
                        .get(20, TimeUnit.SECONDS);
                tell(new AddressList(
                            new AddressListEntry(primaryActor, primaryThreads)), this);
                log("join: %s, threads: %,d", primaryActor, primaryThreads);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        /**
         * @return normalized address
         */
        public ActorAddress.ActorAddressRemoteActor getSelfAddress() {
            if (getSystem() instanceof ActorSystemRemote) {
                ActorSystemRemote system = (ActorSystemRemote) getSystem();
                return system.normalizeHostAddress(system.getServerAddress()).getActor(getName());
            } else {
                return null;
            }
        }

        public AddressListEntry getSelfEntry() {
            ActorAddress.ActorAddressRemoteActor a = getSelfAddress();
            if (a == null) {
                return null;
            } else {
                return new AddressListEntry(a, getSystem().getThreads());
            }
        }

        public void receive(AddressList list, ActorRef sender) {
            logDebug("%s on %s receive: \n   %s \n from %s", this, getSystem(),
                    ConfigBase.lazyToString(() ->
                            list.getCluster().stream()
                            .map(Objects::toString)
                            .collect(Collectors.joining("\n   "))), sender);

            ActorAddress.ActorAddressRemoteActor self = getSelfAddress();

            boolean added = false;
            synchronized (this) {
                for (AddressListEntry a : list.getCluster()) {
                    int index = cluster.indexOf(a);
                    if ((index == -1 || !cluster.get(index).sameInfo(a)) &&
                            (self == null || !self.equals(a.getPlacementActor()))) {
                        if (index == -1) {
                            cluster.add(a);
                            log("receive: added %,d, %s", cluster.size(), a);
                        } else {
                            cluster.set(index, a);
                            log("receive: update %,d, %s", index, a);
                        }
                        added = true;
                    }
                }
            }
            if (added) {
                sendClusterToOthers(sender);
                updateTotalThreads();
            }
        }

        public void sendClusterToOthers(ActorRef sender) {
            List<ActorAddress.ActorAddressRemoteActor> excluded = new ArrayList<>();
            if (sender instanceof ActorRefRemote) {
                excluded.add((ActorAddress.ActorAddressRemoteActor) ((ActorRefRemote) sender).getAddress());
            }
            List<AddressListEntry> es = getCluster();
            AddressList addressListToOthers = new AddressList(es);
            AddressListEntry selfEntry = getSelfEntry();
            if (selfEntry != null) {
                addressListToOthers.getCluster().add(selfEntry);
            }
            for (AddressListEntry r : es) {
                if (!excluded.contains(r.getPlacementActor())) {
                    ActorRefRemote.get(getSystem(), r.getPlacementActor())
                            .tell(addressListToOthers, this);
                }
            }
        }

        protected void updateTotalThreads() {
            totalThreads = getSystem().getThreads() + getCluster().stream()
                    .mapToInt(AddressListEntry::getThreads)
                    .sum();
        }

        @Override
        public ActorRef place(Actor a) {
            long nl = strategy.getNextLocalNumber();
            return placeRetry( a, 0, nl, null);
        }

        public ActorRef placeRetry(Actor a, int retryCount, long nextLocalNumber, Serializable previous) {
            ActorAddress.ActorAddressRemoteActor target = strategy.getNextAddress(this, a, retryCount);
            if (target == null) {
                return placeLocal(a);
            }
            try {
                logDebug("%s on %s place(%d):\n   move %s to %s", this, getSystem(), retryCount, a, target);
                previous = toSerializable(a, nextLocalNumber, previous, target);
                ActorRef remote = ResponsiveCalls.<ActorRef>send(getSystem(),
                        target.ref(getSystem()),
                        new ActorCreationRequest(previous)).get(10, TimeUnit.SECONDS);
                if (a.getName() != null) {
                    system.unregister(a);
                }
                return remote;
            } catch (Throwable ex) {
                if (retryCount < getClusterSize()) {
                    return placeRetry(a, retryCount + 1, nextLocalNumber, previous);
                } else {
                    logger.log(logColor, ex, "placeRetry: %s retryCount=%,d nextLocalNum=%,d", a, retryCount, nextLocalNumber);
                    return placeLocal(a);
                }
            }
        }

        public CompletableFuture<ActorRef> place(Actor a, ActorAddress.ActorAddressRemoteActor targetPlaceActor) {
            Serializable data = toSerializable(a, strategy.getNextLocalNumber(), null, targetPlaceActor);
            return ResponsiveCalls.send(getSystem(),
                    targetPlaceActor.ref(getSystem()),
                    new ActorCreationRequest(data));
        }

        public synchronized int getClusterSize() {
            return cluster.size();
        }

        public void create(ActorCreationRequest c, ActorRef sender) {
            Object res;
            try {
                res = fromSerializable(c.getData(), strategy.getNextLocalNumber());
            } catch (Throwable ex) {
                res = new CallableMessage.CallableFailure(ex);
            }
            sender.tell(res, this);
            long n = createdActors.incrementAndGet();
            if (n > createdActorsNextLog) {
                log("created: %,d, last: %s", n, res);
                createdActorsNextLog *= 10L;
            }
        }

        protected ActorRef placeLocal(Actor a) {
            return a;
        }

        public abstract Serializable toSerializable(Actor a, long num, Serializable previous,
                                                    ActorAddress.ActorAddressRemoteActor target);

        public abstract Actor fromSerializable(Serializable s, long num);

        /** @return implementation field getter */
        public PlacementStrategy getStrategy() {
            return strategy;
        }

        public int getTotalThreads() {
            return totalThreads;
        }

        public AddressListEntry getEntry(ActorAddress address) {
            ActorAddress host = address.getHostAddress();
            return getCluster().stream()
                    .filter(e -> e.getPlacementActor().getHostAddress().equals(host))
                    .findFirst()
                    .orElse(host.equals(getSelfAddress().getHostAddress()) ? getSelfEntry() : null);
        }

        public ActorRef getPlace(ActorRef actor) {
            if (actor instanceof ActorRefRemote) {
                ActorAddress address = ((ActorRefRemote) actor).getAddress();
                AddressListEntry entry = getEntry(address);
                return entry.getPlacementActor().ref(getSystem());
            } else if (actor instanceof Actor) {
                return this;
            } else {
                return this; //
            }
        }

        public void close() {
            log("close");
            getSystem().unregister(this);
            LeaveEntry l = new LeaveEntry(getSelfAddress());
            cluster.forEach(e -> e.getPlacementActor()
                            .ref(getSystem()).tell(l, this));
        }

        public void receiveLeave(LeaveEntry l) {
            log("receive leave: %s", l);
            if (cluster.removeIf(e -> e.getPlacementActor().equals(l.getPlacementActor()))) {
                if (cluster.isEmpty()) {
                    allMemberLeft();
                }
            }
        }

        public void allMemberLeft() {
            if (shutdownWaitMsAfterAllMembersLeft >= 0) {
                log("all members left: schedule system close after %,d ms", shutdownWaitMsAfterAllMembersLeft);
                if (shutdownAfterWait != null) {
                    shutdownAfterWait.cancel(true);
                }
                shutdownAfterWait = getSystem().getScheduledExecutor()
                        .schedule(this::shutdownAfterWait, shutdownWaitMsAfterAllMembersLeft, TimeUnit.MILLISECONDS);
            }
        }

        public void setShutdownWaitMsAfterAllMembersLeft(long shutdownWaitMsAfterAllMembersLeft) {
            this.shutdownWaitMsAfterAllMembersLeft = shutdownWaitMsAfterAllMembersLeft;
        }

        public void shutdownAfterWait() {
            if (cluster.isEmpty()) {
                log("all members left: system close");
                shutdownAfterWait = null;
                getSystem().close();
            }
        }

        @Override
        public String toStringContents() {
            return (Objects.equals(name, PLACEMENT_NAME) ? "" : (name + ", ")) + system;
        }

        public long getCreatedActors() {
            return createdActors.get();
        }
    }

    class CallablePrimaryThreads implements CallableMessage<Actor, Integer> {
        public static final long serialVersionUID = 1L;
        @Override
        public Integer call(Actor self) {
            return self.getSystem().getThreads();
        }
    }

    class AddressList implements Serializable {
        public static final long serialVersionUID = 1L;
        public List<AddressListEntry> cluster;

        public AddressList() {}

        public AddressList(List<AddressListEntry> cluster) {
            this.cluster = cluster;
        }

        public AddressList(AddressListEntry r) {
            cluster = new ArrayList<>(1);
            cluster.add(r);
        }

        public List<AddressListEntry> getCluster() {
            return cluster;
        }

        @Override
        public String toString() {
            return "AddressList{" +
                    cluster +
                    '}';
        }
    }

    class AddressListEntry implements Serializable, ToJson {
        public static final long serialVersionUID = 1L;
        public ActorAddress.ActorAddressRemoteActor placementActor;
        public int threads;

        public AddressListEntry() {
            this(null, 0);
        }

        public AddressListEntry(ActorAddress.ActorAddressRemoteActor placementActor, int threads) {
            this.placementActor = placementActor;
            this.threads = threads;
        }

        public ActorAddress.ActorAddressRemoteActor getPlacementActor() {
            return placementActor;
        }

        public int getThreads() {
            return threads;
        }

        @Override
        public String toString() {
            return "{" +
                    "placementActor=" + placementActor +
                    ", threads=" + threads +
                    '}';
        }

        public boolean sameInfo(AddressListEntry e) {
            return threads == e.getThreads();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AddressListEntry that = (AddressListEntry) o;
            return Objects.equals(placementActor, that.placementActor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(placementActor);
        }

        @Override
        public Map<String, Object> toJson(Function<Object, Object> valueConverter) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("placementActor", toJson(valueConverter, placementActor, ""));
            json.put("threads", toJson(valueConverter, (long) threads));
            return json;
        }
    }

    class ActorCreationRequest implements Serializable {
        public static final long serialVersionUID = 1L;
        public Serializable data;

        public ActorCreationRequest() {}

        public ActorCreationRequest(Serializable data) {
            this.data = data;
        }

        public Serializable getData() {
            return data;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this)) +
                    "(" + (data == null ? "null" : (data.getClass().getSimpleName() + "@" + System.identityHashCode(data))) +
                    ')';
        }
    }

    class LeaveEntry implements Serializable {
        public static final long serialVersionUID = 1L;
        public ActorAddress.ActorAddressRemoteActor placementActor;

        public LeaveEntry() {}

        public LeaveEntry(ActorAddress.ActorAddressRemoteActor placementActor) {
            this.placementActor = placementActor;
        }

        public ActorAddress.ActorAddressRemoteActor getPlacementActor() {
            return placementActor;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + placementActor + ")";
        }
    }

    interface PlacementStrategy {
        ActorAddress.ActorAddressRemoteActor getNextAddress(ActorPlacementDefault pa, Actor a, int retryCount);
        long getNextLocalNumber();
        String getDescription();
    }

    class PlacementStrategyUndertaker implements PlacementStrategy {
        protected AtomicLong localNum = new AtomicLong();
        @Override
        public ActorAddress.ActorAddressRemoteActor getNextAddress(ActorPlacementDefault pa, Actor a, int retryCount) {
            return pa.getSelfAddress();
        }

        @Override
        public long getNextLocalNumber() {
            return localNum.incrementAndGet();
        }

        @Override
        public String getDescription() {
            return "undertaker(" + localNum + ")";
        }
    }

    class PlacementStrategyRoundRobinThreads implements PlacementStrategy {
        protected Map<AddressListEntry, Long> count = new HashMap<>();
        protected AtomicLong localNum = new AtomicLong();
        protected int clusterIndex;

        //it might re-enter the method
        @Override
        public synchronized ActorAddress.ActorAddressRemoteActor getNextAddress(ActorPlacementDefault pa, Actor a, int retryCount) {
            boolean first = true;
            while (true) {
                int i = clusterIndex;
                AddressListEntry e;
                if (i <= 0) { //local
                    e = pa.getSelfEntry();
                } else if (i - 1 < pa.getCluster().size()) {
                    e = pa.getCluster().get(i - 1);
                } else {
                    clusterIndex = 0;
                    e = pa.getSelfEntry();
                }
                long n = count.computeIfAbsent(e, k -> 0L);
                if (n + 1L < e.getThreads()) {
                    count.put(e, n + 1L);
                    return e.getPlacementActor();
                } else if (first) {
                    ++clusterIndex;
                    if (clusterIndex - 1 >= pa.getCluster().size()) {
                        clusterIndex = 0;
                    }
                    first = false;
                } else {
                    break;
                }
            }
            return pa.getSelfAddress();
        }

        @Override
        public long getNextLocalNumber() {
            return localNum.incrementAndGet();
        }

        @Override
        public String getDescription() {
            return "roundRobinThreads(local=" + localNum + ",clusterIdx=" + clusterIndex + ")";
        }

        /** @return implementation field getter */
        public int getClusterIndex() {
            return clusterIndex;
        }

        /** @return implementation field getter */
        public Map<AddressListEntry, Long> getCount() {
            return count;
        }

        /** @return implementation field getter */
        public AtomicLong getLocalNum() {
            return localNum;
        }
    }

    class PlacementStrategyRoundRobin implements PlacementStrategy {
        protected long totalCount;
        protected int localLimit;
        protected int localCount = 0;
        protected int clusterCount = 0;
        protected int currentClusterCount = 0;

        public PlacementStrategyRoundRobin(int localLimit) {
            this.localLimit = localLimit;
        }

        public PlacementStrategyRoundRobin() {
            this(1000);
        }

        @Override
        public synchronized ActorAddress.ActorAddressRemoteActor getNextAddress(ActorPlacementDefault pa, Actor a, int retryCount) {
            if (localCount < localLimit) {
                ++localCount;
                return null; //local
            } else {
                List<AddressListEntry> cluster = pa.getCluster();
                if (cluster.isEmpty()) {
                    ++localCount;
                    return null;
                }
                if (retryCount > 0) { //retry skips current cluster
                    ++clusterCount;
                    currentClusterCount = 0;
                }

                int clusterIndex = clusterCount;
                if (clusterIndex >= cluster.size()) {
                    clusterIndex = clusterIndex % cluster.size();
                    clusterCount = clusterIndex;
                }
                if (currentClusterCount < localLimit) {
                    ++currentClusterCount;
                } else {
                    ++clusterCount;
                    currentClusterCount = 0;
                }
                return cluster.get(clusterIndex).getPlacementActor();
            }
        }

        @Override
        public synchronized long getNextLocalNumber() {
            ++totalCount;
            return totalCount;
        }

        @Override
        public String getDescription() {
            return "roundRobin(total=" + totalCount + ",localLimit=" + localLimit + ")";
        }

        /** @return implementation field getter */
        public int getLocalLimit() {
            return localLimit;
        }

        /** @return implementation field getter */
        public long getTotalCount() {
            return totalCount;
        }

        /** @return implementation field getter */
        public int getLocalCount() {
            return localCount;
        }

        /** @return implementation field getter */
        public int getClusterCount() {
            return clusterCount;
        }

        /** @return implementation field getter */
        public int getCurrentClusterCount() {
            return currentClusterCount;
        }
    }
}
