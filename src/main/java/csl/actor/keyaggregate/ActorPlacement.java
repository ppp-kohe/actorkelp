package csl.actor.keyaggregate;

import csl.actor.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public interface ActorPlacement {
    String PLACEMENT_NAME = ActorPlacement.class.getName() + ".placement";

    ActorRef place(Actor a);

    static Object lazyToString(Supplier<String> s) {
        return new Object() {
            @Override
            public String toString() {
                return s.get();
            }
        };
    }

    abstract class ActorPlacementDefault extends ActorDefault implements ActorPlacement {
        protected List<AddressListEntry> cluster = new ArrayList<>();
        protected PlacementStrategy strategy;
        protected volatile int totalThreads;

        public static boolean debugLog = System.getProperty("csl.actor.debug", "false").equals("true");

        public static void log(String msg, Object... args) {
            if (debugLog) {
                System.err.println("\033[38;5;34m" + Instant.now() + ": " + String.format(msg, args) + "\033[0m");
            }
        }

        public ActorPlacementDefault(ActorSystem system, String name) {
            super(system, name);
            strategy = initStrategy();
            totalThreads = system.getThreads();
            ResponsiveCalls.initCallableTarget(system);
        }

        public ActorPlacementDefault(ActorSystem system, String name, PlacementStrategy strategy) {
            super(system, name);
            this.strategy = strategy;
            totalThreads = system.getThreads();
            ResponsiveCalls.initCallableTarget(system);
        }

        protected abstract PlacementStrategy initStrategy();

        public synchronized List<AddressListEntry> getCluster() {
            return new ArrayList<>(cluster);
        }

        public ActorPlacementDefault(ActorSystem system) {
            this(system, PLACEMENT_NAME);
        }

        public ActorPlacementDefault(ActorSystem system, PlacementStrategy strategy) {
            this(system, PLACEMENT_NAME, strategy);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchWithSender(AddressList.class, this::receive)
                    .matchWithSender(ActorCreationRequest.class, this::create)
                    .build();
        }

        public void join(ActorAddress.ActorAddressRemote master) {
            ActorAddress.ActorAddressRemoteActor masterActor;
            if (master instanceof ActorAddress.ActorAddressRemoteActor) {
                masterActor = (ActorAddress.ActorAddressRemoteActor) master;
            } else {
                masterActor = master.getActor(getName()); //same name
            }
            try {
                int masterThreads = ResponsiveCalls.sendHostTask(getSystem(), masterActor, new CallableMasterThreads())
                        .get(20, TimeUnit.SECONDS);
                tell(new AddressList(
                            new AddressListEntry(masterActor, masterThreads)), this);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public ActorAddress.ActorAddressRemoteActor getSelfAddress() {
            if (getSystem() instanceof ActorSystemRemote) {
                return ((ActorSystemRemote) getSystem()).getServerAddress().getActor(getName());
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
            log("%s on %s receive: \n   %s \n from %s", this, getSystem(),
                    ActorPlacement.lazyToString(() ->
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
                        } else {
                            cluster.set(index, a);
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
                log("%s on %s place(%d):\n   move %s to %s", this, getSystem(), retryCount, a, target);
                previous = toSerializable(a, nextLocalNumber, previous, target);
                return ResponsiveCalls.<ActorRef>send(getSystem(),
                        ActorRefRemote.get(getSystem(), target),
                        new ActorCreationRequest(previous)).get(10, TimeUnit.SECONDS);
            } catch (Throwable ex) {
                if (retryCount < getClusterSize()) {
                    return placeRetry(a, retryCount + 1, nextLocalNumber, previous);
                } else {
                    ex.printStackTrace();
                    return placeLocal(a);
                }
            }
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
    }

    class CallableMasterThreads implements CallableMessage<Actor, Integer> {
        @Override
        public Integer call(Actor self, ActorRef sender) {
            return self.getSystem().getThreads();
        }
    }

    class AddressList implements Serializable {
        protected List<AddressListEntry> cluster;

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

    class AddressListEntry implements Serializable {
        protected ActorAddress.ActorAddressRemoteActor placementActor;
        protected int threads;

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
    }

    class ActorCreationRequest implements Serializable {
        protected Serializable data;

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

    interface PlacementStrategy {
        ActorAddress.ActorAddressRemoteActor getNextAddress(ActorPlacementDefault pa, Actor a, int retryCount);
        long getNextLocalNumber();
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
        protected int count = 0;
        protected int localLimit;

        public PlacementStrategyRoundRobin(int localLimit) {
            this.localLimit = localLimit;
        }

        public PlacementStrategyRoundRobin() {
            this(1000);
        }

        @Override
        public synchronized ActorAddress.ActorAddressRemoteActor getNextAddress(ActorPlacementDefault pa, Actor a, int retryCount) {
            if (retryCount > 0 && localLimit != 0) {
                count += (localLimit - count % localLimit);
            }
            if (0 <= count && count < localLimit) {
                ++count;
                return null; //local
            } else {
                List<AddressListEntry> cluster = pa.getCluster();
                if (cluster.isEmpty()) {
                    return null;
                }
                int clusterIndex = (localLimit == 0 ? (count % cluster.size()) : (count / localLimit));
                ++count;
                if (0 <= clusterIndex && clusterIndex < cluster.size()) {
                    return cluster.get(clusterIndex).getPlacementActor();
                } else {
                    count = 0;
                    return getNextAddress(pa, a, 0);
                }
            }
        }

        @Override
        public synchronized long getNextLocalNumber() {
            ++totalCount;
            ++count;
            return totalCount;
        }

        /** @return implementation field getter */
        public int getLocalLimit() {
            return localLimit;
        }

        /** @return implementation field getter */
        public int getCount() {
            return count;
        }

        /** @return implementation field getter */
        public long getTotalCount() {
            return totalCount;
        }
    }
}
