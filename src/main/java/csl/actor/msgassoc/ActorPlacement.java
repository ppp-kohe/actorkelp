package csl.actor.msgassoc;

import csl.actor.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
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

    abstract class PlacemenActor extends ActorDefault implements ActorPlacement {
        protected List<AddressListEntry> cluster = new ArrayList<>();
        protected PlacementStrategy strategy;
        protected volatile int totalThreads;

        public static boolean debugLog = System.getProperty("csl.actor.debug", "false").equals("true");

        public static void log(String msg, Object... args) {
            if (debugLog) {
                System.err.println("\033[38;5;34m" + Instant.now() + ": " + String.format(msg, args) + "\033[0m");
            }
        }

        public PlacemenActor(ActorSystem system, String name) {
            super(system, name);
            strategy = initStrategy();
            totalThreads = system.getThreads();
        }

        public PlacemenActor(ActorSystem system, String name, PlacementStrategy strategy) {
            super(system, name);
            this.strategy = strategy;
            totalThreads = system.getThreads();
        }

        protected abstract PlacementStrategy initStrategy();

        public List<AddressListEntry> getCluster() {
            return cluster;
        }

        public PlacemenActor(ActorSystem system) {
            this(system, PLACEMENT_NAME);
        }

        public PlacemenActor(ActorSystem system, PlacementStrategy strategy) {
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
                int masterThreads = ResponsiveCalls.send(getSystem(), masterActor, (self, sender) ->
                        self.getSystem().getThreads()).get(2, TimeUnit.SECONDS);
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
            AddressList addressListToOthers = new AddressList(new ArrayList<>(cluster));
            AddressListEntry selfEntry = getSelfEntry();
            if (selfEntry != null) {
                addressListToOthers.getCluster().add(selfEntry);
            }
            for (AddressListEntry r : cluster) {
                if (!excluded.contains(r.getPlacementActor())) {
                    ActorRefRemote.get(getSystem(), r.getPlacementActor())
                            .tell(addressListToOthers, this);
                }
            }
        }

        protected void updateTotalThreads() {
            totalThreads = getSystem().getThreads() + cluster.stream()
                    .mapToInt(AddressListEntry::getThreads)
                    .sum();
        }

        @Override
        public ActorRef place(Actor a) {
            Serializable s = toSerializable(a, strategy.getNextLocalNumber());
            if (s != null) {
                return placeRetry(s, a, 0);
            } else {
                return placeLocal(a);
            }
        }

        public ActorRef placeRetry(Serializable s, Actor a, int retryCount) {
            ActorAddress.ActorAddressRemoteActor target = strategy.getNextAddress(this, a, retryCount);
            if (target == null) {
                return placeLocal(a);
            }
            try {
                log("%s on %s place(%d):\n   move %s to %s", this, getSystem(), retryCount, a, target);
                return ResponsiveCalls.<ActorRef>send(getSystem(),
                        ActorRefRemote.get(getSystem(), target),
                        new ActorCreationRequest(s)).get(2, TimeUnit.SECONDS);
            } catch (Throwable ex) {
                if (retryCount < cluster.size()) {
                    return placeRetry(s, a, retryCount + 1);
                } else {
                    ex.printStackTrace();
                    return placeLocal(a);
                }
            }
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

        public abstract Serializable toSerializable(Actor a, long num);

        public abstract Actor fromSerializable(Serializable s, long num);

        /** @return implementation field getter */
        public PlacementStrategy getStrategy() {
            return strategy;
        }

        public int getTotalThreads() {
            return totalThreads;
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
    }

    interface PlacementStrategy {
        ActorAddress.ActorAddressRemoteActor getNextAddress(PlacemenActor pa, Actor a, int retryCount);
        long getNextLocalNumber();
    }

    class PlacementStrategyRoundRobinThreads implements PlacementStrategy {
        protected Map<AddressListEntry, Long> count = new HashMap<>();
        protected long localNum;
        protected int clusterIndex;

        //it might re-enter the method
        @Override
        public synchronized ActorAddress.ActorAddressRemoteActor getNextAddress(PlacemenActor pa, Actor a, int retryCount) {
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
            localNum++;
            return localNum;
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
        public long getLocalNum() {
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
        public synchronized ActorAddress.ActorAddressRemoteActor getNextAddress(PlacemenActor pa, Actor a, int retryCount) {
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
