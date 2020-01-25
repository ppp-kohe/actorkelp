package csl.actor.msgassoc;

import csl.actor.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
        protected List<ActorAddress.ActorAddressRemoteActor> cluster = new ArrayList<>();
        protected PlacementStrategy strategy;

        public static boolean debugLog = System.getProperty("csl.actor.debug", "false").equals("true");

        public static void log(String msg, Object... args) {
            if (debugLog) {
                System.err.println("\033[38;5;34m" + Instant.now() + ": " + String.format(msg, args) + "\033[0m");
            }
        }

        public PlacemenActor(ActorSystem system, String name) {
            super(system, name);
            strategy = initStrategy();
        }

        protected abstract PlacementStrategy initStrategy();

        public List<ActorAddress.ActorAddressRemoteActor> getCluster() {
            return cluster;
        }

        public PlacemenActor(ActorSystem system) {
            this(system, PLACEMENT_NAME);
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
            receive(new AddressList(masterActor), null);
        }

        public ActorAddress.ActorAddressRemoteActor getSelfAddress() {
            if (getSystem() instanceof ActorSystemRemote) {
                return ((ActorSystemRemote) getSystem()).getServerAddress().getActor(getName());
            } else {
                return null;
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
            for (ActorAddress.ActorAddressRemoteActor a : list.getCluster()) {
                if (!cluster.contains(a) && (self == null || !self.equals(a))) {
                    cluster.add(a);
                    added = true;
                }
            }
            if (added) {
                sendClusterToOthers(sender);
            }
        }

        public void sendClusterToOthers(ActorRef sender) {
            List<ActorAddress.ActorAddressRemoteActor> excluded = new ArrayList<>();
            if (sender instanceof ActorRefRemote) {
                excluded.add((ActorAddress.ActorAddressRemoteActor) ((ActorRefRemote) sender).getAddress());
            }
            AddressList addressListToOthers = new AddressList(new ArrayList<>(cluster));
            ActorAddress.ActorAddressRemoteActor selfAddress = getSelfAddress();
            if (selfAddress != null) {
                addressListToOthers.getCluster().add(selfAddress);
            }
            for (ActorAddress.ActorAddressRemoteActor r : cluster) {
                if (!excluded.contains(r)) {
                    ActorRefRemote.get(getSystem(), r.getActor(getName()))
                            .tell(addressListToOthers, this);
                }
            }
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
                res = new ResponsiveCalls.ResponsiveFailure(ex);
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
    }

    class AddressList implements Serializable {
        protected List<ActorAddress.ActorAddressRemoteActor> cluster;

        public AddressList(List<ActorAddress.ActorAddressRemoteActor> cluster) {
            this.cluster = cluster;
        }

        public AddressList(ActorAddress.ActorAddressRemoteActor r) {
            cluster = new ArrayList<>(1);
            cluster.add(r);
        }

        public List<ActorAddress.ActorAddressRemoteActor> getCluster() {
            return cluster;
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
                List<ActorAddress.ActorAddressRemoteActor> cluster = pa.getCluster();
                int clusterIndex = (localLimit == 0 ? (count % cluster.size()) : (count / localLimit));
                ++count;
                if (0 <= clusterIndex && clusterIndex < cluster.size()) {
                    return cluster.get(clusterIndex);
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
