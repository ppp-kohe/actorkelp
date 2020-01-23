package csl.actor.msgassoc;

import csl.actor.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface ActorPlacement {
    String PLACEMENT_NAME = ActorPlacement.class.getName() + ".placement";

    ActorRef place(Actor a);

    abstract class PlacemenActor extends ActorDefault implements ActorPlacement {
        protected List<ActorAddress.ActorAddressRemoteActor> cluster = new ArrayList<>();
        protected PlacementStrategy strategy;

        public PlacemenActor(ActorSystem system, String name) {
            super(system, name);
            if (system instanceof ActorSystemRemote) {
                cluster.add(((ActorSystemRemote) system).getServerAddress().getActor(name));
            }
            strategy = initStrategy();
        }

        abstract PlacementStrategy initStrategy();

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

        public void receive(AddressList list, ActorRef sender) {
            boolean added = false;
            for (ActorAddress.ActorAddressRemoteActor a : list.getCluster()) {
                if (!cluster.contains(a)) {
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
            if (getSystem() instanceof ActorSystemRemote) {
                excluded.add(((ActorSystemRemote) getSystem()).getServerAddress().getActor(getName()));
            }
            for (ActorAddress.ActorAddressRemoteActor r : cluster) {
                if (!excluded.contains(r)) {
                    ActorRefRemote.get(getSystem(), r.getActor(getName()))
                            .tell(new AddressList(cluster), this);
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
            if (retryCount > 0) {
                count += (localLimit - count % localLimit);
            }
            if (0 <= count && count < localLimit) {
                ++count;
                return null; //local
            } else {
                int clusterIndex = count / localLimit;
                ++count;
                List<ActorAddress.ActorAddressRemoteActor> cluster = pa.getCluster();
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

        public int getLocalLimit() {
            return localLimit;
        }

        public int getCount() {
            return count;
        }

        public long getTotalCount() {
            return totalCount;
        }
    }
}
