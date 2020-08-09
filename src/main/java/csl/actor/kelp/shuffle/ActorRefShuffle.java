package csl.actor.kelp.shuffle;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ActorKelpFunctions;
import csl.actor.kelp.ActorKelpFunctions.KeyExtractor;
import csl.actor.kelp.KelpStage;
import csl.actor.kelp.behavior.ActorBehaviorKelp;
import csl.actor.kelp.behavior.KelpDispatcher;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.util.ResponsiveCalls;
import csl.actor.util.StagingActor;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ActorRefShuffle implements ActorRef, Serializable, Cloneable, KryoSerializable,
        StagingActor.StagingNonSubject, KelpDispatcher.DispatchRef {
    public static final long serialVersionUID = 1L;
    protected transient ActorSystem system;
    protected int bufferSize;
    @Deprecated protected boolean hostIncludePort;
    @Deprecated protected Map<ActorAddress, List<ShuffleEntry>> entries; //host-address to entries
    @Deprecated protected List<List<ShuffleEntry>> entriesList;
    @Deprecated protected List<KeyExtractor<?,?>> keyExtractors;
    protected List<ShuffleEntry> dispatchUnits;
    protected List<ActorBehaviorKelp.KeyExtractorsAndDispatcher> extractorsAndDispatchers;
    @Deprecated protected int shuffleSize;


    public static List<ShuffleEntry> createDispatchUnits(Iterable<? extends ActorRef> actors, int bufferSize) {
        ArrayList<ShuffleEntry> refs = new ArrayList<>();
        int i = 0;
        for (ActorRef ref : actors) {
            refs.add(new ShuffleEntry(ref, bufferSize, i));
            ++i;
        }
        refs.trimToSize();
        return refs;
    }

    @Deprecated
    public static Function<ActorRef, ActorAddress> refToHost(boolean hostIncludePort) {
        return hostIncludePort ? ActorRefShuffle::toHost : ActorRefShuffle::toHostWithoutPort;
    }

    @Deprecated
    public static Map<ActorAddress, List<ShuffleEntry>> createEntries(Iterable<? extends ActorRef> actors, int bufferSize,
                                                                      Function<ActorRef, ActorAddress> refToHost) {
        Map<ActorAddress, List<ShuffleEntry>> refs = new HashMap<>();
        int i = 0;
        for (ActorRef ref : actors) {
            refs.computeIfAbsent(refToHost.apply(ref), _h -> new ArrayList<>())
                    .add(new ShuffleEntry(ref, bufferSize, i));
            ++i;
        }
        refs.values().forEach(es ->
                ((ArrayList<?>) es).trimToSize());
        return refs;
    }

    @Deprecated
    public static ActorAddress.ActorAddressRemote LOCALHOST = ActorAddress.get("localhost", -1);

    @Deprecated
    public static ActorAddress.ActorAddressRemote toHost(ActorRef ref) {
        if (ref instanceof ActorRefRemote) {
            return ((ActorRefRemote) ref).getAddress().getHostAddress();
        } else {
            return LOCALHOST;
        }
    }

    @Deprecated
    public static ActorAddress.ActorAddressRemote toHostWithoutPort(ActorRef ref) {
        return toHostWithoutPort(toHost(ref));
    }
    @Deprecated
    public static ActorAddress.ActorAddressRemote toHostWithoutPort(ActorAddress.ActorAddressRemote a) {
        if (a.getPort() != -1) {
            return ActorAddress.get(a.getHost(), -1);
        } else {
            return a;
        }
    }


    public static void flush(ActorRef ref, ActorRef sender) {
        if (ref instanceof ActorRefShuffle) {
            ((ActorRefShuffle) ref).flush(sender);
        }
    }

    ////////////

    public ActorRefShuffle() {
    }
    @Deprecated
    public ActorRefShuffle(ActorSystem system, Map<ActorAddress, List<ShuffleEntry>> entries, List<KeyExtractor<?,?>> keyExtractors, int bufferSize, boolean hostIncludePort) {
        this.system = system;
        this.keyExtractors = keyExtractors;
        this.bufferSize = bufferSize;
        this.hostIncludePort = hostIncludePort;
        initEntries(entries);
    }

    public ActorRefShuffle(ActorSystem system, List<ShuffleEntry> entries, List<ActorBehaviorKelp.KeyExtractorsAndDispatcher> extractorsAndDispatchers, int bufferSize) {
        this.system = system;
        this.bufferSize = bufferSize;
        initExtractorsAndDispatchers(extractorsAndDispatchers);
        initDispatchUnits(entries);
    }

    protected void initDispatchUnits(List<ShuffleEntry> entries) {
        this.dispatchUnits = entries;
    }

    protected void initExtractorsAndDispatchers(List<ActorBehaviorKelp.KeyExtractorsAndDispatcher> extractorsAndDispatchers) {
        this.extractorsAndDispatchers = extractorsAndDispatchers;
    }

    @Deprecated
    protected void initEntries(Map<ActorAddress, List<ShuffleEntry>> entries) {
        this.entries = entries;
        entriesList = new ArrayList<>(entries.values());
    }

    /**
     * do {@link #flush()} before returning.
     *  The method will cause sending remaining messages to member actors,
     *    before starting StagingWatcher for the member actors
     * @return {@link #getMemberActors()}
     */
    @Override
    public List<ActorRef> getStagingSubjectActors() {
        flush();
        return getMemberActors();
    }

    public List<ActorRef> getMemberActors() {
        return dispatchUnits.stream()
                .map(ShuffleEntry::getActor)
                .collect(Collectors.toList());
    }

    @Override
    public List<ShuffleEntry> getDispatchUnits() {
        return dispatchUnits;
    }

    @Override
    public int getDispatchUnitSize() {
        return dispatchUnits.size();
    }

    @Override
    public KelpDispatcher.DispatchUnit getDispatchUnit(int index) {
        return dispatchUnits.get(index);
    }

    public int getBufferSize() {
        return bufferSize;
    }

    @Deprecated
    public boolean isHostIncludePort() {
        return hostIncludePort;
    }

    @Deprecated
    public List<KeyExtractor<?, ?>> getKeyExtractors() {
        return keyExtractors;
    }

    public ActorRefShuffle use() {
//        Map<ActorAddress, List<ShuffleEntry>> es = new HashMap<>(entries.size());
//        entries.forEach((k,v) ->
//                es.put(k, v.stream()
//                    .map(ShuffleEntry::copy)
//                    .collect(Collectors.toList())));
        return copyForUse(
                dispatchUnits.stream()
                    .map(ShuffleEntry::copy)
                    .collect(Collectors.toList()),
                extractorsAndDispatchers.stream()
                    .map(ActorBehaviorKelp.KeyExtractorsAndDispatcher::copy)
                    .collect(Collectors.toList()));
    }

    protected ActorRefShuffle copyForUse(
            List<ShuffleEntry> dispatchUnits,
            List<ActorBehaviorKelp.KeyExtractorsAndDispatcher> extractorsAndDispatchers) {
        try {
            ActorRefShuffle ref = (ActorRefShuffle) super.clone();
            //ref.initEntries(es);
            ref.initDispatchUnits(dispatchUnits);
            ref.initExtractorsAndDispatchers(this.extractorsAndDispatchers);
            return ref;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void tellMessage(Message<?> message) {
        if (message instanceof ActorKelp.MessageBundle) {
            ((ActorKelp.MessageBundle<?>) message).getData().forEach(d ->
                    tell(d, message.getSender()));
        } else if (KelpStage.isMessageBroadcastedImpl(message)) {
            getMemberActors().forEach(a ->
                    a.tellMessage(message));
        } else {
            tellMessageShuffle(message);
        }
    }

    public void tellMessageShuffle(Message<?> message) {
        tellMessageShuffle(this, extractorsAndDispatchers, message);
    }

    @SuppressWarnings("unchecked")
    public static void tellMessageShuffle(KelpDispatcher.DispatchRef self, List<ActorBehaviorKelp.KeyExtractorsAndDispatcher> extractorsAndDispatchers,
                                          Message<?> message) {
        Object data = message.getData();
        for (ActorBehaviorKelp.KeyExtractorsAndDispatcher e : extractorsAndDispatchers) {
            for (KeyExtractor<?,?> ke : e.getKeyExtractors()) {
                if (ke.matchValue(data)) {
                    e.getDispatcher().dispatch(self, (KeyExtractor<?, Object>) ke, message);
                    return;
                }
            }
        }
        DEFAULT_DISPATCHER.dispatch(self, DEFAULT_KEY_EXTRACTOR, message);
    }

    public static ActorKelpFunctions.KeyExtractorClass<?,Object> DEFAULT_KEY_EXTRACTOR = new ActorKelpFunctions.KeyExtractorClass<>(Object.class,
            ActorKelpFunctions.KeyExtractorFunction.identity());

    public static KelpDispatcher.DispatcherShuffle DEFAULT_DISPATCHER = new KelpDispatcher.DispatcherShuffle();

    @Deprecated
    protected List<ShuffleEntry> entries(Message<?> message, Object key, int keyHash) {
        ActorAddress target = addressFromMessageData(message, key);
        List<ShuffleEntry> es = (target == null ? null : entries.get(target));
        if (es == null) {
            es = entriesList.get(KelpDispatcher.DispatcherShuffle.hashMod(keyHash, entriesList.size()));
        }
        return es;
    }

    @Deprecated
    protected ActorAddress addressFromMessageData(Message<?> message, Object key) {
        return null;
    }

    public void flush() {
        this.flush(null);
    }

    public void flush(ActorRef sender) {
        dispatchUnits.forEach(e -> e.flush(sender));
    }

    public CompletableFuture<Void> connectStage(ActorRef next) {
        return connectStageWithoutInit(
                connectStageInitialActor(next, Integer.MAX_VALUE));
    }

    public CompletableFuture<Void> connectStageWithoutInit(ActorRef next) {
        List<CompletableFuture<?>> tasks = new ArrayList<>();
        for (ActorRef a : getMemberActors()) {
            tasks.add(setNextStage(system, a, next));
        }
        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0]));
    }

    public ActorRef connectStageInitialActor(ActorRef next, int bufferSizeMax) {
        return connectStageInitialActor(system, next, bufferSizeMax);
    }

    public static ActorRef connectStageInitialActor(ActorSystem system, ActorRef next, int bufferSizeMax) {
        if (next instanceof ActorKelp<?> && !((ActorKelp<?>) next).isUnit()) {
            return ((ActorKelp<?>) next).shuffle(bufferSizeMax);
        } else if (!(next instanceof Actor || next instanceof ActorRefShuffle)) { //remote or local ref
            try {
                return ResponsiveCalls.sendTask(system, next, new ToShuffleTask(bufferSizeMax)).get();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } else {
            return next;
        }
    }

    public static class ToShuffleTask implements CallableMessage<Actor, ActorRef> {
        public static final long serialVersionUID = 1L;
        protected int bufferSizeMax;

        public ToShuffleTask() {}

        public ToShuffleTask(int bufferSizeMax) {
            this.bufferSizeMax = bufferSizeMax;
        }

        @Override
        public ActorRef call(Actor self) {
            if (self instanceof ActorKelp<?> && !((ActorKelp<?>) self).isUnit()) {
                return ((ActorKelp<?>) self).shuffle(bufferSizeMax);
            } else {
                return self;
            }
        }
    }

    public static CompletableFuture<?> setNextStage(ActorSystem system, ActorRef a, ActorRef next) {
        return ResponsiveCalls.sendTaskConsumer(system, a, new ConnectTask(next));
    }

    public static class ConnectTask implements CallableMessage.CallableMessageConsumer<Actor> {
        public static final long serialVersionUID = 1L;
        protected ActorRef next;

        public ConnectTask() {}

        public ConnectTask(ActorRef next) {
            this.next = next;
        }

        @Override
        public void accept(Actor self) {
            if (self instanceof StagingActor.StagingSupported) {
                ActorRef nr = next;
                if (next instanceof ActorRefShuffle) {
                    nr = ((ActorRefShuffle) next).use();
                }
                ((StagingActor.StagingSupported) self).setNextStage(nr);
            }
        }
    }

    @Override
    public String toString() {
        int as = getDispatchUnitSize();
        return "refShuffle[" + as + "](" +
                "actors=" + (as == 0 ? "[]" : "[" + getFirstActor() + ",...]") +
                ", bufferSize=" + bufferSize +
                ')';
    }

    @Deprecated
    public int getShuffleSize() {
        return shuffleSize;
    }

    public ActorRef getFirstActor() {
        for (ShuffleEntry e : dispatchUnits) {
            return e.getActor();
        }
        return null;
    }

    public static class ShuffleEntry implements Serializable, KelpDispatcher.DispatchUnit, KryoSerializable {
        public static final long serialVersionUID = 1L;
        protected int bufferSize;
        protected int index;
        protected ActorRef actor;
        protected transient ArrayList<Object> buffer;

        public ShuffleEntry() {}

        public ShuffleEntry(ActorRef actor, int bufferSize, int index) {
            this.actor = actor;
            this.bufferSize = bufferSize;
            buffer = new ArrayList<>(bufferSize);
            this.index = index;
        }

        public ActorRef getActor() {
            return actor;
        }

        public List<Object> getBuffer() {
            return buffer;
        }

        public ShuffleEntry copy() {
            return new ShuffleEntry(actor, bufferSize, index);
        }

        @Override
        public int getIndex() {
            return index;
        }

        public int getBufferSize() {
            return bufferSize;
        }

        @Override
        public void tell(Object data) {
            tell(data, null);
        }

        @Override
        public void tell(Object data, ActorRef sender) {
            if (bufferSize <= 0) {
                actor.tellMessage(new KelpDispatcher.MessageAccepted<>(actor, sender, data));
            } else {
                if (buffer == null) {
                    buffer = new ArrayList<>(bufferSize);
                }
                buffer.add(data);
                if (buffer.size() >= bufferSize) { //suppose the ref is not shared
                    actor.tellMessage(new ActorKelp.MessageBundle<>(actor, sender, buffer));
                    buffer.clear();
                }
            }
        }

        @Override
        public void tellMessage(Message<?> message) {
            tell(message.getData(), message.getSender());
        }

        @Override
        public void flush() {}

        @Override
        public void flush(ActorRef sender) {
            if (buffer != null && !buffer.isEmpty()) {
                actor.tellMessage(new ActorKelp.MessageBundle<>(actor, sender, buffer));
                buffer.clear();
            }
        }

        @Override
        public void write(Kryo kryo, Output output) {
            kryo.writeClassAndObject(output, actor);
            output.writeInt(bufferSize);
            output.writeInt(index);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            actor = (ActorRef) kryo.readClassAndObject(input);
            bufferSize = input.readInt();
            index = input.readInt();
            buffer = new ArrayList<>(bufferSize);
        }
    }

    public void forEach(Consumer<KelpDispatcher.DispatchUnit> task) {
        dispatchUnits.forEach(task);
    }



    ///////////

    public void setSystemBySerializer(ActorSystem system) {
        this.system = system;
    }

    public ActorSystem getSystem() {
        return system;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        //kryo.writeClassAndObject(output, entries);
        //kryo.writeClassAndObject(output, entriesList);
        //kryo.writeClassAndObject(output, keyExtractors);
        kryo.writeClassAndObject(output, dispatchUnits);
        kryo.writeClassAndObject(output, extractorsAndDispatchers);
        output.writeInt(bufferSize);
        //output.writeBoolean(hostIncludePort);
        //output.writeInt(shuffleSize);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void read(Kryo kryo, Input input) {
        //this.entries = (Map<ActorAddress,List<ShuffleEntry>>) kryo.readClassAndObject(input);
        //this.entriesList = (List<List<ShuffleEntry>>) kryo.readClassAndObject(input);
        //this.keyExtractors = (List<KeyExtractor<?,?>>) kryo.readClassAndObject(input);
        this.dispatchUnits = (List<ShuffleEntry>) kryo.readClassAndObject(input);
        this.extractorsAndDispatchers = (List<ActorBehaviorKelp.KeyExtractorsAndDispatcher>) kryo.readClassAndObject(input);
        bufferSize = input.readInt();
        //hostIncludePort = input.readBoolean();
        //shuffleSize = input.readInt();
    }
}
