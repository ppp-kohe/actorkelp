package csl.actor.kelp.shuffle;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.KelpStage;
import csl.actor.kelp.MessageBundle;
import csl.actor.kelp.behavior.KelpDispatcher;
import csl.actor.util.ResponsiveCalls;
import csl.actor.util.StagingActor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ActorRefShuffle implements ActorRef, Serializable, Cloneable, KryoSerializable,
        StagingActor.StagingNonSubject, KelpDispatcher.DispatchRef {
    public static final long serialVersionUID = 1L;
    protected transient ActorSystem system;
    protected int bufferSize;
    protected List<ShuffleEntry> dispatchUnits;
    protected List<KelpDispatcher.SelectiveDispatcher> selectiveDispatchers;

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

    public static void flush(ActorRef ref, ActorRef sender) {
        if (ref instanceof ActorRefShuffle) {
            ((ActorRefShuffle) ref).flush(sender);
        }
    }

    ////////////

    public ActorRefShuffle() {
    }

    public ActorRefShuffle(ActorSystem system, List<ShuffleEntry> entries, List<KelpDispatcher.SelectiveDispatcher> selectiveDispatchers, int bufferSize) {
        this.system = system;
        this.bufferSize = bufferSize;
        initExtractorsAndDispatchers(selectiveDispatchers);
        initDispatchUnits(entries);
    }

    protected void initDispatchUnits(List<ShuffleEntry> entries) {
        this.dispatchUnits = entries;
    }

    protected void initExtractorsAndDispatchers(List<KelpDispatcher.SelectiveDispatcher> extractorsAndDispatchers) {
        this.selectiveDispatchers = extractorsAndDispatchers;
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
                selectiveDispatchers.stream()
                    .map(KelpDispatcher.SelectiveDispatcher::copy)
                    .collect(Collectors.toList()));
    }

    protected ActorRefShuffle copyForUse(
            List<ShuffleEntry> dispatchUnits,
            List<KelpDispatcher.SelectiveDispatcher> extractorsAndDispatchers) {
        try {
            ActorRefShuffle ref = (ActorRefShuffle) super.clone();
            //ref.initEntries(es);
            ref.initDispatchUnits(dispatchUnits);
            ref.initExtractorsAndDispatchers(this.selectiveDispatchers);
            return ref;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void tellMessage(Message<?> message) {
        if (message instanceof MessageBundle) {
            ((MessageBundle<?>) message).getData().forEach(d ->
                    tell(d, message.getSender()));
        } else if (KelpStage.isMessageBroadcastedImpl(message)) {
            getMemberActors().forEach(a ->
                    a.tellMessage(message));
        } else {
            tellMessageShuffle(message);
        }
    }

    public void tellMessageShuffle(Message<?> message) {
        KelpDispatcher.tellMessageShuffle(this, selectiveDispatchers, message);
    }

    public static KelpDispatcher.DispatcherShuffle DEFAULT_DISPATCHER = new KelpDispatcher.DispatcherShuffle();

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
        public int bufferSizeMax;

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
        public ActorRef next;

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
                actor.tellMessage(new MessageAccepted<>(actor, sender, data));
            } else {
                if (buffer == null) {
                    buffer = new ArrayList<>(bufferSize);
                }
                buffer.add(data);
                if (buffer.size() >= bufferSize) { //suppose the ref is not shared
                    actor.tellMessage(new MessageBundle<>(actor, sender, buffer));
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
                actor.tellMessage(new MessageBundle<>(actor, sender, buffer));
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
        kryo.writeClassAndObject(output, dispatchUnits);
        kryo.writeClassAndObject(output, selectiveDispatchers);
        output.writeInt(bufferSize);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void read(Kryo kryo, Input input) {
        this.dispatchUnits = (List<ShuffleEntry>) kryo.readClassAndObject(input);
        this.selectiveDispatchers = (List<KelpDispatcher.SelectiveDispatcher>) kryo.readClassAndObject(input);
        bufferSize = input.readInt();
    }
}
