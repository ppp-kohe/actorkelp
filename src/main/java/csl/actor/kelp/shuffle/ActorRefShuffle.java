package csl.actor.kelp.shuffle;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.KelpStage;
import csl.actor.MessageBundle;
import csl.actor.kelp.behavior.KelpDispatcher;
import csl.actor.util.ResponsiveCalls;
import csl.actor.util.Staging;

import java.io.Flushable;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ActorRefShuffle implements ActorRef, Serializable, Cloneable, KryoSerializable,
        KelpDispatcher.DispatchRef {
    public static final long serialVersionUID = 1L;
    protected transient ActorSystem system;
    protected int bufferSize;
    protected List<ShuffleEntry> dispatchUnits;
    protected List<KelpDispatcher.SelectiveDispatcher> selectiveDispatchers;
    protected String name;

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

    public static void flush(ActorRef ref) {
        if (ref instanceof Flushable) {
            try {
                ((Flushable) ref).flush();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } else if (ref instanceof KelpDispatcher.DispatchUnit) {
            ((KelpDispatcher.DispatchUnit) ref).flush();
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
        this.name = Staging.stageNameArray(getClass().getSimpleName());
    }

    protected void initDispatchUnits(List<ShuffleEntry> entries) {
        this.dispatchUnits = entries;
    }

    protected void initExtractorsAndDispatchers(List<KelpDispatcher.SelectiveDispatcher> extractorsAndDispatchers) {
        this.selectiveDispatchers = extractorsAndDispatchers;
    }

    public String getName() {
        return name;
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
            for (Object m : ((MessageBundle<?>) message).getData()) {
                tell(m);
            }
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
        dispatchUnits
                .forEach(KelpDispatcher.DispatchUnit::flush);
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
            if (self instanceof Staging.StagingSupported) {
                ActorRef nr = next;
                if (next instanceof ActorRefShuffle) {
                    nr = ((ActorRefShuffle) next).use();
                }
                ((Staging.StagingSupported) self).setNextStage(nr);
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

    public int getBufferingCount() {
        return this.dispatchUnits.stream()
                .mapToInt(ShuffleEntry::getBufferingCount)
                .sum();
    }

    public static class ShuffleEntry implements Serializable, KelpDispatcher.DispatchUnit, KryoSerializable {
        public static final long serialVersionUID = 1L;
        public int bufferSize;
        public int index;
        public ActorRef actor;
        public transient Queue<Object> buffer;

        public ShuffleEntry() {}

        public ShuffleEntry(ActorRef actor, int bufferSize, int index) {
            this.actor = actor;
            this.bufferSize = bufferSize;
            buffer = initBuffer();
            this.index = index;
        }

        public ActorRef getActor() {
            return actor;
        }

        public Queue<Object> getBuffer() {
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
        public List<? extends ActorRef> getStagingSubjectActors() {
            return Collections.singletonList(actor);
        }

        @Override
        public boolean hasRemainingMessage() {
            Queue<Object> b = buffer;
            return b != null && !b.isEmpty();
        }
        public boolean isSpecialMessage(Object data) {
            return isSpecialMessageImpl(data);
        }

        public static boolean isSpecialMessageImpl(Object data) { //currently, specific impl.
            return (data instanceof Message.MessageDataSpecial) ||
                    (data instanceof Message.MessageDataDelayed) ||
                    (data instanceof Message.MessageDataHolder<?> &&
                            isSpecialMessageImpl(((Message.MessageDataHolder<?>) data).getData()));
        }

        @Override
        public void tell(Object data) {
            if (isSpecialMessage(data)) {
                flush();
                actor.tellMessage(new MessageBundle.MessageAccepted<>(actor, data));
            } else {
                if (bufferSize <= 0) {
                    actor.tellMessage(new MessageBundle.MessageAccepted<>(actor, data));
                } else {
                    if (buffer == null) {
                        buffer = initBuffer();
                    }
                    buffer.add(data);
                    if (buffer.size() >= bufferSize) { //suppose the ref is not shared
                        actor.tellMessage(new MessageBundle<>(actor, Arrays.asList(buffer.toArray())));
                        buffer.clear();
                    }
                }
            }
        }

        @Override
        public void tellMessage(Message<?> message) {
            tell(message.getData());
        }

        @Override
        public void flush() {
            if (buffer != null && !buffer.isEmpty()) {
                actor.tellMessage(new MessageBundle<>(actor, buffer));
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
            buffer = initBuffer();
        }

        protected Queue<Object> initBuffer() {
            return bufferSize <= 0 ? null : new ArrayBlockingQueue<>(bufferSize);
        }

        public int getBufferingCount() {
            return buffer == null ? 0 : buffer.size();
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
