package csl.actor.kelp2;


import csl.actor.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.util.ResponsiveCalls;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ActorRefShuffle implements ActorRef, Serializable {
    public static final long serialVersionUID = 1L;
    protected int bufferSize;
    protected Map<ActorAddress, List<ShuffleEntry>> entries; //host-address to entries
    protected List<List<ShuffleEntry>> entriesList;

    public static final int BUFFER_SIZE_DEFAULT = 512;

    public static ActorRefShuffle createRef(int actors, IntFunction<? extends ActorRef> actorGen) {
        return createRef(actors, actorGen, ActorRefShuffle.BUFFER_SIZE_DEFAULT);
    }

    public static ActorRefShuffle createRef(int actors, IntFunction<? extends ActorRef> actorGen, int bufferSize) {
        return createRef(IntStream.range(0, actors).mapToObj(actorGen)
                .collect(Collectors.toList()), bufferSize);
    }

    public static ActorRefShuffle createRef(Iterable<? extends ActorRef> actors) {
        return createRef(actors, ActorRefShuffle.BUFFER_SIZE_DEFAULT);
    }

    public static ActorRefShuffle createRef(Iterable<? extends ActorRef> actors, int bufferSize) {
        return new ActorRefShuffle(createEntries(actors, bufferSize, ActorRefShuffle::toHost), bufferSize);
    }

    public static Map<ActorAddress, List<ShuffleEntry>> createEntries(Iterable<? extends ActorRef> actors, int bufferSize,
                                                                      Function<ActorRef, ActorAddress> refToHost) {
        Map<ActorAddress, List<ShuffleEntry>> refs = new HashMap<>();
        for (ActorRef ref : actors) {
            refs.computeIfAbsent(toHost(ref), _h -> new ArrayList<>())
                    .add(new ShuffleEntry(ref, bufferSize));
        }
        refs.values().forEach(es ->
                ((ArrayList<?>) es).trimToSize());
        return refs;
    }

    static ActorAddress LOCALHOST = ActorAddress.get("localhost", -1);

    public static ActorAddress toHost(ActorRef ref) {
        if (ref instanceof ActorRefRemote) {
            return ((ActorRefRemote) ref).getAddress().getHostAddress();
        } else {
            return LOCALHOST;
        }
    }

    public static void flush(ActorRef ref, ActorRef sender) {
        if (ref instanceof ActorRefShuffle) {
            ((ActorRefShuffle) ref).flush(sender);
        }
    }

    public ActorRefShuffle() {
    }

    public ActorRefShuffle(Map<ActorAddress, List<ShuffleEntry>> entries, int bufferSize) {
        this.entries = entries;
        this.bufferSize = bufferSize;
        entriesList = new ArrayList<>(entries.values());
    }

    public List<ActorRef> getActors() {
        return entriesList.stream()
                .flatMap(List::stream)
                .map(ShuffleEntry::getActor)
                .collect(Collectors.toList());
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public ActorRefShuffle use() {
        Map<ActorAddress, List<ShuffleEntry>> es = new HashMap<>(entries.size());
        entries.forEach((k,v) ->
                es.put(k, v.stream()
                    .map(e -> e.copy(bufferSize))
                    .collect(Collectors.toList())));
        return new ActorRefShuffle(es, bufferSize);
    }

    @Override
    public void tellMessage(Message<?> message) {
        if (message instanceof ActorKelp.MessageBundle) {
            ((ActorKelp.MessageBundle<?>) message).getData().forEach(d ->
                    tell(d, message.getSender()));
        } else {
            int hash = Objects.hashCode(message.getData());
            List<ShuffleEntry> es = entries(message, hash);
            int index = hashMod(hash, es.size());
            ShuffleEntry entry = es.get(index);
            entry.tell(bufferSize, message);
        }
    }

    protected List<ShuffleEntry> entries(Message<?> message, int dataHash) {
        ActorAddress target = addressFromMessageData(message);
        List<ShuffleEntry> es = (target == null ? null : entries.get(target));
        if (es == null) {
            es = entriesList.get(hashMod(dataHash, entriesList.size()));
        }
        return es;
    }

    protected ActorAddress addressFromMessageData(Message<?> message) {
        return null;
    }

    /**
     * @param hash hashCode
     * @param size a max+1 value, >= 0 , usually relatively smaller than hash
     * @return the index within size
     */
    public static int hashMod(int hash, int size) {
        if (size <= 1) {
            return 0;
        } else {
            int h = hash;
            int sh = Integer.highestOneBit(size);
            int sizeWidth = Integer.numberOfTrailingZeros(sh);
            //max of sizeWidth is 30
            int result = 0;
            int remainingBits = 32;
            while (remainingBits > 0) {
                result ^= h;
                h >>>= sizeWidth;
                remainingBits -= sizeWidth;
            }
            return Math.abs(result % size);
        }
    }

    public void flush(ActorRef sender) {
        entries.values()
                .forEach(es -> es.forEach(e ->
                        e.flush(sender)));
    }

    public CompletableFuture<Void> connect(ActorSystem system, ActorRef next) {
        List<CompletableFuture<?>> tasks = new ArrayList<>();
        for (ActorRef a : getActors()) {
            tasks.add(setNextStage(system, a, next));
        }
        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0]));
    }

    public <ActorTypeNext extends ActorRef> ActorTypeNext connectAndGet(ActorSystem system, ActorTypeNext next) {
        try {
            connect(system, next).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return next;
    }
    public static CompletableFuture<?> setNextStage(ActorSystem system, ActorRef a, ActorRef next) {
        return ResponsiveCalls.sendTaskConsumer(system, a, (ref) -> {
            if (ref instanceof ActorKelp) {
                ActorRef nr = next;
                if (next instanceof ActorRefShuffle) {
                    nr = ((ActorRefShuffle) next).use();
                }
                ((ActorKelp<?>) ref).setNextStage(nr);
            }
        });
    }

    @Override
    public String toString() {
        int as = getActorSize();
        return "refShuffle[" + as + "](" +
                "actors=" + (as == 0 ? "[]" : "[" + getFirstActor() + ",...]") +
                ", bufferSize=" + bufferSize +
                ')';
    }

    public int getActorSize() {
        int s = 0;
        for (List<ShuffleEntry> es : entriesList) {
            s += es.size();
        }
        return s;
    }

    public ActorRef getFirstActor() {
        for (List<ShuffleEntry> es : entriesList) {
            if (!es.isEmpty()) {
                return es.get(0).getActor();
            }
        }
        return null;
    }

    public static class ShuffleEntry implements Serializable {
        public static final long serialVersionUID = 1L;
        protected ActorRef actor;
        protected transient ArrayList<Object> buffer;

        public ShuffleEntry() {}

        public ShuffleEntry(ActorRef actor, int bufferSize) {
            this.actor = actor;
            buffer = new ArrayList<>(bufferSize);
        }

        public ActorRef getActor() {
            return actor;
        }

        public List<Object> getBuffer() {
            return buffer;
        }

        public ShuffleEntry copy(int bufferSize) {
            return new ShuffleEntry(actor, bufferSize);
        }

        public void tell(int bufferSize, Message<?> message) {
            if (bufferSize <= 0) {
                actor.tell(message.getData(), message.getSender());
            } else {
                if (buffer == null) {
                    buffer = new ArrayList<>(bufferSize);
                }
                buffer.add(message.getData());
                if (buffer.size() >= bufferSize) { //suppose the ref is not shared
                    actor.tellMessage(new ActorKelp.MessageBundle<>(actor, message.getSender(), buffer));
                    buffer.clear();
                }
            }
        }

        public void flush(ActorRef sender) {
            if (buffer != null && !buffer.isEmpty()) {
                actor.tellMessage(new ActorKelp.MessageBundle<>(actor, sender, buffer));
                buffer.clear();
            }
        }
    }
}
