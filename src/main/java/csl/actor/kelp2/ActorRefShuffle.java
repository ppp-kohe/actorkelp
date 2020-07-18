package csl.actor.kelp2;


import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.CallableMessage;
import csl.actor.Message;
import csl.actor.util.ResponsiveCalls;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ActorRefShuffle implements ActorRef, Serializable {
    public static final long serialVersionUID = 1L;

    protected List<ActorRef> actors;
    protected transient List<List<Object>> messageBuffers;
    protected int bufferSize;

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
        ArrayList<ActorRef> refs = new ArrayList<>();
        for (ActorRef a: actors) {
            refs.add(a);
        }
        refs.trimToSize();
        return new ActorRefShuffle(refs, bufferSize);
    }

    public static void flush(ActorRef ref, ActorRef sender) {
        if (ref instanceof ActorRefShuffle) {
            ((ActorRefShuffle) ref).flush(sender);
        }
    }

    public ActorRefShuffle() {
    }

    public ActorRefShuffle(List<ActorRef> actors) {
        this(actors, BUFFER_SIZE_DEFAULT);
    }

    public ActorRefShuffle(List<ActorRef> actors, int bufferSize) {
        this.actors = actors;
        this.bufferSize = bufferSize;
        initBuffers();
    }

    public void initBuffers() {
        messageBuffers = IntStream.range(0, actors.size())
                .mapToObj(i -> bufferSize <= 0 ? Collections.emptyList() : new ArrayList<>(bufferSize))
                .collect(Collectors.toList());
    }

    public List<ActorRef> getActors() {
        return actors;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public ActorRefShuffle use() {
        return new ActorRefShuffle(actors, bufferSize);
    }

    @Override
    public void tellMessage(Message<?> message) {
        if (message instanceof ActorKelp.MessageBundle) {
            ((ActorKelp.MessageBundle<?>) message).getData().forEach(d ->
                    tell(d, message.getSender()));
        } else {
            int index = index(message.getData());
            if (bufferSize <= 0) {
                ActorRef actor = actors.get(index);
                actor.tell(message.getData(), message.getSender());
            } else {
                if (messageBuffers == null) {
                    initBuffers();
                }
                List<Object> buffer = messageBuffers.get(index);
                buffer.add(message.getData());

                if (buffer.size() >= bufferSize) { //suppose the ref is not shared
                    sendBuffer(index, message.getSender());
                }
            }
        }
    }

    protected int index(Object data) {
        return hashMod(Objects.hashCode(data), actors.size());
    }

    /**
     * @param hash hashCode
     * @param size a max+1 value, >= 0 , usually relatively smaller than hash
     * @return the index within size
     */
    public static int hashMod(int hash, int size) {
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

    public void flush(ActorRef sender) {
        if (messageBuffers == null) {
            return;
        }
        for (int i = 0, l = actors.size(); i < l; ++i) {
            sendBuffer(i, sender);
        }
    }

    protected void sendBuffer(int index, ActorRef sender) {
        if (messageBuffers == null) {
            initBuffers();
        }
        List<Object> buffer = messageBuffers.get(index);
        if (!buffer.isEmpty()) {
            ActorRef actor = actors.get(index);
            actor.tellMessage(new ActorKelp.MessageBundle<>(actor, sender, buffer));
            buffer.clear();
        }
    }

    public CompletableFuture<Void> connect(ActorSystem system, ActorRef next) {
        List<CompletableFuture<?>> tasks = new ArrayList<>();
        for (ActorRef a : actors) {
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
        return "refShuffle[" + actors.size() + "](" +
                "actors=" + (actors.isEmpty()? "[]" : "[" + actors.get(0) + ",...]") +
                ", bufferSize=" + bufferSize +
                ')';
    }
}
