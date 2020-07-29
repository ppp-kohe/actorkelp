package csl.actor.kelp;

import csl.actor.*;
import csl.actor.kelp.behavior.MailboxKelp;
import csl.actor.util.ResponsiveCalls;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ActorKelpMergerSharing<ActorType extends ActorKelp<ActorType>> implements AutoCloseable {
    protected ActorSystem system;
    protected ExecutorService executor;
    protected ConfigKelp config;
    protected UUID id;
    protected boolean share;

    public ActorKelpMergerSharing(ActorSystem system, ConfigKelp config) {
        this(system, config, UUID.randomUUID());
    }

    public ActorKelpMergerSharing(ActorSystem system, ConfigKelp config, UUID id) {
        this.system = system;
        this.config = config;
        executor = Executors.newCachedThreadPool();
        this.id = id;
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    public ActorKelpSerializable<ActorType> shareSync(List<? extends ActorRef> members) {
        try {
            share = true;
            ActorKelpSerializable<ActorType> state = mergeSync(members);
            members.forEach(m ->
                    m.tell(new SetStateTask(id, state)));
            return state;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ActorType mergeToLocalSync(List<? extends ActorRef> members) {
        try {
            share = false;
            ActorKelpSerializable<ActorType> k = mergeSync(members);
            return (k == null ? null : k.restore(system, -1, config));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ActorKelpSerializable<ActorType> mergeSync(List<? extends ActorRef> members) {
        try {
            return mergeAsync(members).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public Future<ActorKelpSerializable<ActorType>> mergeAsync(List<? extends ActorRef> members) {
        if (members.isEmpty()) {
            return null;
        } else if (members.size() == 1) {
            return CompletableFuture.completedFuture(mergeSingle(members.get(0)));
        } else if (members.size() == 2) {
            return mergeAsync(
                    CompletableFuture.completedFuture(mergeSingle(members.get(0))),
                    CompletableFuture.completedFuture(mergeSingle(members.get(1))));
        } else {
            int hs = members.size() / 2;
            return mergeAsync(
                    mergeAsync(members.subList(0, hs)),
                    mergeAsync(members.subList(hs, members.size())));
        }
    }

    @SuppressWarnings("unchecked")
    public ActorKelpSerializable<ActorType> mergeSingle(ActorRef ref) {
        try {
            return (ActorKelpSerializable<ActorType>) ResponsiveCalls.sendTask(system, ref,
                    new ToStateTask(id, share)).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public Future<ActorKelpSerializable<ActorType>> mergeAsync(
            Future<ActorKelpSerializable<ActorType>> l,
            Future<ActorKelpSerializable<ActorType>> r) {
        return executor.submit(() -> merge(l.get(), r.get()));
    }

    public ActorKelpSerializable<ActorType> merge(ActorKelpSerializable<ActorType> l, ActorKelpSerializable<ActorType> r) {
        try {
            ActorType tmp = temporaryActor(l);
            tmp.merge(r);
            return tmp.toSerializable(false);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class ToStateTask implements Serializable, CallableMessage<Actor, ActorKelpSerializable<?>>, MailboxKelp.MessageControl {
        public static final long serialVersionUID = -1;
        protected UUID id;
        protected boolean suspend;

        public ToStateTask() {}

        public ToStateTask(UUID id, boolean suspend) {
            this.id = id;
            this.suspend = suspend;
        }

        @Override
        public void control(Message<?> message, MailboxKelp mbox) {
            if (suspend) {
                mbox.getOrCreateControlEntry(id, MailboxKelp.ControlEntry::new);
            }
        }

        @Override
        public ActorKelpSerializable<?> call(Actor another) {
            ActorKelp<?> kelp = (ActorKelp<?>) another;

            ActorKelpSerializable<?> s = kelp.toSerializable(false);
            s.internalStateUsed = true;
            return s;
        }
    }

    public static class SetStateTask implements CallableMessage.CallableMessageConsumer<ActorKelp<?>>, MailboxKelp.MessageControl {
        public static final long serialVersionUID = -1;
        protected UUID id;
        protected ActorKelpSerializable<?> state;

        public SetStateTask() {}

        public SetStateTask(UUID id, ActorKelpSerializable<?> state) {
            this.id = id;
            this.state = state;
        }

        @Override
        public void control(Message<?> message, MailboxKelp mbox) {
            mbox.removeControlEntry(id);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public void accept(ActorKelp<?> self) {
            if (state != null) {
                self.setSerializable((ActorKelpSerializable) state);
            }
        }
    }


    public ActorType temporaryActor(ActorKelpSerializable<ActorType> state) throws Exception {
        ActorType tmp = state.create(system, null, config);
        tmp.setNameRandom();
        return tmp;
    }

    public static class StateSharingActor extends ActorDefault {
        protected UUID id;
        protected List<ActorRef> members = new ArrayList<>();
        protected ConfigKelp config;
        protected Instant endTime = Instant.now();

        public StateSharingActor(ActorSystem system, ConfigKelp config) {
            super(system);
            this.config = config;
            id = UUID.randomUUID();
            setNameRandom();
        }

        public List<ActorRef> getMembers() {
            return members;
        }

        public void setNameRandom() {
            name = getClass().getSimpleName() + "_" + id;
            system.register(this);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(StateSharingRequest.class, this::receive)
                    .build();
        }

        public void receive(StateSharingRequest req) {
            if (endTime.isBefore(req.getTime())) {
                return;
            }
            
            config.log("share state");
            try (ActorKelpMergerSharing<?> m = new ActorKelpMergerSharing<>(system, config)) {
                m.shareSync(getMembers());
                endTime = Instant.now();
            }
        }
    }

    public static class StateSharingRequest implements Serializable {
        public static final long serialVersionUID = -1;
        protected ActorRef sender;
        protected Instant time;

        public StateSharingRequest() {}

        public StateSharingRequest(ActorRef sender) {
            this.sender = sender;
            time = Instant.now();
        }

        public Instant getTime() {
            return time;
        }

        public ActorRef getSender() {
            return sender;
        }
    }
}
