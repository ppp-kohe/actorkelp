package csl.actor.kelp.shuffle;

import csl.actor.*;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.behavior.MailboxKelp;
import csl.actor.util.ResponsiveCalls;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

public abstract class ActorKelpStateSharing<ActorType extends Actor, StateType> implements AutoCloseable {
    protected ActorSystem system;
    protected ExecutorService executor;
    protected boolean executorOwner;
    protected ConfigKelp config;
    protected UUID id;
    protected boolean share;

    public ActorKelpStateSharing(ActorSystem system, ConfigKelp config) {
        this(system, config, UUID.randomUUID());
    }

    public ActorKelpStateSharing(ActorSystem system, ConfigKelp config, UUID id) {
        this.system = system;
        this.config = config;
        executor = ActorSystemKelp.getMergerExecutors(system);
        if (executor == null) {
            executor = Executors.newCachedThreadPool();
            executorOwner = true;
        }
        this.id = id;
    }

    @Override
    public void close() {
        if (executorOwner) {
            executor.shutdown();
        }
    }

    public StateType shareSync(List<? extends ActorRef> members) {
        try {
            share = true;
            StateType state = mergeSync(members);
            members.forEach(m ->
                    m.tell(new SetStateTask<>(id, state, getSetState()).withSender(null)));
            return state;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    public StateType mergeSync(List<? extends ActorRef> members) {
        try {
            return mergeAsync(members).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public Future<StateType> mergeAsync(List<? extends ActorRef> members) {
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

    public StateType mergeSingle(ActorRef ref) {
        try {
            return ResponsiveCalls.sendTask(system, ref,
                    new ToStateTask<>(id, share, getToState())).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public abstract ToStateFunction<ActorType, StateType> getToState();

    public interface ToStateFunction<ActorType, StateType> extends Serializable {
        long serialVersionUID = -1L;

        StateType apply(ActorType self);
    }

    public abstract SetStateFunction<ActorType, StateType> getSetState();

    public interface SetStateFunction<ActorType, StateType> extends Serializable {
        long serialVersionUID = -1L;

        void accept(ActorType self, StateType state);
    }


    public Future<StateType> mergeAsync(
            Future<StateType> l,
            Future<StateType> r) {
        return executor.submit(() -> merge(l.get(), r.get()));
    }

    public abstract StateType merge(StateType l, StateType r);

    public static class ToStateTask<ActorType extends Actor, StateType>
            implements Serializable, CallableMessage<ActorType, StateType>, MailboxKelp.MessageDataControl {
        public static final long serialVersionUID = -1;
        public UUID id;
        public boolean suspend;
        public ToStateFunction<ActorType, StateType> function;

        public ToStateTask() {}

        public ToStateTask(UUID id, boolean suspend, ToStateFunction<ActorType, StateType> function) {
            this.id = id;
            this.suspend = suspend;
            this.function = function;
        }

        @Override
        public void control(Message<?> message, MailboxKelp mbox) {
            if (suspend) {
                mbox.getOrCreateControlEntry(id, MailboxKelp.ControlEntry::new);
            }
        }

        @Override
        public StateType call(ActorType another) {
            return function.apply(another);
        }
    }

    public static class SetStateTask<ActorType extends Actor, StateType>
            implements CallableMessage.CallableMessageConsumer<ActorType>, MailboxKelp.MessageDataControl {
        public static final long serialVersionUID = -1;
        public UUID id;
        public StateType state;
        public SetStateFunction<ActorType, StateType> function;

        public SetStateTask() {}

        public SetStateTask(UUID id, StateType state, SetStateFunction<ActorType, StateType> function) {
            this.id = id;
            this.state = state;
            this.function = function;
        }

        @Override
        public void control(Message<?> message, MailboxKelp mbox) {
            mbox.removeControlEntry(id);
        }

        @Override
        public void accept(ActorType self) {
            if (state != null) {
                function.accept(self, state);
            }
        }
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
            name = getClass().getSimpleName() + NAME_ID_SEPARATOR + id;
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
            try (ActorKelpStateSharing<?,?> m = req.create(system, config)) {
                m.shareSync(getMembers());
                endTime = Instant.now();
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static class StateSharingRequest implements Serializable, Message.MessageData {
        public static final long serialVersionUID = -1;
        public ActorRef sender;
        public Instant time;
        public Class<? extends ActorKelpStateSharing> type;

        public StateSharingRequest() {}

        public StateSharingRequest(ActorRef sender, Class<? extends ActorKelpStateSharing> type) {
            this.sender = sender;
            time = Instant.now();
            this.type = type;
        }

        public Instant getTime() {
            return time;
        }

        public ActorRef getSender() {
            return sender;
        }

        public ActorKelpStateSharing<?,?> create(ActorSystem system, ConfigKelp config) {
            try {
                return type.getConstructor(ActorSystem.class, ConfigKelp.class)
                        .newInstance(system, config);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public interface MergerOperator<StateType> extends Serializable, BinaryOperator<StateType> {
        long serialVersionUID = -1;
    }

    public static <ActorType extends Actor, StateType> ActorKelpStateSharingLambda<ActorType,StateType> get(ActorSystem system, ConfigKelp config,
                                       ToStateFunction<ActorType, StateType> toState,
                                       SetStateFunction<ActorType, StateType> setState,
                                       MergerOperator<StateType> merger) {
        return new ActorKelpStateSharingLambda<>(system, config, toState, setState, merger);
    }

    public static <ActorType extends Actor, StateType> BiFunction<ActorSystem, ConfigKelp, ActorKelpStateSharing<ActorType,StateType>> factory(
                                                                                                            ToStateFunction<ActorType, StateType> toState,
                                                                                                            SetStateFunction<ActorType, StateType> setState,
                                                                                                            MergerOperator<StateType> merger) {
        return (system, config) -> get(system, config, toState, setState, merger);
    }

    public static <ActorType extends Actor, StateType> BiFunction<ActorSystem, ConfigKelp, ActorKelpStateSharing<ActorType,StateType>> factory(
            ToStateFunction<ActorType, StateType> toState,
            MergerOperator<StateType> merger) {
        return (system, config) -> get(system, config, toState,
                new SetStateFunctionThrowException<>(),
                merger);
    }

    public static class SetStateFunctionThrowException<ActorType extends Actor, StateType> implements SetStateFunction<ActorType, StateType> {
        public static final long serialVersionUID = -1;
        @Override
        public void accept(ActorType self, StateType state) {
            throw new RuntimeException("unsupported : " + self + " : " + state);
        }
    }

    public static <ActorType extends Actor, StateType> StateSharingRequestLambda<ActorType,StateType> request(ActorRef sender,
                                                                                                                           ToStateFunction<ActorType, StateType> toState,
                                                                                                                           SetStateFunction<ActorType, StateType> setState,
                                                                                                                           MergerOperator<StateType> merger) {
        return new StateSharingRequestLambda<>(sender, toState, setState, merger);
    }

    public static class ActorKelpStateSharingLambda<ActorType extends Actor, StateType> extends ActorKelpStateSharing<ActorType,StateType> {
        protected ToStateFunction<ActorType, StateType> toState;
        protected SetStateFunction<ActorType, StateType> setState;
        protected MergerOperator<StateType> merger;

        public ActorKelpStateSharingLambda(ActorSystem system, ConfigKelp config,
                                           ToStateFunction<ActorType, StateType> toState,
                                           SetStateFunction<ActorType, StateType> setState,
                                           MergerOperator<StateType> merger) {
            super(system, config);
            this.toState = toState;
            this.setState = setState;
            this.merger = merger;
        }

        public ActorKelpStateSharingLambda(ActorSystem system, ConfigKelp config, UUID id,
                                           ToStateFunction<ActorType, StateType> toState,
                                           SetStateFunction<ActorType, StateType> setState,
                                           MergerOperator<StateType> merger) {
            super(system, config, id);
            this.toState = toState;
            this.setState = setState;
            this.merger = merger;
        }

        @Override
        public ToStateFunction<ActorType, StateType> getToState() {
            return toState;
        }

        @Override
        public SetStateFunction<ActorType, StateType> getSetState() {
            return setState;
        }

        @Override
        public StateType merge(StateType l, StateType r) {
            return merger.apply(l, r);
        }
    }

    public static class StateSharingRequestLambda<ActorType extends Actor, StateType> extends StateSharingRequest {
        public static final long serialVersionUID = -1;
        public ToStateFunction<ActorType, StateType> toState;
        public SetStateFunction<ActorType, StateType> setState;
        public MergerOperator<StateType> merger;

        public StateSharingRequestLambda() { }

        public StateSharingRequestLambda(ActorRef sender,
                                         ToStateFunction<ActorType, StateType> toState,
                                         SetStateFunction<ActorType, StateType> setState,
                                         MergerOperator<StateType> merger) {
            super(sender, ActorKelpStateSharingLambda.class);
            this.toState = toState;
            this.setState = setState;
            this.merger = merger;
        }

        @Override
        public ActorKelpStateSharing<?, ?> create(ActorSystem system, ConfigKelp config) {
            return new ActorKelpStateSharingLambda<>(system, config, toState, setState, merger);
        }
    }
}
