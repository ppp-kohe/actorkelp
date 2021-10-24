package csl.actor.kelp;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.kelp.behavior.KelpDispatcher;
import csl.actor.kelp.shuffle.ActorKelpStateSharing;
import csl.actor.kelp.shuffle.ActorRefShuffle;
import csl.actor.kelp.shuffle.ActorRefShuffleKelp;
import csl.actor.util.StagingActor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public interface KelpStage<ActorType extends Actor> extends ActorRef, KelpDispatcher.DispatchRef {

    /**
     * set the nextStage property of each shuffle-members to the next actor.
     *  <ol>
     *      <li>If the next is an original actor ({@link ActorKelp#isUnit()}==false),
     *          it uses {@link ActorKelp#shuffle(int)} as the actual next</li>
     *      <li>If the next is a remote (or local address) reference,
     *          it sends synchronous task for doing the first step. </li>
     *      <li>If the next is an {@link ActorRefShuffle},
     *          it will copy the next for each shuffle-members, by using {@link ActorRefShuffle#use()}</li>
     *  </ol>
     * @param actor the next stage actor
     * @param <NextActorType> the actual actor type of the next stage actor
     * @return the next stage
     */
    @SuppressWarnings("unchecked")
    default <NextActorType extends Actor> KelpStage<NextActorType> connects(NextActorType actor) {
        return connects((Class<NextActorType>) actor.getClass(), actor);
    }

    <NextActorType extends Actor> KelpStage<NextActorType> connects(Class<NextActorType> actorType, ActorRef ref);

    List<ActorRef> getMemberActors();

    /**
     * If the target is an {@link ActorRefShuffleKelp},
     * it collects state of member {@link ActorKelp}s
     *  and returns as a local actor by merging.
     *    It is a synchronous task.
     * Each member actor becomes disabled.
     * The actor's {@link AutoCloseable#close()} will be executed.
     * @return the merged actor
     */
    ActorType merge();

    /**
     * similar to {@link #merge()}, but just collecting and merging states.
     * @return a temporary created merged actor (or this)
     */
    ActorType getMergedState();

    /**
     * collecting member states obtained by the function
     * @param toState the function (actor) -&gt; state
     * @param <StateType> the state type
     * @return list of collected states
     */
    default <StateType> List<StateType> collectStates(ActorKelpStateSharing.ToStateFunction<ActorType, StateType> toState) {
        return merge(ActorKelpStateSharing.factory(new CollectStates<>(toState), new MergerOperatorConcat<>()));
    }

    class CollectStates<ActorType extends Actor, StateType> implements ActorKelpStateSharing.ToStateFunction<ActorType, List<StateType>> {
        public ActorKelpStateSharing.ToStateFunction<ActorType, StateType> toState;

        public CollectStates() {}

        public CollectStates(ActorKelpStateSharing.ToStateFunction<ActorType, StateType> toState) {
            this.toState = toState;
        }

        @Override
        public List<StateType> apply(ActorType self) {
            List<StateType> sl = new ArrayList<>();
            sl.add(toState.apply(self));
            return sl;
        }
    }

    class MergerOperatorConcat<StateType> implements ActorKelpStateSharing.MergerOperator<List<StateType>> {
        @Override
        public List<StateType> apply(List<StateType> stateTypes, List<StateType> stateTypes2) {
            stateTypes.addAll(stateTypes2);
            return stateTypes;
        }
    }

    /**
     * collecting and merging states obtained by the functions
     * @param toState the state getter function (actor) -&gt; state
     * @param merger  (state1,state2)-&gt;mergedState
     * @param <StateType> the state type
     * @return the merged state for all members
     */
    default <StateType> StateType getMergedState(ActorKelpStateSharing.ToStateFunction<ActorType, StateType> toState,
                                                 ActorKelpStateSharing.MergerOperator<StateType> merger) {
        return merge(ActorKelpStateSharing.factory(toState, merger));
    }

    <StateType> StateType merge(BiFunction<ActorSystem,ConfigKelp, ? extends ActorKelpStateSharing<ActorType,StateType>> factory);

    default void setSystemBySerializer(ActorSystem system) { }

    /**
     * flush buffered messages
     */
    default void flush() {}

    /**
     * flush buffered messages
     * @param sender the sender
     */
    default void flush(ActorRef sender) {}

    ActorSystem getSystem();

    /**
     * @return the member size
     */
    default int getDispatchUnitSize() {
        return 1;
    }

    ////forEach

    void forEach(Consumer<KelpDispatcher.DispatchUnit> task);

    default void forEachTell(Object msg) {
        forEach(s -> s.tell(msg));
    }

    /**
     * {@link #flush()} and returns a {@link CompletableFuture} from {@link StagingActor}
     * @return a completable future
     */
    default CompletableFuture<StagingActor.StagingCompleted> sync() {
        return sync(Instant.now());
    }

    /**
     * {@link #flush()} and returns a {@link CompletableFuture} from {@link StagingActor}
     * @param startTime the staring time passed to the {@link StagingActor}
     * @return a completable future
     */
    default CompletableFuture<StagingActor.StagingCompleted> sync(Instant startTime) {
        return sync(s -> s.withStartTime(startTime));
    }

    default CompletableFuture<StagingActor.StagingCompleted> sync(Consumer<StagingActor> editor) {
        flush();
        StagingActor s = StagingActor.staging(getSystem());
        editor.accept(s);
        return s.startActors(getMemberActors());
    }

    default CompletableFuture<StagingActor.StagingCompleted> forEachTellSync(Instant startTime, Consumer<KelpDispatcher.DispatchUnit> task) {
        forEach(task);
        return sync(startTime);
    }

    default CompletableFuture<StagingActor.StagingCompleted> forEachTellSync(Consumer<KelpDispatcher.DispatchUnit> task) {
        forEach(task);
        return sync();
    }

    default CompletableFuture<StagingActor.StagingCompleted> forEachTellSync(Instant startTime, Object msg) {
        forEachTell(msg);
        return sync(startTime);
    }

    default CompletableFuture<StagingActor.StagingCompleted> forEachTellSync(Object msg) {
        forEachTell(msg);
        return sync();
    }


    default boolean isMessageBroadcasted(Message<?> message) {
        return isMessageBroadcastedImpl(message);
    }

    static boolean isMessageBroadcastedImpl(Message<?> message) {
        if (message instanceof Message.MessageNone) {
            return true;
        } else {
            Object data = message.getData();
            return data instanceof StagingActor.StagingWatcher ||
                    data instanceof StagingActor.StagingCompleted ||
                    data instanceof StagingActor.StagingHandlerCompleted ||
                    data instanceof StagingActor.StagingNotification;
        }
    }
}
