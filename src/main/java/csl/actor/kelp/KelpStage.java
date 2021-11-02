package csl.actor.kelp;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.kelp.behavior.KelpDispatcher;
import csl.actor.kelp.shuffle.ActorKelpStateSharing;
import csl.actor.kelp.shuffle.ActorRefShuffle;
import csl.actor.kelp.shuffle.ActorRefShuffleKelp;
import csl.actor.util.Staging;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public interface KelpStage<ActorType extends Actor> extends ActorRef, KelpDispatcher.DispatchRef, Staging.StagingPoint {

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

    Class<ActorType> getActorType();

    /**
     * set the nextStage property of each shuffle-members to the next junction.
     * The method is intended to combine with {@link ActorKelp#shuffle()}:
     *  <pre>
     *     KelpStage prevStage1 = ...
     *     KelpStage prevStage2 = ...
     *
     *     MyActorKelp next = new MyActorKelp(...);
     *     prevStage1.connects(next); // first next.shuffle()
     *     prevStage2.connects(next); //ERROR! second next.shuffle(): unintended copying of next will happen.
     *
     *     //instead...
     *     KelpStage&lt;MyActorKelp&gt; next = new MyActorKelp(...).shuffle();
     *     prevStage1.connectsStage(next); //copying stage: without copying the unit actors
     *     prevStage2.connectsStage(next); //OK
     *  </pre>
     *
     * @param stage the junction
     * @param <NextActorType> the actual actor type
     * @return the next stage
     */
    default <NextActorType extends Actor> KelpStage<NextActorType> connectsStage(KelpStage<NextActorType> stage) {
        return connects(stage.getActorType(), stage);
    }

    List<ActorRef> getMemberActors();

    /**
     * @return a unique name between stages, might be the actor name
     */
    String getName();

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
        public static final long serialVersionUID = -1;
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
        public static final long serialVersionUID = -1;
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

    default void tellAndFlush(Object data) {
        tell(data);
        flush();
    }

    /**
     * flush buffered messages
     */
    default void flush() {}

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
     * do {@link KelpStageGraphActor#get(ActorSystem, ActorRef...)} with <code>(getSystem(), this)</code>.
     * constructs a stage graph and returns it. The target stage will be the entry point of the graph.
     * @return a graph actor constructed from this
     */
    default KelpStageGraphActor stageGraph() {
        try {
            return KelpStageGraphActor.get(getSystem(), this);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * {@link #flush()} and returns a {@link CompletableFuture} from {@link KelpStageGraphActor}
     * @return a completable future
     */
    default CompletableFuture<KelpStageGraphActor> sync() {
        return sync(Instant.now());
    }

    /**
     * {@link #flush()} and returns a {@link CompletableFuture} from {@link KelpStageGraphActor}
     * @param startTime the staring time passed to the {@link KelpStageGraphActor}
     * @return a completable future
     */
    default CompletableFuture<KelpStageGraphActor> sync(Instant startTime) {
        return sync(startTime, e -> {});
    }

    default CompletableFuture<KelpStageGraphActor> sync(Instant startTime, Consumer<KelpStageGraphActor> editor) {
        flush();
        try {
            KelpStageGraphActor s = stageGraph();
            editor.accept(s);
            return s.startAwait(startTime);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    default CompletableFuture<KelpStageGraphActor> forEachTellSync(Instant startTime, Consumer<KelpDispatcher.DispatchUnit> task) {
        forEach(task);
        return sync(startTime);
    }

    default CompletableFuture<KelpStageGraphActor> forEachTellSync(Consumer<KelpDispatcher.DispatchUnit> task) {
        forEach(task);
        return sync();
    }

    default CompletableFuture<KelpStageGraphActor> forEachTellSync(Instant startTime, Object msg) {
        forEachTell(msg);
        return sync(startTime);
    }

    default CompletableFuture<KelpStageGraphActor> forEachTellSync(Object msg) {
        forEachTell(msg);
        return sync();
    }


    default boolean isMessageBroadcasted(Message<?> message) {
        return isMessageBroadcastedImpl(message);
    }

    static boolean isMessageBroadcastedImpl(Message<?> message) {
        return (message instanceof Message.MessageNone);
    }
}
