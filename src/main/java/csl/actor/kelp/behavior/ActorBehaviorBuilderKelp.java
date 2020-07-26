package csl.actor.kelp.behavior;

import csl.actor.*;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ActorKelpFunctions.*;
import csl.actor.kelp.behavior.ActorBehaviorKelp.ActorBehaviorMatchKeyList;
import csl.actor.kelp.behavior.ActorBehaviorKelp.ActorBehaviorMatchKeyListFuture;
import csl.actor.kelp.behavior.ActorBehaviorKelp.ActorBehaviorMatchKeyListFutureStageEnd;
import csl.actor.util.FileSplitter;
import csl.actor.util.StagingActor;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ActorBehaviorBuilderKelp extends ActorBehaviorBuilder {
    protected Consumer<List<HistogramProcessor>> histogramProcessorsTarget;
    protected Map<Integer, HistogramProcessor> processors = new HashMap<>();
    protected ActorBehaviorMatchKeyFactory matchKeyFactory = new ActorBehaviorMatchKeyFactory();

    public ActorBehaviorBuilderKelp(
            Consumer<List<HistogramProcessor>> histogramProcessorsTarget) {
        this.histogramProcessorsTarget = histogramProcessorsTarget;
    }

    public ActorBehaviorBuilderKelp matchKeyFactory(ActorBehaviorMatchKeyFactory matchKeyFactory) {
        this.matchKeyFactory = matchKeyFactory;
        return this;
    }

    public ActorBehaviorMatchKeyFactory getMatchKeyFactory() {
        return matchKeyFactory;
    }

    @Override
    public <DataType> ActorBehaviorBuilderKelp match(Class<DataType> dataType, Consumer<DataType> handler) {
        super.match(dataType, handler);
        return this;
    }

    @Override
    public <DataType> ActorBehaviorBuilderKelp matchWithSender(Class<DataType> dataType, BiConsumer<DataType, ActorRef> handler) {
        super.matchWithSender(dataType, handler);
        return this;
    }

    @Override
    public ActorBehaviorBuilderKelp matchAny(BiConsumer<Object, ActorRef> handler) {
        super.matchAny(handler);
        return this;
    }

    @Override
    public ActorBehaviorBuilderKelp with(ActorBehavior behavior) {
        super.with(behavior);
        return this;
    }

    @Override
    public ActorBehavior build() {
        with(new ActorBehaviorBundle());
        with(new ActorBehaviorKelpCompleted());
        with(new ActorBehaviorKelpFileSplit());
        ActorBehavior b = super.build();
        histogramProcessorsTarget.accept(
                processors.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList()));
        return b;
    }

    public static class ActorBehaviorBundle implements ActorBehavior {
        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            if (message instanceof ActorKelp.MessageBundle) {
                if (self instanceof ActorKelp<?>) {
                    ((ActorKelp<?>) self).processMessageBundle(
                            (ActorKelp.MessageBundle<Object>) message);
                } else {
                    ActorKelp.processMessageBundle(self,
                            (ActorKelp.MessageBundle<Object>) message);
                }
                return true;
            }
            return false;
        }
    }

    public static class ActorBehaviorKelpCompleted implements ActorBehavior {
        @Override
        public boolean process(Actor self, Message<?> message) {
            if (message.getData() instanceof StagingActor.StagingCompleted &&
                    self instanceof StagingActor.StagingSupported) {
                if (self instanceof ActorKelp<?>) {
                    ((ActorKelp<?>) self).processStagingCompleted(
                            (StagingActor.StagingCompleted) message.getData());
                } else {
                    ActorKelp.processStagingCompleted(self,
                            (StagingActor.StagingCompleted) message.getData());
                }
                return true;
            }
            return false;
        }
    }

    public static class ActorBehaviorKelpFileSplit implements ActorBehavior {
        @Override
        public boolean process(Actor self, Message<?> message) {
            if (message.getData() instanceof FileSplitter.FileSplit &&
                    self instanceof ActorKelp<?>) {
                FileSplitter.FileSplit split = (FileSplitter.FileSplit) message.getData();
                ((ActorKelp<?>) self).processFileSplit(split);
                return true;
            }
            return false;
        }
    }

    /**
     * an entry point of key-value-matching
     * @param valueType the matching message type (the message type is the value type)
     * @param keyExtractorFromValue  a function extracting a key from the valueType message.
     *                                If the message becomes the key, use {@link Function#identity()}
     * @param <ValueType> the value type
     * @param <KeyType> the key type
     * @return a subsequent builder
     */
    public <ValueType, KeyType> KelpMatchKey1<KeyType, ValueType, ValueType> matchKey(
            Class<ValueType> valueType, KeyExtractorFunction<ValueType, KeyType> keyExtractorFromValue) {
        return new KelpMatchKey1<>(this, new KeyExtractorClass<>(valueType, keyExtractorFromValue), Function.identity());
    }

    public <ValueType, KeyType extends Comparable<KeyType>> KelpMatchKey1<KeyType, ValueType, ValueType> matchKeyOrdered(
            Class<ValueType> valueType, KeyExtractorFunction<ValueType, KeyType> keyExtractorFromValue) {
        return new KelpMatchKey1<>(this, new KeyExtractorClass<>(valueType, keyExtractorFromValue), Function.identity())
                .sort(new KeyComparatorOrdered<>());
    }

    /**
     * an entry point of key-value-matching
     * @param valueType the value type extracted from the message
     * @param keyExtractorFromValue  a function extracting a key from the paramType message.
     *                                If the message becomes the key, use {@link Function#identity()}
     * @param valueExtractorFromValue a function extracting a value from the paramType message.
     * @param <ValueType> the value type
     * @param <ParamType> the matching message type
     * @param <KeyType> the key type
     * @return a subsequent builder
     */
    public <ValueType, ParamType, KeyType> KelpMatchKey1<KeyType, ParamType, ValueType> matchKey(
            Class<ParamType> valueType, KeyExtractorFunction<ParamType, KeyType> keyExtractorFromValue, Function<ParamType, ValueType> valueExtractorFromValue) {
        return new KelpMatchKey1<>(this, new KeyExtractorClass<>(valueType, keyExtractorFromValue), valueExtractorFromValue);
    }

    public <ValueType, ParamType, KeyType extends Comparable<KeyType>> KelpMatchKey1<KeyType, ParamType, ValueType> matchKeyOrdered(
            Class<ParamType> valueType, KeyExtractorFunction<ParamType, KeyType> keyExtractorFromValue, Function<ParamType, ValueType> valueExtractorFromValue) {
        return new KelpMatchKey1<>(this, new KeyExtractorClass<>(valueType, keyExtractorFromValue), valueExtractorFromValue)
                .sort(new KeyComparatorOrdered<>());
    }

    public int nextMatchKeyEntry() {
        int id = processors.size();
        processors.put(id, null); //null value for preserving the id
        return id;
    }

    public void setProcessor(int matchKeyEntryId, HistogramProcessor processor) {
        processors.put(matchKeyEntryId, processor);
    }

    public ActorBehaviorBuilderKelp withProcessor(int matchKeyEntryId, ActorBehavior behavior) {
        if (behavior instanceof HistogramProcessor) {
            setProcessor(matchKeyEntryId, (HistogramProcessor) behavior);
        }
        return with(behavior);
    }

    public static class KelpMatchKey<KeyType> {
        protected ActorBehaviorBuilderKelp builder;
        protected KeyComparator<KeyType> keyComparator;

        public KelpMatchKey(ActorBehaviorBuilderKelp builder) {
            this(builder, new KeyComparatorDefault<>());
        }

        public KelpMatchKey(ActorBehaviorBuilderKelp builder, KeyComparator<KeyType> keyComparator) {
            this.builder = builder;
            this.keyComparator = keyComparator;
        }

        public KelpMatchKey<KeyType> sort(KeyComparator<KeyType> keyComparator) {
            this.keyComparator = keyComparator;
            return this;
        }

        protected ActorBehaviorBuilderKelp action(Function<Integer, ActorBehavior> behaviorFactory) {
            int id = builder.nextMatchKeyEntry();
            return builder.withProcessor(id, behaviorFactory.apply(id));
        }

    }

    public static class KelpMatchKey1<KeyType, ParamType, ValueType> extends KelpMatchKey<KeyType> {
        protected KeyExtractor<KeyType, ParamType> extractor1;
        protected Function<ParamType, ValueType> valueExtractor1;

        public KelpMatchKey1(ActorBehaviorBuilderKelp builder, KeyExtractor<KeyType, ParamType> extractor1,
                             Function<ParamType, ValueType> valueExtractor1) {
            super(builder);
            this.extractor1 = extractor1;
            this.valueExtractor1 = valueExtractor1;
        }

        /**
         * optionally matching to another message type.
         * extracted values are paired by "matchKey" and "or" selections with grouping by the same key.
         *   <pre>
         *       .matchKey(T1.class, t1-&gt;k)
         *             .or(T2.class, t2-&gt;k)
         *           =&gt; constructing {k=&gt;[(t1,t2)], k'=&gt;[(t1',t2')], ...}
         *   </pre>
         * @param valueType another value type
         * @param keyExtractorFromValue a function extracting a key from the value message
         * @param <ValueType2> another value message type
         * @return a subsequent builder
         */
        public <ValueType2> KelpMatchKey2<KeyType, ParamType, ValueType2, ValueType, ValueType2> or(
                Class<ValueType2> valueType, KeyExtractorFunction<ValueType2, KeyType> keyExtractorFromValue) {
            return new KelpMatchKey2<>(builder, extractor1, new KeyExtractorClass<>(valueType, keyExtractorFromValue), valueExtractor1, Function.identity());
        }

        /**
         *
         * optionally matching to another message type.
         * extracted values are paired by "matchKey" and "or" selections with grouping by the same key.
         *   <pre>
         *       .matchKey(T1.class, t1-&gt;k, t1-&gt;v1)
         *             .or(T2.class, t2-&gt;k, t2-&gt;v2)
         *           =&gt; constructing {k=&gt;[(v1,v2)], k'=&gt;[(v1',v2')], ...}
         *   </pre>
         * @param valueType another message type
         * @param keyExtractorFromValue a function extracting a key from the value message
         * @param valueExtractor2 a function extracting a value from the message
         * @param <ParamType2> another message type
         * @param <ValueType2> another value type
         * @return a subsequent builder
         *
         */
        public <ParamType2, ValueType2> KelpMatchKey2<KeyType, ParamType, ParamType2, ValueType, ValueType2> or(
                Class<ParamType2> valueType, KeyExtractorFunction<ParamType2, KeyType> keyExtractorFromValue, Function<ParamType2, ValueType2> valueExtractor2) {
            return new KelpMatchKey2<>(builder, extractor1, new KeyExtractorClass<>(valueType, keyExtractorFromValue), valueExtractor1, valueExtractor2);
        }

        @Override
        public KelpMatchKey1<KeyType, ParamType, ValueType> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        /**
         * inserting intermediate reducing operation.
         *   <pre>
         *       .matchKey(T1.class, t1-&gt;k, t1-&gt;v)
         *           =&gt; constructing {k=&gt;[v1,v2,...], k'=&gt;[v3,v4,...], ...}
         *         .reduce((k,vs) -&gt; vs')
         *           =&gt; {k=&gt;[v1'], k'=&gt;[v3'], ...} //reduced lists
         *   </pre>
         *   The reducing will happen on demands.
         * @param keyValuesReducer the reducer (k,vs) -&gt; vs'
         * @return a list builder
         */
        public KelpMatchKeyList<KeyType, ValueType> reduce(BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer) {
            return new KelpMatchKeyList<>(builder, keyComparator, keyValuesReducer,
                    Collections.singletonList(extractor1),
                    Collections.singletonList(valueExtractor1));
        }

        /**
         * inserting intermediate reducing operation.
         *   <pre>
         *       .matchKey(T1.class, t1-&gt;k, t1-&gt;v)
         *           =&gt; constructing {k=&gt;[v1,v2,...], k'=&gt;[v3,v4,...], ...}
         *         .fold((k,vs) -&gt; v')
         *           =&gt; {k=&gt;[v1'], k'=&gt;[v3'], ...} //reduced lists
         *   </pre>
         *   The reducing will happen on demands.
         * @param keyValuesReducer the reducer (k,vs) -&gt; v'
         * @return a list builder
         */
        public KelpMatchKeyList<KeyType, ValueType> fold(BiFunction<KeyType, List<ValueType>, ValueType> keyValuesReducer) {
            return reduce((k,vs) -> Collections.singletonList(keyValuesReducer.apply(k, vs)));
        }

        /**
         * a terminal action for iterating over key-value pairs.
         * The action will be immediately invoked when the value arrived.
         * @param handler the action (k,v)-&gt;()
         * @return an end of matchKey construction
         */
        public ActorBehaviorBuilderKelp forEachKeyValue(BiConsumer<KeyType, ValueType> handler) {
            return action(id -> builder.getMatchKeyFactory().get1(id, keyComparator, extractor1, valueExtractor1, handler));
        }

        /**
         * a terminal action for iterating over the value of key-value pairs.
         * The action will be immediately invoked when the value arrived.
         * @param handler the action (v)-&gt;()
         * @return an end of matchKey construction
         */
        public ActorBehaviorBuilderKelp forEach(Consumer<ValueType> handler) {
            return forEachKeyValue((k,v) -> handler.accept(v));
        }

        public ActorBehaviorBuilderKelp forEachKeyList(int requiredSize, BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> builder.getMatchKeyFactory().getList(id, requiredSize, keyComparator, extractor1, valueExtractor1, handler));
        }

        public ActorBehaviorBuilderKelp forEachKeyList(BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuture(id, keyComparator, extractor1, valueExtractor1, handler));
        }
    }

    public static class KelpMatchKey2<KeyType, ParamType1, ParamType2, ValueType1, ValueType2> extends KelpMatchKey<KeyType> {
        protected KeyExtractor<KeyType, ParamType1> extractor1;
        protected KeyExtractor<KeyType, ParamType2> extractor2;
        protected Function<ParamType1, ValueType1> valueExtractor1;
        protected Function<ParamType2, ValueType2> valueExtractor2;

        public KelpMatchKey2(ActorBehaviorBuilderKelp builder,
                             KeyExtractor<KeyType, ParamType1> extractor1,
                             KeyExtractor<KeyType, ParamType2> extractor2,
                             Function<ParamType1, ValueType1> valueExtractor1,
                             Function<ParamType2, ValueType2> valueExtractor2) {
            super(builder);
            this.extractor1 = extractor1;
            this.extractor2 = extractor2;
            this.valueExtractor1 = valueExtractor1;
            this.valueExtractor2 = valueExtractor2;
        }

        public <ValueType3> KelpMatchKey3<KeyType, ParamType1, ParamType2, ValueType3, ValueType1, ValueType2, ValueType3> or(
                Class<ValueType3> valueType, KeyExtractorFunction<ValueType3, KeyType> keyExtractorFromValue) {
            return new KelpMatchKey3<>(builder, extractor1, extractor2, new KeyExtractorClass<>(valueType, keyExtractorFromValue),
                    valueExtractor1, valueExtractor2, Function.identity());
        }

        public <ParamType3, ValueType3> KelpMatchKey3<KeyType, ParamType1, ParamType2, ParamType3, ValueType1, ValueType2, ValueType3> or(
                Class<ParamType3> valueType, KeyExtractorFunction<ParamType3, KeyType> keyExtractorFromValue, Function<ParamType3, ValueType3> valueExtractorFromValue) {
            return new KelpMatchKey3<>(builder, extractor1, extractor2, new KeyExtractorClass<>(valueType, keyExtractorFromValue),
                    valueExtractor1, valueExtractor2, valueExtractorFromValue);
        }

        @Override
        public KelpMatchKey2<KeyType, ParamType1, ParamType2, ValueType1, ValueType2> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        @SuppressWarnings("unchecked")
        public KelpMatchKeyList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new KelpMatchKeyList<>(builder, keyComparator, keyValuesReducer,
                    Arrays.asList(extractor1, extractor2),
                    Arrays.asList((Function<?,Object>) valueExtractor1, (Function<?,Object>) valueExtractor2));
        }

        public KelpMatchKeyList<KeyType, Object> fold(BiFunction<KeyType, List<Object>, Object> keyValuesReducer) {
            return reduce((k,vs) -> Collections.singletonList(keyValuesReducer.apply(k, vs)));
        }

        public ActorBehaviorBuilderKelp forEachPair(BiConsumer<ValueType1, ValueType2> handler) {
            return forEachKeyPair((k,v1,v2) -> handler.accept(v1,v2));
        }

        public ActorBehaviorBuilderKelp forEachKeyPair(TriConsumer<KeyType, ValueType1, ValueType2> handler) {
            return action(id -> builder.getMatchKeyFactory().get2(id,
                    keyComparator, extractor1, extractor2, valueExtractor1, valueExtractor2, handler));
        }

        public ActorBehaviorBuilderKelp forEachKeyValue(BiConsumer<KeyType, Object> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(k, vs.get(0)));
        }

        public ActorBehaviorBuilderKelp forEachKeyList(int requiredSize, BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> builder.getMatchKeyFactory().getList(id, requiredSize, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2),
                    new ExtractorWithSelection2<>(extractor1, extractor2, valueExtractor1, valueExtractor2), handler));
        }

        public ActorBehaviorBuilderKelp forEachKeyList(BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuture(id, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2),
                    new ExtractorWithSelection2<>(extractor1, extractor2, valueExtractor1, valueExtractor2), handler));
        }
    }

    public static class KelpMatchKey3<KeyType, ParamType1, ParamType2, ParamType3, ValueType1, ValueType2, ValueType3> extends KelpMatchKey<KeyType> {
        protected KeyExtractor<KeyType, ParamType1> extractor1;
        protected KeyExtractor<KeyType, ParamType2> extractor2;
        protected KeyExtractor<KeyType, ParamType3> extractor3;
        protected Function<ParamType1, ValueType1> valueExtractor1;
        protected Function<ParamType2, ValueType2> valueExtractor2;
        protected Function<ParamType3, ValueType3> valueExtractor3;

        public KelpMatchKey3(ActorBehaviorBuilderKelp builder,
                             KeyExtractor<KeyType, ParamType1> extractor1,
                             KeyExtractor<KeyType, ParamType2> extractor2,
                             KeyExtractor<KeyType, ParamType3> extractor3,
                             Function<ParamType1, ValueType1> valueExtractor1,
                             Function<ParamType2, ValueType2> valueExtractor2,
                             Function<ParamType3, ValueType3> valueExtractor3) {
            super(builder);
            this.extractor1 = extractor1;
            this.extractor2 = extractor2;
            this.extractor3 = extractor3;
            this.valueExtractor1 = valueExtractor1;
            this.valueExtractor2 = valueExtractor2;
            this.valueExtractor3 = valueExtractor3;
        }

        public <ValueType4> KelpMatchKey4<KeyType, ParamType1, ParamType2, ParamType3, ValueType4, ValueType1, ValueType2, ValueType3, ValueType4> or(
                Class<ValueType4> valueType, KeyExtractorFunction<ValueType4, KeyType> keyExtractorFromValue) {
            return new KelpMatchKey4<>(builder, extractor1, extractor2, extractor3, new KeyExtractorClass<>(valueType, keyExtractorFromValue),
                    valueExtractor1, valueExtractor2, valueExtractor3, Function.identity());
        }

        public <ParamType4, ValueType4> KelpMatchKey4<KeyType, ParamType1, ParamType2, ParamType3, ParamType4, ValueType1, ValueType2, ValueType3, ValueType4> or(
                Class<ParamType4> valueType, KeyExtractorFunction<ParamType4, KeyType> keyExtractorFromValue, Function<ParamType4, ValueType4> valueExtractorFromValue) {
            return new KelpMatchKey4<>(builder, extractor1, extractor2, extractor3, new KeyExtractorClass<>(valueType, keyExtractorFromValue),
                    valueExtractor1, valueExtractor2, valueExtractor3, valueExtractorFromValue);
        }


        @Override
        public KelpMatchKey3<KeyType, ParamType1, ParamType2, ParamType3, ValueType1, ValueType2, ValueType3> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        @SuppressWarnings("unchecked")
        public KelpMatchKeyList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new KelpMatchKeyList<>(builder, keyComparator, keyValuesReducer,
                    Arrays.asList(extractor1, extractor2, extractor3),
                    Arrays.asList((Function<?,Object>) valueExtractor1, (Function<?,Object>) valueExtractor2, (Function<?,Object>) valueExtractor3));
        }

        public KelpMatchKeyList<KeyType, Object> fold(BiFunction<KeyType, List<Object>, Object> keyValuesReducer) {
            return reduce((k,vs) -> Collections.singletonList(keyValuesReducer.apply(k, vs)));
        }

        public ActorBehaviorBuilderKelp forEachTriple(TriConsumer<ValueType1, ValueType2, ValueType3> handler) {
            return forEachKeyTriple((k,v1,v2,v3) -> handler.accept(v1,v2,v3));
        }

        public ActorBehaviorBuilderKelp forEachKeyTriple(QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> handler) {
            return action(id -> builder.getMatchKeyFactory().get3(id,
                    keyComparator, extractor1, extractor2, extractor3, valueExtractor1, valueExtractor2, valueExtractor3, handler));
        }

        public ActorBehaviorBuilderKelp forEachKeyValue(BiConsumer<KeyType, Object> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(k, vs.get(0)));
        }

        public ActorBehaviorBuilderKelp forEachKeyList(int requiredSize, BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> builder.getMatchKeyFactory().getList(id, requiredSize, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2, extractor3),
                    new ExtractorWithSelection3<>(extractor1, extractor2, extractor3, valueExtractor1, valueExtractor2, valueExtractor3), handler));
        }

        public ActorBehaviorBuilderKelp forEachKeyList(BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuture(id, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2, extractor3),
                    new ExtractorWithSelection3<>(extractor1, extractor2, extractor3, valueExtractor1, valueExtractor2, valueExtractor3), handler));
        }
    }

    public static class KelpMatchKey4<KeyType, ParamType1, ParamType2, ParamType3, ParamType4, ValueType1, ValueType2, ValueType3, ValueType4> extends KelpMatchKey<KeyType> {
        protected KeyExtractor<KeyType, ParamType1> extractor1;
        protected KeyExtractor<KeyType, ParamType2> extractor2;
        protected KeyExtractor<KeyType, ParamType3> extractor3;
        protected KeyExtractor<KeyType, ParamType4> extractor4;
        protected Function<ParamType1, ValueType1> valueExtractor1;
        protected Function<ParamType2, ValueType2> valueExtractor2;
        protected Function<ParamType3, ValueType3> valueExtractor3;
        protected Function<ParamType4, ValueType4> valueExtractor4;

        public KelpMatchKey4(ActorBehaviorBuilderKelp builder,
                             KeyExtractor<KeyType, ParamType1> extractor1,
                             KeyExtractor<KeyType, ParamType2> extractor2,
                             KeyExtractor<KeyType, ParamType3> extractor3,
                             KeyExtractor<KeyType, ParamType4> extractor4,
                             Function<ParamType1, ValueType1> valueExtractor1,
                             Function<ParamType2, ValueType2> valueExtractor2,
                             Function<ParamType3, ValueType3> valueExtractor3,
                             Function<ParamType4, ValueType4> valueExtractor4) {
            super(builder);
            this.extractor1 = extractor1;
            this.extractor2 = extractor2;
            this.extractor3 = extractor3;
            this.extractor4 = extractor4;
            this.valueExtractor1 = valueExtractor1;
            this.valueExtractor2 = valueExtractor2;
            this.valueExtractor3 = valueExtractor3;
            this.valueExtractor4 = valueExtractor4;
        }

        @SuppressWarnings("unchecked")
        public <ValueType5> KelpMatchKeyList<KeyType, Object> or(
                Class<ValueType5> valueType, KeyExtractorFunction<ValueType5, KeyType> keyExtractorFromValue) {
            return new KelpMatchKeyList<>(builder,
                    Arrays.asList(extractor1, extractor2, extractor3, extractor4, new KeyExtractorClass<>(valueType, keyExtractorFromValue)),
                    Arrays.asList((Function<?,Object>) valueExtractor1, (Function<?,Object>) valueExtractor2, (Function<?,Object>) valueExtractor3, (Function<?,Object>) valueExtractor4,
                            (Function<?,Object>) v -> v));
        }

        @SuppressWarnings("unchecked")
        public <ParamType5, ValueType5> KelpMatchKeyList<KeyType, Object> or(
                Class<ParamType5> valueType, KeyExtractorFunction<ParamType5, KeyType> keyExtractorFromValue, Function<ParamType5, ValueType5> valueExtractorFromValue) {
            return new KelpMatchKeyList<>(builder,
                    Arrays.asList(extractor1, extractor2, extractor3, extractor4, new KeyExtractorClass<>(valueType, keyExtractorFromValue)),
                    Arrays.asList((Function<?,Object>) valueExtractor1, (Function<?,Object>) valueExtractor2, (Function<?,Object>) valueExtractor3, (Function<?,Object>) valueExtractor4,
                            (Function<?,Object>) valueExtractorFromValue));
        }

        @Override
        public KelpMatchKey4<KeyType, ParamType1, ParamType2, ParamType3, ParamType4, ValueType1, ValueType2, ValueType3, ValueType4> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        @SuppressWarnings("unchecked")
        public KelpMatchKeyList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new KelpMatchKeyList<>(builder, keyComparator, keyValuesReducer,
                    Arrays.asList(extractor1, extractor2, extractor3, extractor4),
                    Arrays.asList((Function<?,Object>) valueExtractor1, (Function<?,Object>) valueExtractor2, (Function<?,Object>) valueExtractor3, (Function<?,Object>) valueExtractor4));
        }

        public KelpMatchKeyList<KeyType, Object> fold(BiFunction<KeyType, List<Object>, Object> keyValuesReducer) {
            return reduce((k,vs) -> Collections.singletonList(keyValuesReducer.apply(k, vs)));
        }

        public ActorBehaviorBuilderKelp forEachQuad(QuadConsumer<ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            return forEachKeyQuad((k,v1,v2,v3,v4) -> handler.accept(v1,v2,v3,v4));
        }

        public ActorBehaviorBuilderKelp forEachKeyQuad(QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            return action(id -> builder.getMatchKeyFactory().get4(id,
                            keyComparator, extractor1, extractor2, extractor3, extractor4,
                            valueExtractor1, valueExtractor2, valueExtractor3, valueExtractor4, handler));
        }

        public ActorBehaviorBuilderKelp forEachKeyValue(BiConsumer<KeyType, Object> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(k, vs.get(0)));
        }

        public ActorBehaviorBuilderKelp forEachKeyList(int requiredSize, BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> builder.getMatchKeyFactory().getList(id, requiredSize, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2, extractor3, extractor4),
                    new ExtractorWithSelection4<>(extractor1, extractor2, extractor3, extractor4,
                            valueExtractor1, valueExtractor2, valueExtractor3, valueExtractor4), handler));
        }

        public ActorBehaviorBuilderKelp forEachKeyList(BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuture(id, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2, extractor3, extractor4),
                    new ExtractorWithSelection4<>(extractor1, extractor2, extractor3, extractor4,
                            valueExtractor1, valueExtractor2, valueExtractor3, valueExtractor4), handler));
        }
    }


    public static class KelpMatchKeyList<KeyType, ValueType> extends KelpMatchKey<KeyType> {
        protected List<KeyExtractor<KeyType,?>> extractors;
        protected List<Function<?, ValueType>> valueExtractors;
        protected List<BiFunction<KeyType, List<ValueType>, Iterable<ValueType>>> keyValuesReducers;

        public KelpMatchKeyList(ActorBehaviorBuilderKelp builder,
                                List<KeyExtractor<KeyType, ?>> extractors,
                                List<Function<?, ValueType>> valueExtractors) {
            super(builder);
            this.extractors = new ArrayList<>(extractors);
            this.valueExtractors = new ArrayList<>(valueExtractors);
            this.keyValuesReducers = new ArrayList<>(3);
        }

        public KelpMatchKeyList(ActorBehaviorBuilderKelp builder,
                                KeyComparator<KeyType> keyComparator,
                                BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                List<KeyExtractor<KeyType, ?>> extractors,
                                List<Function<?, ValueType>> valueExtractors) {
            super(builder, keyComparator);
            this.extractors = new ArrayList<>(extractors);
            this.valueExtractors = new ArrayList<>(valueExtractors);
            this.keyValuesReducers = new ArrayList<>(3);
            keyValuesReducers.add(keyValuesReducer);
        }

        public KelpMatchKeyList(ActorBehaviorBuilderKelp builder,
                                KeyComparator<KeyType> keyComparator,
                                List<BiFunction<KeyType, List<ValueType>, Iterable<ValueType>>> keyValuesReducers,
                                List<KeyExtractor<KeyType, ?>> extractors,
                                List<Function<?, ValueType>> valueExtractors) {
            super(builder, keyComparator);
            this.extractors = extractors;
            this.keyValuesReducers = keyValuesReducers;
            this.valueExtractors = valueExtractors;
        }

        public KelpMatchKeyList<KeyType,ValueType> or(
                Class<ValueType> valueType, KeyExtractorFunction<ValueType, KeyType> keyExtractorFromValue) {
            extractors.add(new KeyExtractorClass<>(valueType, keyExtractorFromValue));
            return this;
        }

        public <ParamType> KelpMatchKeyList<KeyType,ValueType> or(
                Class<ParamType> valueType, KeyExtractorFunction<ParamType, KeyType> keyExtractorFromValue, Function<ParamType, ValueType> valueExtractorFromValue) {
            extractors.add(new KeyExtractorClass<>(valueType, keyExtractorFromValue));
            valueExtractors.add(valueExtractorFromValue);
            return this;
        }

        @Override
        public KelpMatchKeyList<KeyType, ValueType> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        public KelpMatchKeyList<KeyType, ValueType> reduce(BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer) {
            keyValuesReducers.add(keyValuesReducer);
            return this;
        }

        public KelpMatchKeyList<KeyType, ValueType> fold(BiFunction<KeyType, List<ValueType>, ValueType> keyValuesReducer) {
            return reduce((k,vs) -> Collections.singletonList(keyValuesReducer.apply(k, vs)));
        }

        public ActorBehaviorBuilderKelp forEachKeyValue(BiConsumer<KeyType, ValueType> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(k, vs.get(0)));
        }

        public ActorBehaviorBuilderKelp forEach(Consumer<ValueType> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(vs.get(0)));
        }

        public ActorBehaviorBuilderKelp forEachKeyList(int requiredSize, BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> builder.getMatchKeyFactory().getList(id, requiredSize, keyComparator,
                    new KeyExtractorList<>(extractors),
                    valueExtractorList(), handler)
                    .withKeyValuesReducers(keyValuesReducers));
        }

        public ActorBehaviorBuilderKelp forEachKeyList(BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuture(id, keyComparator,
                    new KeyValuesReducerList<>(keyValuesReducers),
                    new KeyExtractorList<>(extractors),
                    valueExtractorList(), handler));
        }

        @SuppressWarnings({"rawtype", "unchecked"})
        protected ExtractorWithSelectionList<Object, ValueType> valueExtractorList() {
            return new ExtractorWithSelectionList<Object, ValueType>(List.class.cast(extractors), List.class.cast(valueExtractors));
        }

        public KelpMatchKeyListPhase<KeyType, ValueType> eventually() {
            return new KelpMatchKeyListPhase<>(builder, keyComparator, keyValuesReducers, extractors, valueExtractors);
        }
    }

    public static class KelpMatchKeyListPhase<KeyType, ValueType> extends KelpMatchKeyList<KeyType, ValueType> {
        public KelpMatchKeyListPhase(ActorBehaviorBuilderKelp builder, List<KeyExtractor<KeyType, ?>> keyExtractors,
                                     List<Function<?, ValueType>> valueExtractors) {
            super(builder, keyExtractors, valueExtractors);
        }

        public KelpMatchKeyListPhase(ActorBehaviorBuilderKelp builder, KeyComparator<KeyType> keyComparator,
                                     BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                     List<KeyExtractor<KeyType, ?>> keyExtractors,
                                     List<Function<?, ValueType>> valueExtractors) {
            super(builder, keyComparator, keyValuesReducer, keyExtractors, valueExtractors);
        }

        public KelpMatchKeyListPhase(ActorBehaviorBuilderKelp builder, KeyComparator<KeyType> keyComparator,
                                     List<BiFunction<KeyType, List<ValueType>, Iterable<ValueType>>> keyValuesReducers,
                                     List<KeyExtractor<KeyType, ?>> extractors,
                                     List<Function<?, ValueType>> valueExtractors) {
            super(builder, keyComparator, keyValuesReducers, extractors, valueExtractors);
        }

        public ActorBehaviorBuilderKelp forEachKeyList(int requiredSize, BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuturePhase(id, requiredSize, keyComparator,
                    new KeyValuesReducerList<>(keyValuesReducers),
                    new KeyExtractorList<>(extractors),
                    valueExtractorList(),
                    handler));
        }

        public ActorBehaviorBuilderKelp forEachKeyList(BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuturePhase(id, 1, keyComparator,
                    new KeyValuesReducerList<>(keyValuesReducers),
                    new KeyExtractorList<>(extractors),
                    valueExtractorList(), handler));
        }
    }




    public static class ActorBehaviorMatchKeyFactory {
        public <KeyType, ParamType1, ValueType1> ActorBehavior get1(int matchKeyEntryId, KeyComparator<KeyType> keyComparator,
                                                  KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                                  Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                                  BiConsumer<KeyType, ValueType1> handler) {
            return new ActorBehaviorKelp.ActorBehaviorMatchKey1<>(matchKeyEntryId, keyComparator,
                    keyExtractorFromValue1,
                    valueExtractorFromValue1, handler);
        }

        public <KeyType, ParamType1, ParamType2, ValueType1, ValueType2> ActorBehavior get2(int matchKeyEntryId, KeyComparator<KeyType> keyComparator,
                                                KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                                KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2,
                                                Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                                Function<ParamType2, ValueType2> valueExtractorFromValue2,
                                                TriConsumer<KeyType, ValueType1, ValueType2> handler) {
            return new ActorBehaviorKelp.ActorBehaviorMatchKey2<>(matchKeyEntryId, keyComparator,
                    keyExtractorFromValue1, keyExtractorFromValue2,
                    valueExtractorFromValue1, valueExtractorFromValue2, handler);
        }

        public <KeyType, ParamType1, ParamType2, ParamType3, ValueType1, ValueType2, ValueType3> ActorBehavior get3(int matchKeyEntryId, KeyComparator<KeyType> keyComparator,
                                                KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                                KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2,
                                                KeyExtractor<KeyType, ParamType3> keyExtractorFromValue3,
                                                Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                                Function<ParamType2, ValueType2> valueExtractorFromValue2,
                                                Function<ParamType3, ValueType3> valueExtractorFromValue3,
                                                QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> handler) {
            return new ActorBehaviorKelp.ActorBehaviorMatchKey3<>(matchKeyEntryId, keyComparator,
                    keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3,
                    valueExtractorFromValue1, valueExtractorFromValue2, valueExtractorFromValue3, handler);
        }

        public <KeyType, ParamType1, ParamType2, ParamType3, ParamType4, ValueType1, ValueType2, ValueType3, ValueType4> ActorBehavior get4(int matchKeyEntryId, KeyComparator<KeyType> keyComparator,
                                                KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                                KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2,
                                                KeyExtractor<KeyType, ParamType3> keyExtractorFromValue3,
                                                KeyExtractor<KeyType, ParamType4> keyExtractorFromValue4,
                                                Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                                Function<ParamType2, ValueType2> valueExtractorFromValue2,
                                                Function<ParamType3, ValueType3> valueExtractorFromValue3,
                                                Function<ParamType4, ValueType4> valueExtractorFromValue4,
                                                QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            return new ActorBehaviorKelp.ActorBehaviorMatchKey4<>(matchKeyEntryId, keyComparator,
                    keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3, keyExtractorFromValue4,
                    valueExtractorFromValue1, valueExtractorFromValue2, valueExtractorFromValue3, valueExtractorFromValue4, handler);
        }

        public <KeyType, ParamType, ValueType> ActorBehaviorMatchKeyList<KeyType, ParamType, ValueType> getList(int matchKeyEntryId, int threshold, KeyComparator<KeyType> keyComparator,
                                               KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                               Function<ParamType, ValueType> valueExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            return new ActorBehaviorMatchKeyList<>(matchKeyEntryId, threshold,
                    keyComparator, keyExtractorFromValue, valueExtractorFromValue, handler);
        }

        public <KeyType, ParamType, ValueType> ActorBehaviorMatchKeyListFuture<KeyType, ParamType, ValueType> getListFuture(int matchKeyEntryId,
                                               KeyComparator<KeyType> keyComparator,
                                               KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                               Function<ParamType, ValueType> valueExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            return getListFuture(matchKeyEntryId, 1,
                    keyComparator, (k,vs) -> vs, keyExtractorFromValue, valueExtractorFromValue, handler);
        }


        public <KeyType, ParamType, ValueType> ActorBehaviorMatchKeyListFuture<KeyType, ParamType, ValueType> getListFuture(int matchKeyEntryId,
                                               KeyComparator<KeyType> keyComparator,
                                               BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                               KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                               Function<ParamType, ValueType> valueExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            return getListFuture(matchKeyEntryId, 1, keyComparator,
                    keyValuesReducer, keyExtractorFromValue, valueExtractorFromValue, handler);
        }


        public <KeyType, ParamType, ValueType> ActorBehaviorMatchKeyListFuture<KeyType, ParamType, ValueType> getListFuture(int matchKeyEntryId, int requiredSize,
                                              KeyComparator<KeyType> keyComparator,
                                              BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                              KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                              Function<ParamType, ValueType> valueExtractorFromValue,
                                              BiConsumer<KeyType, List<ValueType>> handler) {
            return new ActorBehaviorMatchKeyListFuture<>(matchKeyEntryId, requiredSize,
                    keyComparator, keyValuesReducer, keyExtractorFromValue, valueExtractorFromValue, handler);
        }


        public <KeyType, ParamType, ValueType> ActorBehaviorMatchKeyListFutureStageEnd<KeyType, ParamType, ValueType> getListFuturePhase(int matchKeyEntryId, int requiredSize,
                                                                                                                                         KeyComparator<KeyType> keyComparator,
                                                                                                                                         BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                                                                                                                         KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                                                                                                                         Function<ParamType, ValueType> valueExtractorFromValue,
                                                                                                                                         BiConsumer<KeyType, List<ValueType>> handler) {
            return new ActorBehaviorMatchKeyListFutureStageEnd<>(matchKeyEntryId, requiredSize,
                    keyComparator, keyValuesReducer, keyExtractorFromValue, valueExtractorFromValue, handler);
        }

    }


}
