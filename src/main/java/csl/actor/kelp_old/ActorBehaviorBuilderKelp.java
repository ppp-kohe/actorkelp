package csl.actor.kelp_old;

import csl.actor.ActorBehavior;
import csl.actor.ActorBehaviorBuilder;
import csl.actor.ActorRef;
import csl.actor.kelp_old.ActorBehaviorKelp.ActorBehaviorMatchKeyList;
import csl.actor.kelp_old.ActorBehaviorKelp.ActorBehaviorMatchKeyListFuture;
import csl.actor.kelp_old.ActorBehaviorKelp.ActorBehaviorMatchKeyListFuturePhase;
import csl.actor.kelp_old.KeyHistograms.KeyComparator;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Deprecated
public class ActorBehaviorBuilderKelp extends ActorBehaviorBuilder {
    protected Consumer<List<MailboxKelp.HistogramProcessor>> histogramProcessorsTarget;
    protected Map<Integer, MailboxKelp.HistogramProcessor> processors = new HashMap<>();
    protected ActorBehaviorMatchKeyFactory matchKeyFactory = new ActorBehaviorMatchKeyFactory();

    public ActorBehaviorBuilderKelp(
            Consumer<List<MailboxKelp.HistogramProcessor>> histogramProcessorsTarget) {
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
        ActorBehavior b = super.build();
        histogramProcessorsTarget.accept(
                processors.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList()));
        return b;
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
    public <ValueType, KeyType> RelayToCollect1<KeyType, ValueType, ValueType> matchKey(
            Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
        return new RelayToCollect1<>(this, new KeyExtractorClass<>(valueType, keyExtractorFromValue), Function.identity());
    }

    public <ValueType, KeyType extends Comparable<KeyType>> RelayToCollect1<KeyType, ValueType, ValueType> matchKeyOrdered(
            Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
        return new RelayToCollect1<>(this, new KeyExtractorClass<>(valueType, keyExtractorFromValue), Function.identity())
                .sort(new ActorBehaviorKelp.KeyComparatorOrdered<>());
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
    public <ValueType, ParamType, KeyType> RelayToCollect1<KeyType, ParamType, ValueType> matchKey(
            Class<ParamType> valueType, Function<ParamType, KeyType> keyExtractorFromValue, Function<ParamType, ValueType> valueExtractorFromValue) {
        return new RelayToCollect1<>(this, new KeyExtractorClass<>(valueType, keyExtractorFromValue), valueExtractorFromValue);
    }

    public <ValueType, ParamType, KeyType extends Comparable<KeyType>> RelayToCollect1<KeyType, ParamType, ValueType> matchKeyOrdered(
            Class<ParamType> valueType, Function<ParamType, KeyType> keyExtractorFromValue, Function<ParamType, ValueType> valueExtractorFromValue) {
        return new RelayToCollect1<>(this, new KeyExtractorClass<>(valueType, keyExtractorFromValue), valueExtractorFromValue)
                .sort(new ActorBehaviorKelp.KeyComparatorOrdered<>());
    }

    public int nextMatchKeyEntry() {
        int id = processors.size();
        processors.put(id, null); //null value for preserving the id
        return id;
    }

    public void setProcessor(int matchKeyEntryId, MailboxKelp.HistogramProcessor processor) {
        processors.put(matchKeyEntryId, processor);
    }

    public ActorBehaviorBuilderKelp withProcessor(int matchKeyEntryId, ActorBehavior behavior) {
        if (behavior instanceof MailboxKelp.HistogramProcessor) {
            setProcessor(matchKeyEntryId, (MailboxKelp.HistogramProcessor) behavior);
        }
        return with(behavior);
    }

    public static class RelayToCollect<KeyType> {
        protected ActorBehaviorBuilderKelp builder;
        protected KeyComparator<KeyType> keyComparator;

        public RelayToCollect(ActorBehaviorBuilderKelp builder) {
            this(builder, new KeyComparatorDefault<>());
        }

        public RelayToCollect(ActorBehaviorBuilderKelp builder, KeyComparator<KeyType> keyComparator) {
            this.builder = builder;
            this.keyComparator = keyComparator;
        }

        public RelayToCollect<KeyType> sort(KeyComparator<KeyType> keyComparator) {
            this.keyComparator = keyComparator;
            return this;
        }

        protected ActorBehaviorBuilderKelp action(Function<Integer, ActorBehavior> behaviorFactory) {
            int id = builder.nextMatchKeyEntry();
            return builder.withProcessor(id, behaviorFactory.apply(id));
        }

    }

    public static class RelayToCollect1<KeyType, ParamType, ValueType> extends RelayToCollect<KeyType> {
        protected KeyExtractor<KeyType, ParamType> extractor1;
        protected Function<ParamType, ValueType> valueExtractor1;

        public RelayToCollect1(ActorBehaviorBuilderKelp builder, KeyExtractor<KeyType, ParamType> extractor1,
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
        public <ValueType2> RelayToCollect2<KeyType, ParamType, ValueType2, ValueType, ValueType2> or(
                Class<ValueType2> valueType, Function<ValueType2, KeyType> keyExtractorFromValue) {
            return new RelayToCollect2<>(builder, extractor1, new KeyExtractorClass<>(valueType, keyExtractorFromValue), valueExtractor1, Function.identity());
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
        public <ParamType2, ValueType2> RelayToCollect2<KeyType, ParamType, ParamType2, ValueType, ValueType2> or(
                Class<ParamType2> valueType, Function<ParamType2, KeyType> keyExtractorFromValue, Function<ParamType2, ValueType2> valueExtractor2) {
            return new RelayToCollect2<>(builder, extractor1, new KeyExtractorClass<>(valueType, keyExtractorFromValue), valueExtractor1, valueExtractor2);
        }

        @Override
        public RelayToCollect1<KeyType, ParamType, ValueType> sort(KeyComparator<KeyType> keyComparator) {
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
        public RelayToCollectList<KeyType, ValueType> reduce(BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer) {
            return new RelayToCollectList<>(builder, keyComparator, keyValuesReducer,
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
        public RelayToCollectList<KeyType, ValueType> fold(BiFunction<KeyType, List<ValueType>, ValueType> keyValuesReducer) {
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

    public static class RelayToCollect2<KeyType, ParamType1, ParamType2, ValueType1, ValueType2> extends RelayToCollect<KeyType> {
        protected KeyExtractor<KeyType, ParamType1> extractor1;
        protected KeyExtractor<KeyType, ParamType2> extractor2;
        protected Function<ParamType1, ValueType1> valueExtractor1;
        protected Function<ParamType2, ValueType2> valueExtractor2;

        public RelayToCollect2(ActorBehaviorBuilderKelp builder,
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

        public <ValueType3> RelayToCollect3<KeyType, ParamType1, ParamType2, ValueType3, ValueType1, ValueType2, ValueType3> or(
                Class<ValueType3> valueType, Function<ValueType3, KeyType> keyExtractorFromValue) {
            return new RelayToCollect3<>(builder, extractor1, extractor2, new KeyExtractorClass<>(valueType, keyExtractorFromValue),
                    valueExtractor1, valueExtractor2, Function.identity());
        }

        public <ParamType3, ValueType3> RelayToCollect3<KeyType, ParamType1, ParamType2, ParamType3, ValueType1, ValueType2, ValueType3> or(
                Class<ParamType3> valueType, Function<ParamType3, KeyType> keyExtractorFromValue, Function<ParamType3, ValueType3> valueExtractorFromValue) {
            return new RelayToCollect3<>(builder, extractor1, extractor2, new KeyExtractorClass<>(valueType, keyExtractorFromValue),
                    valueExtractor1, valueExtractor2, valueExtractorFromValue);
        }

        @Override
        public RelayToCollect2<KeyType, ParamType1, ParamType2, ValueType1, ValueType2> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        @SuppressWarnings("unchecked")
        public RelayToCollectList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new RelayToCollectList<>(builder, keyComparator, keyValuesReducer,
                    Arrays.asList(extractor1, extractor2),
                    Arrays.asList((Function<?,Object>) valueExtractor1, (Function<?,Object>) valueExtractor2));
        }

        public RelayToCollectList<KeyType, Object> fold(BiFunction<KeyType, List<Object>, Object> keyValuesReducer) {
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

    public static class ExtractorWithSelection2<ParamType1, ParamType2> implements Function<Object,Object> {
        protected KeyExtractor<?, ?> keyExtractor1;
        protected KeyExtractor<?, ?> keyExtractor2;
        protected Function<ParamType1,?> valueExtractor1;
        protected Function<ParamType2,?> valueExtractor2;

        public ExtractorWithSelection2(KeyExtractor<?, ?> keyExtractor1, KeyExtractor<?, ?> keyExtractor2,
                                      Function<ParamType1, ?> valueExtractor1, Function<ParamType2, ?> valueExtractor2) {
            this.keyExtractor1 = keyExtractor1;
            this.keyExtractor2 = keyExtractor2;
            this.valueExtractor1 = valueExtractor1;
            this.valueExtractor2 = valueExtractor2;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object apply(Object o) {
            if (keyExtractor1.matchValue(o)) {
                return valueExtractor1.apply((ParamType1) o);
            } else if (keyExtractor2.matchValue(o)) {
                return valueExtractor2.apply((ParamType2) o);
            }
            return null;
        }
    }

    public static class RelayToCollect3<KeyType, ParamType1, ParamType2, ParamType3, ValueType1, ValueType2, ValueType3> extends RelayToCollect<KeyType> {
        protected KeyExtractor<KeyType, ParamType1> extractor1;
        protected KeyExtractor<KeyType, ParamType2> extractor2;
        protected KeyExtractor<KeyType, ParamType3> extractor3;
        protected Function<ParamType1, ValueType1> valueExtractor1;
        protected Function<ParamType2, ValueType2> valueExtractor2;
        protected Function<ParamType3, ValueType3> valueExtractor3;

        public RelayToCollect3(ActorBehaviorBuilderKelp builder,
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

        public <ValueType4> RelayToCollect4<KeyType, ParamType1, ParamType2, ParamType3, ValueType4, ValueType1, ValueType2, ValueType3, ValueType4> or(
                Class<ValueType4> valueType, Function<ValueType4, KeyType> keyExtractorFromValue) {
            return new RelayToCollect4<>(builder, extractor1, extractor2, extractor3, new KeyExtractorClass<>(valueType, keyExtractorFromValue),
                    valueExtractor1, valueExtractor2, valueExtractor3, Function.identity());
        }

        public <ParamType4, ValueType4> RelayToCollect4<KeyType, ParamType1, ParamType2, ParamType3, ParamType4, ValueType1, ValueType2, ValueType3, ValueType4> or(
                Class<ParamType4> valueType, Function<ParamType4, KeyType> keyExtractorFromValue, Function<ParamType4, ValueType4> valueExtractorFromValue) {
            return new RelayToCollect4<>(builder, extractor1, extractor2, extractor3, new KeyExtractorClass<>(valueType, keyExtractorFromValue),
                    valueExtractor1, valueExtractor2, valueExtractor3, valueExtractorFromValue);
        }


        @Override
        public RelayToCollect3<KeyType, ParamType1, ParamType2, ParamType3, ValueType1, ValueType2, ValueType3> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        @SuppressWarnings("unchecked")
        public RelayToCollectList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new RelayToCollectList<>(builder, keyComparator, keyValuesReducer,
                    Arrays.asList(extractor1, extractor2, extractor3),
                    Arrays.asList((Function<?,Object>) valueExtractor1, (Function<?,Object>) valueExtractor2, (Function<?,Object>) valueExtractor3));
        }

        public RelayToCollectList<KeyType, Object> fold(BiFunction<KeyType, List<Object>, Object> keyValuesReducer) {
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

    public static class ExtractorWithSelection3<ParamType1, ParamType2, ParamType3> implements Function<Object,Object> {
        protected KeyExtractor<?, ?> keyExtractor1;
        protected KeyExtractor<?, ?> keyExtractor2;
        protected KeyExtractor<?, ?> keyExtractor3;
        protected Function<ParamType1,?> valueExtractor1;
        protected Function<ParamType2,?> valueExtractor2;
        protected Function<ParamType3,?> valueExtractor3;

        public ExtractorWithSelection3(KeyExtractor<?, ?> keyExtractor1, KeyExtractor<?, ?> keyExtractor2, KeyExtractor<?, ?> keyExtractor3,
                                      Function<ParamType1, ?> valueExtractor1, Function<ParamType2, ?> valueExtractor2, Function<ParamType3, ?> valueExtractor3) {
            this.keyExtractor1 = keyExtractor1;
            this.keyExtractor2 = keyExtractor2;
            this.keyExtractor3 = keyExtractor3;
            this.valueExtractor1 = valueExtractor1;
            this.valueExtractor2 = valueExtractor2;
            this.valueExtractor3 = valueExtractor3;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object apply(Object o) {
            if (keyExtractor1.matchValue(o)) {
                return valueExtractor1.apply((ParamType1) o);
            } else if (keyExtractor2.matchValue(o)) {
                return valueExtractor2.apply((ParamType2) o);
            } else if (keyExtractor3.matchValue(o)) {
                return valueExtractor3.apply((ParamType3) o);
            }
            return null;
        }
    }


    public static class RelayToCollect4<KeyType, ParamType1, ParamType2, ParamType3, ParamType4, ValueType1, ValueType2, ValueType3, ValueType4> extends RelayToCollect<KeyType> {
        protected KeyExtractor<KeyType, ParamType1> extractor1;
        protected KeyExtractor<KeyType, ParamType2> extractor2;
        protected KeyExtractor<KeyType, ParamType3> extractor3;
        protected KeyExtractor<KeyType, ParamType4> extractor4;
        protected Function<ParamType1, ValueType1> valueExtractor1;
        protected Function<ParamType2, ValueType2> valueExtractor2;
        protected Function<ParamType3, ValueType3> valueExtractor3;
        protected Function<ParamType4, ValueType4> valueExtractor4;

        public RelayToCollect4(ActorBehaviorBuilderKelp builder,
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
        public <ValueType5> RelayToCollectList<KeyType, Object> or(
                Class<ValueType5> valueType, Function<ValueType5, KeyType> keyExtractorFromValue) {
            return new RelayToCollectList<>(builder,
                    Arrays.asList(extractor1, extractor2, extractor3, extractor4, new KeyExtractorClass<>(valueType, keyExtractorFromValue)),
                    Arrays.asList((Function<?,Object>) valueExtractor1, (Function<?,Object>) valueExtractor2, (Function<?,Object>) valueExtractor3, (Function<?,Object>) valueExtractor4,
                            (Function<?,Object>) v -> v));
        }

        @SuppressWarnings("unchecked")
        public <ParamType5, ValueType5> RelayToCollectList<KeyType, Object> or(
                Class<ParamType5> valueType, Function<ParamType5, KeyType> keyExtractorFromValue, Function<ParamType5, ValueType5> valueExtractorFromValue) {
            return new RelayToCollectList<>(builder,
                    Arrays.asList(extractor1, extractor2, extractor3, extractor4, new KeyExtractorClass<>(valueType, keyExtractorFromValue)),
                    Arrays.asList((Function<?,Object>) valueExtractor1, (Function<?,Object>) valueExtractor2, (Function<?,Object>) valueExtractor3, (Function<?,Object>) valueExtractor4,
                            (Function<?,Object>) valueExtractorFromValue));
        }

        @Override
        public RelayToCollect4<KeyType, ParamType1, ParamType2, ParamType3, ParamType4, ValueType1, ValueType2, ValueType3, ValueType4> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        @SuppressWarnings("unchecked")
        public RelayToCollectList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new RelayToCollectList<>(builder, keyComparator, keyValuesReducer,
                    Arrays.asList(extractor1, extractor2, extractor3, extractor4),
                    Arrays.asList((Function<?,Object>) valueExtractor1, (Function<?,Object>) valueExtractor2, (Function<?,Object>) valueExtractor3, (Function<?,Object>) valueExtractor4));
        }

        public RelayToCollectList<KeyType, Object> fold(BiFunction<KeyType, List<Object>, Object> keyValuesReducer) {
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

    public static class ExtractorWithSelection4<ParamType1, ParamType2, ParamType3, ParamType4> implements Function<Object,Object> {
        protected KeyExtractor<?, ?> keyExtractor1;
        protected KeyExtractor<?, ?> keyExtractor2;
        protected KeyExtractor<?, ?> keyExtractor3;
        protected KeyExtractor<?, ?> keyExtractor4;
        protected Function<ParamType1,?> valueExtractor1;
        protected Function<ParamType2,?> valueExtractor2;
        protected Function<ParamType3,?> valueExtractor3;
        protected Function<ParamType4,?> valueExtractor4;

        public ExtractorWithSelection4(KeyExtractor<?, ?> keyExtractor1, KeyExtractor<?, ?> keyExtractor2,
                                       KeyExtractor<?, ?> keyExtractor3, KeyExtractor<?, ?> keyExtractor4,
                                       Function<ParamType1, ?> valueExtractor1, Function<ParamType2, ?> valueExtractor2,
                                       Function<ParamType3, ?> valueExtractor3, Function<ParamType4, ?> valueExtractor4) {
            this.keyExtractor1 = keyExtractor1;
            this.keyExtractor2 = keyExtractor2;
            this.keyExtractor3 = keyExtractor3;
            this.keyExtractor4 = keyExtractor4;
            this.valueExtractor1 = valueExtractor1;
            this.valueExtractor2 = valueExtractor2;
            this.valueExtractor3 = valueExtractor3;
            this.valueExtractor4 = valueExtractor4;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object apply(Object o) {
            if (keyExtractor1.matchValue(o)) {
                return valueExtractor1.apply((ParamType1) o);
            } else if (keyExtractor2.matchValue(o)) {
                return valueExtractor2.apply((ParamType2) o);
            } else if (keyExtractor3.matchValue(o)) {
                return valueExtractor3.apply((ParamType3) o);
            }
            return null;
        }
    }

    public static class RelayToCollectList<KeyType, ValueType> extends RelayToCollect<KeyType> {
        protected List<KeyExtractor<KeyType,?>> extractors;
        protected List<Function<?, ValueType>> valueExtractors;
        protected List<BiFunction<KeyType, List<ValueType>, Iterable<ValueType>>> keyValuesReducers;

        public RelayToCollectList(ActorBehaviorBuilderKelp builder,
                                  List<KeyExtractor<KeyType, ?>> extractors,
                                  List<Function<?, ValueType>> valueExtractors) {
            super(builder);
            this.extractors = new ArrayList<>(extractors);
            this.valueExtractors = new ArrayList<>(valueExtractors);
            this.keyValuesReducers = new ArrayList<>(3);
        }

        public RelayToCollectList(ActorBehaviorBuilderKelp builder,
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

        public RelayToCollectList(ActorBehaviorBuilderKelp builder,
                                  KeyComparator<KeyType> keyComparator,
                                  List<BiFunction<KeyType, List<ValueType>, Iterable<ValueType>>> keyValuesReducers,
                                  List<KeyExtractor<KeyType, ?>> extractors,
                                  List<Function<?, ValueType>> valueExtractors) {
            super(builder, keyComparator);
            this.extractors = extractors;
            this.keyValuesReducers = keyValuesReducers;
            this.valueExtractors = valueExtractors;
        }

        public RelayToCollectList<KeyType,ValueType> or(
                Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
            extractors.add(new KeyExtractorClass<>(valueType, keyExtractorFromValue));
            return this;
        }

        public <ParamType> RelayToCollectList<KeyType,ValueType> or(
                Class<ParamType> valueType, Function<ParamType, KeyType> keyExtractorFromValue, Function<ParamType, ValueType> valueExtractorFromValue) {
            extractors.add(new KeyExtractorClass<>(valueType, keyExtractorFromValue));
            valueExtractors.add(valueExtractorFromValue);
            return this;
        }

        @Override
        public RelayToCollectList<KeyType, ValueType> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        public RelayToCollectList<KeyType, ValueType> reduce(BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer) {
            keyValuesReducers.add(keyValuesReducer);
            return this;
        }

        public RelayToCollectList<KeyType, ValueType> fold(BiFunction<KeyType, List<ValueType>, ValueType> keyValuesReducer) {
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

        public RelayToCollectListPhase<KeyType, ValueType> eventually() {
            return new RelayToCollectListPhase<>(builder, keyComparator, keyValuesReducers, extractors, valueExtractors);
        }
    }

    public static class RelayToCollectListPhase<KeyType, ValueType> extends RelayToCollectList<KeyType, ValueType> {
        public RelayToCollectListPhase(ActorBehaviorBuilderKelp builder, List<KeyExtractor<KeyType, ?>> keyExtractors,
                                       List<Function<?, ValueType>> valueExtractors) {
            super(builder, keyExtractors, valueExtractors);
        }

        public RelayToCollectListPhase(ActorBehaviorBuilderKelp builder, KeyComparator<KeyType> keyComparator,
                                       BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                       List<KeyExtractor<KeyType, ?>> keyExtractors,
                                       List<Function<?, ValueType>> valueExtractors) {
            super(builder, keyComparator, keyValuesReducer, keyExtractors, valueExtractors);
        }

        public RelayToCollectListPhase(ActorBehaviorBuilderKelp builder, KeyComparator<KeyType> keyComparator,
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

    public static class ExtractorWithSelectionList<ParamType, ValueType> implements Function<ParamType, ValueType> {
        protected List<KeyExtractor<?, ?>> keyExtractors;
        protected List<Function<?,?>> valueExtractors;

        public ExtractorWithSelectionList(List<KeyExtractor<?, ?>> keyExtractors,
                                       List<Function<?, ?>> valueExtractors) {
            this.keyExtractors = keyExtractors;
            this.valueExtractors = valueExtractors;
        }

        @SuppressWarnings("unchecked")
        @Override
        public ValueType apply(Object o) {
            int i = 0;
            for (KeyExtractor<?, ?> e : keyExtractors) {
                if (e.matchValue(o)) {
                    return (ValueType) ((Function<Object,Object>) valueExtractors.get(i)).apply(o);
                }
                ++i;
            }
            return null;
        }
    }


    public static class KeyValuesReducerList<KeyType, ValueType> implements BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> {
        protected List<BiFunction<KeyType, List<ValueType>, Iterable<ValueType>>> keyValuesReducers;

        public KeyValuesReducerList(List<BiFunction<KeyType, List<ValueType>, Iterable<ValueType>>> keyValuesReducers) {
            this.keyValuesReducers = keyValuesReducers;
        }

        @Override
        public Iterable<ValueType> apply(KeyType key, List<ValueType> values) {
            List<ValueType> nextInput = values;
            Iterable<ValueType> lastResult = values;
            boolean first = true;
            for (BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> f : keyValuesReducers) {
                if (first) {
                    first = false;
                } else {
                    if (lastResult instanceof List<?>) {
                        nextInput = (List<ValueType>) lastResult;
                    } else {
                        nextInput = new ArrayList<>();
                        for (ValueType v : lastResult) {
                            nextInput.add(v);
                        }
                    }
                }
                if (nextInput.isEmpty()) {
                    break;
                }
                lastResult = f.apply(key, nextInput);
            }
            return lastResult;
        }
    }

    public interface KeyExtractor<KeyType, ValueType> {
        boolean matchValue(Object value);
        KeyType toKey(ValueType v);
    }

    public static class KeyExtractorClass<KeyType, ValueType> implements KeyExtractor<KeyType, ValueType> {
        protected Class<ValueType> valueType;
        protected Function<ValueType, KeyType> keyExtractorFromValue;

        public KeyExtractorClass(Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
            this.valueType = valueType;
            this.keyExtractorFromValue = keyExtractorFromValue;
        }

        public Class<ValueType> getValueType() {
            return valueType;
        }

        public Function<ValueType, KeyType> getKeyExtractorFromValue() {
            return keyExtractorFromValue;
        }

        @Override
        public boolean matchValue(Object value) {
            return valueType.isInstance(value);
        }

        @Override
        public KeyType toKey(ValueType v) {
            return keyExtractorFromValue.apply(v);
        }
    }

    public static class KeyExtractorList<KeyType, ValueType> implements KeyExtractor<KeyType, ValueType> {
        protected List<KeyExtractor<KeyType, ?>> keyExtractors;

        public KeyExtractorList(List<KeyExtractor<KeyType, ?>> keyExtractors) {
            this.keyExtractors = keyExtractors;
        }

        @SafeVarargs
        @SuppressWarnings("varargs")
        public KeyExtractorList(KeyExtractor<KeyType, ?>... keyExtractors) {
            this.keyExtractors = Arrays.asList(keyExtractors);
        }

        @Override
        public boolean matchValue(Object value) {
            for (KeyExtractor<KeyType, ?> e : keyExtractors) {
                if (e.matchValue(value)) {
                    return true;
                }
            }
            return false;
        }

        @SuppressWarnings("unchecked")
        @Override
        public KeyType toKey(ValueType v) {
            for (KeyExtractor<KeyType, ?> e : keyExtractors) {
                if (e.matchValue(v)) {
                    return ((KeyExtractor<KeyType,Object>) e).toKey(v);
                }
            }
            return null;
        }
    }


    public static class KeyComparatorDefault<KeyType> implements KeyComparator<KeyType> {
        public static final long serialVersionUID = 1L;
        @SuppressWarnings("unchecked")
        @Override
        public int compare(KeyType key1, KeyType key2) {
            if (key1 instanceof Comparable<?>) {
                return ((Comparable<Object>) key1).compareTo(key2);
            } else {
                return Integer.compare(Objects.hash(key1), Objects.hash(key2));
            }
        }

        @Override
        public KeyType centerPoint(KeyType leftEnd, KeyType rightStart) {
            if (leftEnd instanceof Comparable<?>) {
                return ActorBehaviorKelp.centerPointPrimitive(leftEnd, rightStart);
            } else {
                return rightStart;
            }
        }
    }


    @FunctionalInterface
    public interface TriConsumer<V1,V2,V3> {
        void accept(V1 v1, V2 v2, V3 v3);
    }

    @FunctionalInterface
    public interface QuadConsumer<V1,V2,V3,V4> {
        void accept(V1 v1, V2 v2, V3 v3, V4 v4);
    }

    @FunctionalInterface
    public interface QuintConsumer<V1,V2,V3,V4,V5> {
        void accept(V1 v1, V2 v2, V3 v3, V4 v4, V5 v5);
    }


    public static class ActorBehaviorMatchKeyFactory {
        public <KeyType, ParamType1, ValueType1> ActorBehavior get1(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                                  ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                                  Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                                  BiConsumer<KeyType, ValueType1> handler) {
            return new ActorBehaviorKelp.ActorBehaviorMatchKey1<>(matchKeyEntryId, keyComparator,
                    keyExtractorFromValue1,
                    valueExtractorFromValue1, handler);
        }

        public <KeyType, ParamType1, ParamType2, ValueType1, ValueType2> ActorBehavior get2(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                                ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                                ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2,
                                                Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                                Function<ParamType2, ValueType2> valueExtractorFromValue2,
                                                TriConsumer<KeyType, ValueType1, ValueType2> handler) {
            return new ActorBehaviorKelp.ActorBehaviorMatchKey2<>(matchKeyEntryId, keyComparator,
                    keyExtractorFromValue1, keyExtractorFromValue2,
                    valueExtractorFromValue1, valueExtractorFromValue2, handler);
        }

        public <KeyType, ParamType1, ParamType2, ParamType3, ValueType1, ValueType2, ValueType3> ActorBehavior get3(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                                ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                                ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2,
                                                ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType3> keyExtractorFromValue3,
                                                Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                                Function<ParamType2, ValueType2> valueExtractorFromValue2,
                                                Function<ParamType3, ValueType3> valueExtractorFromValue3,
                                                QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> handler) {
            return new ActorBehaviorKelp.ActorBehaviorMatchKey3<>(matchKeyEntryId, keyComparator,
                    keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3,
                    valueExtractorFromValue1, valueExtractorFromValue2, valueExtractorFromValue3, handler);
        }

        public <KeyType, ParamType1, ParamType2, ParamType3, ParamType4, ValueType1, ValueType2, ValueType3, ValueType4> ActorBehavior get4(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                                ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                                ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2,
                                                ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType3> keyExtractorFromValue3,
                                                ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType4> keyExtractorFromValue4,
                                                Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                                Function<ParamType2, ValueType2> valueExtractorFromValue2,
                                                Function<ParamType3, ValueType3> valueExtractorFromValue3,
                                                Function<ParamType4, ValueType4> valueExtractorFromValue4,
                                                QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            return new ActorBehaviorKelp.ActorBehaviorMatchKey4<>(matchKeyEntryId, keyComparator,
                    keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3, keyExtractorFromValue4,
                    valueExtractorFromValue1, valueExtractorFromValue2, valueExtractorFromValue3, valueExtractorFromValue4, handler);
        }

        public <KeyType, ParamType, ValueType> ActorBehaviorMatchKeyList<KeyType, ParamType, ValueType> getList(int matchKeyEntryId, int threshold, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                               ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                               Function<ParamType, ValueType> valueExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            return new ActorBehaviorMatchKeyList<>(matchKeyEntryId, threshold,
                    keyComparator, keyExtractorFromValue, valueExtractorFromValue, handler);
        }

        public <KeyType, ParamType, ValueType> ActorBehaviorMatchKeyListFuture<KeyType, ParamType, ValueType> getListFuture(int matchKeyEntryId,
                                               KeyHistograms.KeyComparator<KeyType> keyComparator,
                                               ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                               Function<ParamType, ValueType> valueExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            return getListFuture(matchKeyEntryId, 1,
                    keyComparator, (k,vs) -> vs, keyExtractorFromValue, valueExtractorFromValue, handler);
        }


        public <KeyType, ParamType, ValueType> ActorBehaviorMatchKeyListFuture<KeyType, ParamType, ValueType> getListFuture(int matchKeyEntryId,
                                               KeyHistograms.KeyComparator<KeyType> keyComparator,
                                               BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                               ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                               Function<ParamType, ValueType> valueExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            return getListFuture(matchKeyEntryId, 1, keyComparator,
                    keyValuesReducer, keyExtractorFromValue, valueExtractorFromValue, handler);
        }


        public <KeyType, ParamType, ValueType> ActorBehaviorMatchKeyListFuture<KeyType, ParamType, ValueType> getListFuture(int matchKeyEntryId, int requiredSize,
                                              KeyHistograms.KeyComparator<KeyType> keyComparator,
                                              BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                              ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                              Function<ParamType, ValueType> valueExtractorFromValue,
                                              BiConsumer<KeyType, List<ValueType>> handler) {
            return new ActorBehaviorMatchKeyListFuture<>(matchKeyEntryId, requiredSize,
                    keyComparator, keyValuesReducer, keyExtractorFromValue, valueExtractorFromValue, handler);
        }


        public <KeyType, ParamType, ValueType> ActorBehaviorMatchKeyListFuturePhase<KeyType, ParamType, ValueType> getListFuturePhase(int matchKeyEntryId, int requiredSize,
                                               KeyHistograms.KeyComparator<KeyType> keyComparator,
                                               BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                               ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                               Function<ParamType, ValueType> valueExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            return new ActorBehaviorMatchKeyListFuturePhase<>(matchKeyEntryId, requiredSize,
                    keyComparator, keyValuesReducer, keyExtractorFromValue, valueExtractorFromValue, handler);
        }

    }


}
