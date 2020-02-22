package csl.actor.keyaggregate;

import csl.actor.ActorBehavior;
import csl.actor.ActorBehaviorBuilder;
import csl.actor.ActorRef;
import csl.actor.keyaggregate.ActorBehaviorKeyAggregation.ActorBehaviorMatchKeyList;
import csl.actor.keyaggregate.ActorBehaviorKeyAggregation.ActorBehaviorMatchKeyListFuture;
import csl.actor.keyaggregate.ActorBehaviorKeyAggregation.ActorBehaviorMatchKeyListFuturePhase;
import csl.actor.keyaggregate.KeyHistograms.KeyComparator;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ActorBehaviorBuilderKeyAggregation extends ActorBehaviorBuilder {
    protected Consumer<List<MailboxKeyAggregation.HistogramProcessor>> histogramProcessorsTarget;
    protected Map<Integer, MailboxKeyAggregation.HistogramProcessor> processors = new HashMap<>();
    protected ActorBehaviorMatchKeyFactory matchKeyFactory = new ActorBehaviorMatchKeyFactory();

    public ActorBehaviorBuilderKeyAggregation(
            Consumer<List<MailboxKeyAggregation.HistogramProcessor>> histogramProcessorsTarget) {
        this.histogramProcessorsTarget = histogramProcessorsTarget;
    }

    public ActorBehaviorBuilderKeyAggregation matchKeyFactory(ActorBehaviorMatchKeyFactory matchKeyFactory) {
        this.matchKeyFactory = matchKeyFactory;
        return this;
    }

    public ActorBehaviorMatchKeyFactory getMatchKeyFactory() {
        return matchKeyFactory;
    }

    @Override
    public <DataType> ActorBehaviorBuilderKeyAggregation match(Class<DataType> dataType, Consumer<DataType> handler) {
        super.match(dataType, handler);
        return this;
    }

    @Override
    public <DataType> ActorBehaviorBuilderKeyAggregation matchWithSender(Class<DataType> dataType, BiConsumer<DataType, ActorRef> handler) {
        super.matchWithSender(dataType, handler);
        return this;
    }

    @Override
    public ActorBehaviorBuilderKeyAggregation matchAny(BiConsumer<Object, ActorRef> handler) {
        super.matchAny(handler);
        return this;
    }

    @Override
    public ActorBehaviorBuilderKeyAggregation with(ActorBehavior behavior) {
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

    public <ValueType, KeyType> RelayToCollect1<KeyType, ValueType> matchKey(
            Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
        return new RelayToCollect1<>(this, new KeyExtractorClass<>(valueType, keyExtractorFromValue));
    }

    public <ValueType, KeyType extends Comparable<KeyType>> RelayToCollect1<KeyType, ValueType> matchKeyOrdered(
            Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
        return new RelayToCollect1<>(this, new KeyExtractorClass<>(valueType, keyExtractorFromValue))
                .sort(new ActorBehaviorKeyAggregation.KeyComparatorOrdered<>());
    }

    public int nextMatchKeyEntry() {
        int id = processors.size();
        processors.put(id, null); //null value for preserving the id
        return id;
    }

    public void setProcessor(int matchKeyEntryId, MailboxKeyAggregation.HistogramProcessor processor) {
        processors.put(matchKeyEntryId, processor);
    }

    public ActorBehaviorBuilderKeyAggregation withProcessor(int matchKeyEntryId, ActorBehavior behavior) {
        if (behavior instanceof MailboxKeyAggregation.HistogramProcessor) {
            setProcessor(matchKeyEntryId, (MailboxKeyAggregation.HistogramProcessor) behavior);
        }
        return with(behavior);
    }

    public static class RelayToCollect<KeyType> {
        protected ActorBehaviorBuilderKeyAggregation builder;
        protected KeyComparator<KeyType> keyComparator;

        public RelayToCollect(ActorBehaviorBuilderKeyAggregation builder) {
            this(builder, new KeyComparatorDefault<>());
        }

        public RelayToCollect(ActorBehaviorBuilderKeyAggregation builder, KeyComparator<KeyType> keyComparator) {
            this.builder = builder;
            this.keyComparator = keyComparator;
        }

        public RelayToCollect<KeyType> sort(KeyComparator<KeyType> keyComparator) {
            this.keyComparator = keyComparator;
            return this;
        }

        protected ActorBehaviorBuilderKeyAggregation action(Function<Integer, ActorBehavior> behaviorFactory) {
            int id = builder.nextMatchKeyEntry();
            return builder.withProcessor(id, behaviorFactory.apply(id));
        }

    }

    public static class RelayToCollect1<KeyType, ValueType> extends RelayToCollect<KeyType> {
        protected KeyExtractor<KeyType, ValueType> extractor1;

        public RelayToCollect1(ActorBehaviorBuilderKeyAggregation builder, KeyExtractor<KeyType, ValueType> extractor1) {
            super(builder);
            this.extractor1 = extractor1;
        }

        public <ValueType2> RelayToCollect2<KeyType, ValueType, ValueType2> or(
                Class<ValueType2> valueType, Function<ValueType2, KeyType> keyExtractorFromValue) {
            return new RelayToCollect2<>(builder, extractor1, new KeyExtractorClass<>(valueType, keyExtractorFromValue));
        }

        @Override
        public RelayToCollect1<KeyType, ValueType> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }


        public RelayToCollectList<KeyType, ValueType> reduce(BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer) {
            return new RelayToCollectList<>(builder, keyComparator, keyValuesReducer,
                    Collections.singletonList(extractor1));
        }

        public RelayToCollectList<KeyType, ValueType> fold(BiFunction<KeyType, List<ValueType>, ValueType> keyValuesReducer) {
            return reduce((k,vs) -> Collections.singletonList(keyValuesReducer.apply(k, vs)));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyValue(BiConsumer<KeyType, ValueType> handler) {
            return action(id -> builder.getMatchKeyFactory().get1(id, keyComparator, extractor1, handler));
        }

        public ActorBehaviorBuilderKeyAggregation forEach(Consumer<ValueType> handler) {
            return forEachKeyValue((k,v) -> handler.accept(v));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyList(int requiredSize, BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> builder.getMatchKeyFactory().getList(id, requiredSize, keyComparator, extractor1, handler));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyList(BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuture(id, keyComparator, extractor1, handler));
        }
    }

    public static class RelayToCollect2<KeyType, ValueType1, ValueType2> extends RelayToCollect<KeyType> {
        protected KeyExtractor<KeyType, ValueType1> extractor1;
        protected KeyExtractor<KeyType, ValueType2> extractor2;

        public RelayToCollect2(ActorBehaviorBuilderKeyAggregation builder,
                               KeyExtractor<KeyType, ValueType1> extractor1,
                               KeyExtractor<KeyType, ValueType2> extractor2) {
            super(builder);
            this.extractor1 = extractor1;
            this.extractor2 = extractor2;
        }

        public <ValueType3> RelayToCollect3<KeyType, ValueType1, ValueType2, ValueType3> or(
                Class<ValueType3> valueType, Function<ValueType3, KeyType> keyExtractorFromValue) {
            return new RelayToCollect3<>(builder, extractor1, extractor2, new KeyExtractorClass<>(valueType, keyExtractorFromValue));
        }


        @Override
        public RelayToCollect2<KeyType, ValueType1, ValueType2> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        public RelayToCollectList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new RelayToCollectList<>(builder, keyComparator, keyValuesReducer,
                    Arrays.asList(extractor1, extractor2));
        }

        public RelayToCollectList<KeyType, Object> fold(BiFunction<KeyType, List<Object>, Object> keyValuesReducer) {
            return reduce((k,vs) -> Collections.singletonList(keyValuesReducer.apply(k, vs)));
        }

        public ActorBehaviorBuilderKeyAggregation forEachPair(BiConsumer<ValueType1, ValueType2> handler) {
            return forEachKeyPair((k,v1,v2) -> handler.accept(v1,v2));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyPair(TriConsumer<KeyType, ValueType1, ValueType2> handler) {
            return action(id -> builder.getMatchKeyFactory().get2(id,
                    keyComparator, extractor1, extractor2, handler));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyValue(BiConsumer<KeyType, Object> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(k, vs.get(0)));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyList(int requiredSize, BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> builder.getMatchKeyFactory().getList(id, requiredSize, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2), handler));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyList(BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuture(id, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2), handler));
        }
    }

    public static class RelayToCollect3<KeyType, ValueType1, ValueType2, ValueType3> extends RelayToCollect<KeyType> {
        protected KeyExtractor<KeyType, ValueType1> extractor1;
        protected KeyExtractor<KeyType, ValueType2> extractor2;
        protected KeyExtractor<KeyType, ValueType3> extractor3;

        public RelayToCollect3(ActorBehaviorBuilderKeyAggregation builder,
                               KeyExtractor<KeyType, ValueType1> extractor1,
                               KeyExtractor<KeyType, ValueType2> extractor2,
                               KeyExtractor<KeyType, ValueType3> extractor3) {
            super(builder);
            this.extractor1 = extractor1;
            this.extractor2 = extractor2;
            this.extractor3 = extractor3;
        }

        public <ValueType4> RelayToCollect4<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> or(
                Class<ValueType4> valueType, Function<ValueType4, KeyType> keyExtractorFromValue) {
            return new RelayToCollect4<>(builder, extractor1, extractor2, extractor3, new KeyExtractorClass<>(valueType, keyExtractorFromValue));
        }


        @Override
        public RelayToCollect3<KeyType, ValueType1, ValueType2, ValueType3> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        public RelayToCollectList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new RelayToCollectList<>(builder, keyComparator, keyValuesReducer,
                    Arrays.asList(extractor1, extractor2, extractor3));
        }

        public RelayToCollectList<KeyType, Object> fold(BiFunction<KeyType, List<Object>, Object> keyValuesReducer) {
            return reduce((k,vs) -> Collections.singletonList(keyValuesReducer.apply(k, vs)));
        }

        public ActorBehaviorBuilderKeyAggregation forEachTriple(TriConsumer<ValueType1, ValueType2, ValueType3> handler) {
            return forEachKeyTriple((k,v1,v2,v3) -> handler.accept(v1,v2,v3));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyTriple(QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> handler) {
            return action(id -> builder.getMatchKeyFactory().get3(id,
                    keyComparator, extractor1, extractor2, extractor3, handler));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyValue(BiConsumer<KeyType, Object> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(k, vs.get(0)));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyList(int requiredSize, BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> builder.getMatchKeyFactory().getList(id, requiredSize, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2, extractor3), handler));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyList(BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuture(id, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2, extractor3), handler));
        }
    }

    public static class RelayToCollect4<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> extends RelayToCollect<KeyType> {
        protected KeyExtractor<KeyType, ValueType1> extractor1;
        protected KeyExtractor<KeyType, ValueType2> extractor2;
        protected KeyExtractor<KeyType, ValueType3> extractor3;
        protected KeyExtractor<KeyType, ValueType4> extractor4;

        public RelayToCollect4(ActorBehaviorBuilderKeyAggregation builder,
                               KeyExtractor<KeyType, ValueType1> extractor1,
                               KeyExtractor<KeyType, ValueType2> extractor2,
                               KeyExtractor<KeyType, ValueType3> extractor3,
                               KeyExtractor<KeyType, ValueType4> extractor4) {
            super(builder);
            this.extractor1 = extractor1;
            this.extractor2 = extractor2;
            this.extractor3 = extractor3;
            this.extractor4 = extractor4;
        }

        public <ValueType5> RelayToCollectList<KeyType, Object> or(
                Class<ValueType5> valueType, Function<ValueType5, KeyType> keyExtractorFromValue) {
            return new RelayToCollectList<>(builder,
                    Arrays.asList(extractor1, extractor2, extractor3, extractor4, new KeyExtractorClass<>(valueType, keyExtractorFromValue)));
        }

        @Override
        public RelayToCollect4<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> sort(KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        public RelayToCollectList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new RelayToCollectList<>(builder, keyComparator, keyValuesReducer,
                    Arrays.asList(extractor1, extractor2, extractor3, extractor4));
        }

        public RelayToCollectList<KeyType, Object> fold(BiFunction<KeyType, List<Object>, Object> keyValuesReducer) {
            return reduce((k,vs) -> Collections.singletonList(keyValuesReducer.apply(k, vs)));
        }

        public ActorBehaviorBuilderKeyAggregation forEachQuad(QuadConsumer<ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            return forEachKeyQuad((k,v1,v2,v3,v4) -> handler.accept(v1,v2,v3,v4));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyQuad(QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            return action(id -> builder.getMatchKeyFactory().get4(id,
                            keyComparator, extractor1, extractor2, extractor3, extractor4, handler));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyValue(BiConsumer<KeyType, Object> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(k, vs.get(0)));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyList(int requiredSize, BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> builder.getMatchKeyFactory().getList(id, requiredSize, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2, extractor3, extractor4), handler));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyList(BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuture(id, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2, extractor3, extractor4), handler));
        }
    }

    public static class RelayToCollectList<KeyType, ValueType> extends RelayToCollect<KeyType> {
        protected List<KeyExtractor<KeyType,?>> extractors;
        protected List<BiFunction<KeyType, List<ValueType>, Iterable<ValueType>>> keyValuesReducers;

        public RelayToCollectList(ActorBehaviorBuilderKeyAggregation builder,
                                  List<KeyExtractor<KeyType, ?>> extractors) {
            super(builder);
            this.extractors = new ArrayList<>(extractors);
            this.keyValuesReducers = new ArrayList<>(3);
        }

        public RelayToCollectList(ActorBehaviorBuilderKeyAggregation builder,
                                  KeyComparator<KeyType> keyComparator,
                                  BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                  List<KeyExtractor<KeyType, ?>> extractors) {
            super(builder, keyComparator);
            this.extractors = new ArrayList<>(extractors);
            this.keyValuesReducers = new ArrayList<>(3);
            keyValuesReducers.add(keyValuesReducer);
        }

        public RelayToCollectList(ActorBehaviorBuilderKeyAggregation builder,
                                  KeyComparator<KeyType> keyComparator,
                                  List<BiFunction<KeyType, List<ValueType>, Iterable<ValueType>>> keyValuesReducers,
                                  List<KeyExtractor<KeyType, ?>> extractors) {
            super(builder, keyComparator);
            this.extractors = extractors;
            this.keyValuesReducers = keyValuesReducers;
        }

        public RelayToCollectList<KeyType,ValueType> or(
                Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
            extractors.add(new KeyExtractorClass<>(valueType, keyExtractorFromValue));
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

        public ActorBehaviorBuilderKeyAggregation forEachKeyValue(BiConsumer<KeyType, ValueType> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(k, vs.get(0)));
        }

        public ActorBehaviorBuilderKeyAggregation forEach(Consumer<ValueType> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(vs.get(0)));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyList(int requiredSize, BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> builder.getMatchKeyFactory().getList(id, requiredSize, keyComparator,
                    new KeyExtractorList<>(extractors), handler)
                    .withKeyValuesReducers(keyValuesReducers));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyList(BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuture(id, keyComparator,
                    new KeyValuesReducerList<>(keyValuesReducers),
                    new KeyExtractorList<>(extractors), handler));
        }

        public RelayToCollectListPhase<KeyType, ValueType> eventually() {
            return new RelayToCollectListPhase<>(builder, keyComparator, keyValuesReducers, extractors);
        }
    }

    public static class RelayToCollectListPhase<KeyType, ValueType> extends RelayToCollectList<KeyType, ValueType> {
        public RelayToCollectListPhase(ActorBehaviorBuilderKeyAggregation builder, List<KeyExtractor<KeyType, ?>> keyExtractors) {
            super(builder, keyExtractors);
        }

        public RelayToCollectListPhase(ActorBehaviorBuilderKeyAggregation builder, KeyComparator<KeyType> keyComparator,
                                       BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                       List<KeyExtractor<KeyType, ?>> keyExtractors) {
            super(builder, keyComparator, keyValuesReducer, keyExtractors);
        }

        public RelayToCollectListPhase(ActorBehaviorBuilderKeyAggregation builder, KeyComparator<KeyType> keyComparator,
                                       List<BiFunction<KeyType, List<ValueType>, Iterable<ValueType>>> keyValuesReducers,
                                       List<KeyExtractor<KeyType, ?>> extractors) {
            super(builder, keyComparator, keyValuesReducers, extractors);
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyList(int requiredSize, BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuturePhase(id, requiredSize, keyComparator,
                    new KeyValuesReducerList<>(keyValuesReducers),
                    new KeyExtractorList<>(extractors),
                    handler));
        }

        public ActorBehaviorBuilderKeyAggregation forEachKeyList(BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> builder.getMatchKeyFactory().getListFuturePhase(id, 1, keyComparator,
                    new KeyValuesReducerList<>(keyValuesReducers),
                    new KeyExtractorList<>(extractors), handler));
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
                return ActorBehaviorKeyAggregation.centerPointPrimitive(leftEnd, rightStart);
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
        public <KeyType, ValueType1> ActorBehavior get1(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                                  ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1,
                                                  BiConsumer<KeyType, ValueType1> handler) {
            return new ActorBehaviorKeyAggregation.ActorBehaviorMatchKey1<>(matchKeyEntryId, keyComparator,
                    keyExtractorFromValue1, handler);
        }

        public <KeyType, ValueType1, ValueType2> ActorBehavior get2(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                                ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1,
                                                ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2,
                                                TriConsumer<KeyType, ValueType1, ValueType2> handler) {
            return new ActorBehaviorKeyAggregation.ActorBehaviorMatchKey2<>(matchKeyEntryId, keyComparator,
                    keyExtractorFromValue1, keyExtractorFromValue2, handler);
        }

        public <KeyType, ValueType1, ValueType2, ValueType3> ActorBehavior get3(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                                ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1,
                                                ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2,
                                                ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType3> keyExtractorFromValue3,
                                                QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> handler) {
            return new ActorBehaviorKeyAggregation.ActorBehaviorMatchKey3<>(matchKeyEntryId, keyComparator,
                    keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3, handler);
        }

        public <KeyType, ValueType1, ValueType2, ValueType3, ValueType4> ActorBehavior get4(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                                ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1,
                                                ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2,
                                                ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType3> keyExtractorFromValue3,
                                                ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType4> keyExtractorFromValue4,
                                                QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            return new ActorBehaviorKeyAggregation.ActorBehaviorMatchKey4<>(matchKeyEntryId, keyComparator,
                    keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3, keyExtractorFromValue4, handler);
        }

        public <KeyType, ValueType> ActorBehaviorMatchKeyList<KeyType, ValueType> getList(int matchKeyEntryId, int threshold, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                               ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType> keyExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            return new ActorBehaviorMatchKeyList<>(matchKeyEntryId, threshold,
                    keyComparator, keyExtractorFromValue, handler);
        }

        public <KeyType, ValueType> ActorBehaviorMatchKeyListFuture<KeyType,ValueType> getListFuture(int matchKeyEntryId,
                                               KeyHistograms.KeyComparator<KeyType> keyComparator,
                                               ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType> keyExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            return getListFuture(matchKeyEntryId, 1,
                    keyComparator, (k,vs) -> vs, keyExtractorFromValue, handler);
        }


        public <KeyType, ValueType> ActorBehaviorMatchKeyListFuture<KeyType, ValueType> getListFuture(int matchKeyEntryId,
                                               KeyHistograms.KeyComparator<KeyType> keyComparator,
                                               BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                               ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType> keyExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            return getListFuture(matchKeyEntryId, 1, keyComparator,
                    keyValuesReducer, keyExtractorFromValue, handler);
        }


        public <KeyType, ValueType> ActorBehaviorMatchKeyListFuture<KeyType,ValueType> getListFuture(int matchKeyEntryId, int requiredSize,
                                              KeyHistograms.KeyComparator<KeyType> keyComparator,
                                              BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                              ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType> keyExtractorFromValue,
                                              BiConsumer<KeyType, List<ValueType>> handler) {
            return new ActorBehaviorMatchKeyListFuture<>(matchKeyEntryId, requiredSize,
                    keyComparator, keyValuesReducer, keyExtractorFromValue, handler);
        }


        public <KeyType, ValueType> ActorBehaviorMatchKeyListFuturePhase<KeyType,ValueType> getListFuturePhase(int matchKeyEntryId, int requiredSize,
                                               KeyHistograms.KeyComparator<KeyType> keyComparator,
                                               BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                               ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType> keyExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            return new ActorBehaviorMatchKeyListFuturePhase<>(matchKeyEntryId, requiredSize,
                    keyComparator, keyValuesReducer, keyExtractorFromValue, handler);
        }

    }


}