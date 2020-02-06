package csl.actor.msgassoc;

import csl.actor.ActorBehavior;
import csl.actor.ActorBehaviorBuilder;
import csl.actor.ActorRef;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ActorBehaviorBuilderKeyValue extends ActorBehaviorBuilder {
    protected Consumer<List<MailboxAggregation.HistogramProcessor>> histogramProcessorsTarget;
    protected Map<Integer, MailboxAggregation.HistogramProcessor> processors = new HashMap<>();


    public ActorBehaviorBuilderKeyValue(
            Consumer<List<MailboxAggregation.HistogramProcessor>> histogramProcessorsTarget) {
        this.histogramProcessorsTarget = histogramProcessorsTarget;
    }

    @Override
    public <DataType> ActorBehaviorBuilderKeyValue match(Class<DataType> dataType, Consumer<DataType> handler) {
        super.match(dataType, handler);
        return this;
    }

    @Override
    public <DataType> ActorBehaviorBuilderKeyValue matchWithSender(Class<DataType> dataType, BiConsumer<DataType, ActorRef> handler) {
        super.matchWithSender(dataType, handler);
        return this;
    }

    @Override
    public ActorBehaviorBuilderKeyValue matchAny(BiConsumer<Object, ActorRef> handler) {
        super.matchAny(handler);
        return this;
    }

    @Override
    public ActorBehaviorBuilderKeyValue with(ActorBehavior behavior) {
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
                .sort(new ActorBehaviorAggregation.KeyComparatorOrdered<>());
    }

    public int nextMatchKeyEntry() {
        int id = processors.size();
        processors.put(id, null); //null value for preserving the id
        return id;
    }

    public void setProcessor(int matchKeyEntryId, MailboxAggregation.HistogramProcessor processor) {
        processors.put(matchKeyEntryId, processor);
    }

    public ActorBehaviorBuilderKeyValue withProcessor(int matchKeyEntryId, ActorBehavior behavior) {
        if (behavior instanceof MailboxAggregation.HistogramProcessor) {
            setProcessor(matchKeyEntryId, (MailboxAggregation.HistogramProcessor) behavior);
        }
        return with(behavior);
    }

    public static class RelayToCollect<KeyType> {
        protected ActorBehaviorBuilderKeyValue builder;
        protected KeyHistograms.KeyComparator<KeyType> keyComparator;

        public RelayToCollect(ActorBehaviorBuilderKeyValue builder) {
            this(builder, new KeyComparatorDefault<>());
        }

        public RelayToCollect(ActorBehaviorBuilderKeyValue builder, KeyHistograms.KeyComparator<KeyType> keyComparator) {
            this.builder = builder;
            this.keyComparator = keyComparator;
        }

        public RelayToCollect<KeyType> sort(KeyHistograms.KeyComparator<KeyType> keyComparator) {
            this.keyComparator = keyComparator;
            return this;
        }

        protected ActorBehaviorBuilderKeyValue action(Function<Integer, ActorBehavior> behaviorFactory) {
            int id = builder.nextMatchKeyEntry();
            return builder.withProcessor(id, behaviorFactory.apply(id));
        }

    }

    public static class RelayToCollect1<KeyType, ValueType> extends RelayToCollect<KeyType> {
        protected KeyExtractor<KeyType, ValueType> extractor1;
        public RelayToCollect1(ActorBehaviorBuilderKeyValue builder, KeyExtractor<KeyType, ValueType> extractor1) {
            super(builder);
            this.extractor1 = extractor1;
        }

        public <ValueType2> RelayToCollect2<KeyType, ValueType, ValueType2> or(
                Class<ValueType2> valueType, Function<ValueType2, KeyType> keyExtractorFromValue) {
            return new RelayToCollect2<>(builder, extractor1, new KeyExtractorClass<>(valueType, keyExtractorFromValue));
        }

        @Override
        public RelayToCollect1<KeyType, ValueType> sort(KeyHistograms.KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }


        public RelayToCollectList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new RelayToCollectList<>(builder, keyComparator, keyValuesReducer,
                    Collections.singletonList(extractor1));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyValue(BiConsumer<KeyType, ValueType> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKey1<>(id,
                    keyComparator, extractor1, handler));
        }

        public ActorBehaviorBuilderKeyValue forEach(Consumer<ValueType> handler) {
            return forEachKeyValue((k,v) -> handler.accept(v));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyList(int requiredSize, BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKeyList<>(id, requiredSize, keyComparator,
                    extractor1, handler));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyList(BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKeyListFuture<>(id, keyComparator,
                    extractor1, handler));
        }
    }

    public static class RelayToCollect2<KeyType, ValueType1, ValueType2> extends RelayToCollect<KeyType> {
        protected KeyExtractor<KeyType, ValueType1> extractor1;
        protected KeyExtractor<KeyType, ValueType2> extractor2;

        public RelayToCollect2(ActorBehaviorBuilderKeyValue builder,
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
        public RelayToCollect2<KeyType, ValueType1, ValueType2> sort(KeyHistograms.KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        public RelayToCollectList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new RelayToCollectList<>(builder, keyComparator, keyValuesReducer,
                    Arrays.asList(extractor1, extractor2));
        }

        public ActorBehaviorBuilderKeyValue forEachPair(BiConsumer<ValueType1, ValueType2> handler) {
            return forEachKeyPair((k,v1,v2) -> handler.accept(v1,v2));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyPair(TriConsumer<KeyType, ValueType1, ValueType2> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKey2<>(id,
                    keyComparator, extractor1, extractor2, handler));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyValue(BiConsumer<KeyType, Object> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(k, vs.get(0)));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyList(int requiredSize, BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKeyList<>(id, requiredSize, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2), handler));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyList(BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKeyListFuture<>(id, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2), handler));
        }
    }

    public static class RelayToCollect3<KeyType, ValueType1, ValueType2, ValueType3> extends RelayToCollect<KeyType> {
        protected KeyExtractor<KeyType, ValueType1> extractor1;
        protected KeyExtractor<KeyType, ValueType2> extractor2;
        protected KeyExtractor<KeyType, ValueType3> extractor3;

        public RelayToCollect3(ActorBehaviorBuilderKeyValue builder,
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
        public RelayToCollect3<KeyType, ValueType1, ValueType2, ValueType3> sort(KeyHistograms.KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        public RelayToCollectList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new RelayToCollectList<>(builder, keyComparator, keyValuesReducer,
                    Arrays.asList(extractor1, extractor2, extractor3));
        }

        public ActorBehaviorBuilderKeyValue forEachTriple(TriConsumer<ValueType1, ValueType2, ValueType3> handler) {
            return forEachKeyTriple((k,v1,v2,v3) -> handler.accept(v1,v2,v3));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyTriple(QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKey3<>(id,
                    keyComparator, extractor1, extractor2, extractor3, handler));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyValue(BiConsumer<KeyType, Object> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(k, vs.get(0)));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyList(int requiredSize, BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKeyList<>(id, requiredSize, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2, extractor3), handler));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyList(BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKeyListFuture<>(id, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2, extractor3), handler));
        }
    }

    public static class RelayToCollect4<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> extends RelayToCollect<KeyType> {
        protected KeyExtractor<KeyType, ValueType1> extractor1;
        protected KeyExtractor<KeyType, ValueType2> extractor2;
        protected KeyExtractor<KeyType, ValueType3> extractor3;
        protected KeyExtractor<KeyType, ValueType4> extractor4;

        public RelayToCollect4(ActorBehaviorBuilderKeyValue builder,
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
        public RelayToCollect4<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> sort(KeyHistograms.KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        public RelayToCollectList<KeyType, Object> reduce(BiFunction<KeyType, List<Object>, Iterable<Object>> keyValuesReducer) {
            return new RelayToCollectList<>(builder, keyComparator, keyValuesReducer,
                    Arrays.asList(extractor1, extractor2, extractor3, extractor4));
        }

        public ActorBehaviorBuilderKeyValue forEachQuad(QuadConsumer<ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            return forEachKeyQuad((k,v1,v2,v3,v4) -> handler.accept(v1,v2,v3,v4));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyQuad(QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKey4<>(id,
                            keyComparator, extractor1, extractor2, extractor3, extractor4, handler));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyValue(BiConsumer<KeyType, Object> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(k, vs.get(0)));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyList(int requiredSize, BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKeyList<>(id, requiredSize, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2, extractor3, extractor4), handler));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyList(BiConsumer<KeyType, List<Object>> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKeyListFuture<>(id, keyComparator,
                    new KeyExtractorList<>(extractor1, extractor2, extractor3, extractor4), handler));
        }
    }

    public static class RelayToCollectList<KeyType, ValueType> extends RelayToCollect<KeyType> {
        protected List<KeyExtractor<KeyType,?>> extractors;
        protected List<BiFunction<KeyType, List<ValueType>, Iterable<ValueType>>> keyValuesReducers;

        public RelayToCollectList(ActorBehaviorBuilderKeyValue builder,
                               List<KeyExtractor<KeyType, ?>> extractors) {
            super(builder);
            this.extractors = new ArrayList<>(extractors);
        }

        public RelayToCollectList(ActorBehaviorBuilderKeyValue builder,
                                  KeyHistograms.KeyComparator<KeyType> keyComparator,
                                  BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                  List<KeyExtractor<KeyType, ?>> extractors) {
            super(builder, keyComparator);
            this.extractors = new ArrayList<>(extractors);
            this.keyValuesReducers = new ArrayList<>();
            keyValuesReducers.add(keyValuesReducer);
        }

        public RelayToCollectList<KeyType,ValueType> or(
                Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
            extractors.add(new KeyExtractorClass<>(valueType, keyExtractorFromValue));
            return this;
        }

        @Override
        public RelayToCollectList<KeyType, ValueType> sort(KeyHistograms.KeyComparator<KeyType> keyComparator) {
            super.sort(keyComparator);
            return this;
        }

        public RelayToCollectList<KeyType, ValueType> reduce(BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer) {
            keyValuesReducers.add(keyValuesReducer);
            return this;
        }

        public ActorBehaviorBuilderKeyValue forEachKeyValue(BiConsumer<KeyType, ValueType> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(k, vs.get(0)));
        }

        public ActorBehaviorBuilderKeyValue forEach(Consumer<ValueType> handler) {
            return forEachKeyList(1, (k,vs) -> handler.accept(vs.get(0)));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyList(int requiredSize, BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKeyList<>(id, requiredSize, keyComparator,
                    new KeyExtractorList<>(extractors), handler)
                    .withKeyValuesReducers(keyValuesReducers));
        }

        public ActorBehaviorBuilderKeyValue forEachKeyList(BiConsumer<KeyType, List<ValueType>> handler) {
            return action(id -> new ActorBehaviorAggregation.ActorBehaviorMatchKeyListFuture<>(id, keyComparator,
                    new KeyExtractorList<>(extractors), handler)
                    .withKeyValuesReducers(keyValuesReducers));
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
/*
    @Deprecated
    public static class RelayToBehavior<KeyType> {
        protected ActorBehaviorBuilderKeyValue builder;
        protected List<KeyExtractor<KeyType, ?>> messages;
        protected int machKeyEntryId;
        protected KeyHistograms.KeyComparator<KeyType> keyComparator;

        public RelayToBehavior(ActorBehaviorBuilderKeyValue builder, List<KeyExtractor<KeyType, ?>> messages,
                               KeyHistograms.KeyComparator<KeyType> keyComparator) {
            this.builder = builder;
            this.messages = messages;
            this.keyComparator = keyComparator;
            this.machKeyEntryId = builder.nextMatchKeyEntry(); //determines the entry id here
        }

        @SuppressWarnings("unchecked")
        public <ValueType1, ValueType2> ActorBehaviorBuilderKeyValue forEachPair(
                BiConsumer<ValueType1, ValueType2> handler) {
            KeyExtractor<KeyType, ValueType1> m1 = (KeyExtractor<KeyType, ValueType1>) messages.get(0);
            KeyExtractor<KeyType, ValueType2> m2 = (KeyExtractor<KeyType, ValueType2>) messages.get(1);
            //TODO check size of messages
            return builder.withProcessor(machKeyEntryId, new ActorBehaviorAggregation.ActorBehaviorMatchKey2<>(machKeyEntryId, keyComparator, m1, m2, handler));
        }

        @SuppressWarnings("unchecked")
        public <ValueType1, ValueType2, ValueType3> ActorBehaviorBuilderKeyValue forEachTri(
                TriConsumer<ValueType1, ValueType2, ValueType3> handler) {
            KeyExtractor<KeyType, ValueType1> m1 = (KeyExtractor<KeyType, ValueType1>) messages.get(0);
            KeyExtractor<KeyType, ValueType2> m2 = (KeyExtractor<KeyType, ValueType2>) messages.get(1);
            KeyExtractor<KeyType, ValueType3> m3 = (KeyExtractor<KeyType, ValueType3>) messages.get(2);
            //TODO check size of messages
            return builder.withProcessor(machKeyEntryId, new ActorBehaviorAggregation.ActorBehaviorMatchKey3<>(machKeyEntryId, keyComparator, m1, m2, m3, handler));
        }

        @SuppressWarnings("unchecked")
        public <ValueType1, ValueType2, ValueType3, ValueType4> ActorBehaviorBuilderKeyValue forEachQuad(
                QuadConsumer<ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            KeyExtractor<KeyType, ValueType1> m1 = (KeyExtractor<KeyType, ValueType1>) messages.get(0);
            KeyExtractor<KeyType, ValueType2> m2 = (KeyExtractor<KeyType, ValueType2>) messages.get(1);
            KeyExtractor<KeyType, ValueType3> m3 = (KeyExtractor<KeyType, ValueType3>) messages.get(2);
            KeyExtractor<KeyType, ValueType4> m4 = (KeyExtractor<KeyType, ValueType4>) messages.get(3);
            //TODO check size of messages
            return builder.withProcessor(machKeyEntryId, new ActorBehaviorAggregation.ActorBehaviorMatchKey4<>(machKeyEntryId, keyComparator, m1, m2, m3, m4, handler));
        }

        @SuppressWarnings("unchecked")
        public <ValueType> ActorBehaviorBuilderKeyValue forEachList(
                int threshold, BiConsumer<KeyType, List<ValueType>> handler) {
            KeyExtractor<KeyType, ValueType> m = (KeyExtractor<KeyType, ValueType>) messages.get(0);
            //TODO check size of messages
            return builder.withProcessor(machKeyEntryId, new ActorBehaviorAggregation.ActorBehaviorMatchKeyList<>(machKeyEntryId, threshold, keyComparator, m, handler));
        }
    }*/


    public static class KeyComparatorDefault<KeyType> implements KeyHistograms.KeyComparator<KeyType> {
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
                return ActorBehaviorAggregation.centerPointPrimitive(leftEnd, rightStart);
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

}
