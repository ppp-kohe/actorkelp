package csl.actor.msgassoc;

import csl.actor.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class ActorBehaviorBuilderKeyValue extends ActorBehaviorBuilder {
    protected Consumer<List<Supplier<KeyHistograms.Histogram>>> histogramFactoriesTarget;
    protected List<Supplier<KeyHistograms.Histogram>> histogramFactories = new ArrayList<>();

    public ActorBehaviorBuilderKeyValue(Consumer<List<Supplier<KeyHistograms.Histogram>>> histogramFactoriesTarget) {
        this.histogramFactoriesTarget = histogramFactoriesTarget;
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
        histogramFactoriesTarget.accept(getHistogramFactories());
        return b;
    }

    public <ValueType, KeyType> RelayToCollect<KeyType> matchKey(
            Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
        return new RelayToCollect<KeyType>(this).or(valueType, keyExtractorFromValue);
    }

    public <ValueType, KeyType extends Comparable<KeyType>> RelayToCollect<KeyType> matchKeyOrdered(
            Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
        return new RelayToCollect<KeyType>(this).or(valueType, keyExtractorFromValue)
                .withHistogram(KeyHistograms.HistogramComparable::new);
    }

    public int nextMatchKeyEntry(Supplier<KeyHistograms.Histogram> histogramFactory) {
        int id = histogramFactories.size();
        histogramFactories.add(histogramFactory);
        return id;
    }

    public List<Supplier<KeyHistograms.Histogram>> getHistogramFactories() {
        return histogramFactories;
    }

    public static class RelayToCollect<KeyType> {
        protected ActorBehaviorBuilderKeyValue builder;
        protected List<AssociatedMessage<KeyType, ?>> messages;
        protected Supplier<KeyHistograms.Histogram> histogramFactory;

        public RelayToCollect(ActorBehaviorBuilderKeyValue builder) {
            this.builder = builder;
            this.messages = new ArrayList<>();
            this.histogramFactory = KeyHistograms.HistogramNonComparable::new;
        }

        public <ValueType> RelayToCollect<KeyType> or(
                Class<ValueType> valueType,
                Function<ValueType, KeyType> keyExtractorFromValue) {
            messages.add(new AssociatedMessage<>(valueType, keyExtractorFromValue));
            return this;
        }

        public RelayToCollect<KeyType> withHistogram(Supplier<KeyHistograms.Histogram> histogramFactory) {
            this.histogramFactory = histogramFactory;
            return this;
        }

        public RelayToBehavior<KeyType> collect() {
            return new RelayToBehavior<>(builder, messages, histogramFactory);
        }
    }

    public static class AssociatedMessage<KeyType, ValueType> {
        protected Class<ValueType> valueType;
        protected Function<ValueType, KeyType> keyExtractorFromValue;

        public AssociatedMessage(Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
            this.valueType = valueType;
            this.keyExtractorFromValue = keyExtractorFromValue;
        }

        public Class<ValueType> getValueType() {
            return valueType;
        }

        public Function<ValueType, KeyType> getKeyExtractorFromValue() {
            return keyExtractorFromValue;
        }
    }

    public static class RelayToBehavior<KeyType> {
        protected ActorBehaviorBuilderKeyValue builder;
        protected List<AssociatedMessage<KeyType, ?>> messages;
        protected int machKeyEntryId;

        public RelayToBehavior(ActorBehaviorBuilderKeyValue builder, List<AssociatedMessage<KeyType, ?>> messages,
                               Supplier<KeyHistograms.Histogram> histogramFactory) {
            this.builder = builder;
            this.messages = messages;
            this.machKeyEntryId = builder.nextMatchKeyEntry(histogramFactory); //determines the entry id here
        }

        @SuppressWarnings("unchecked")
        public <ValueType1, ValueType2> ActorBehaviorBuilderKeyValue forEachPair(
                BiConsumer<ValueType1, ValueType2> handler) {
            AssociatedMessage<KeyType, ValueType1> m1 = (AssociatedMessage<KeyType, ValueType1>) messages.get(0);
            AssociatedMessage<KeyType, ValueType2> m2 = (AssociatedMessage<KeyType, ValueType2>) messages.get(1);
            //TODO check size of messages
            return builder.with(new ActorBehaviorMatchKeyTwo<>(machKeyEntryId,
                    m1.getValueType(), m2.getValueType(),
                    m1.getKeyExtractorFromValue(), m2.getKeyExtractorFromValue(),
                    handler));
        }

        @SuppressWarnings("unchecked")
        public <ValueType> ActorBehaviorBuilderKeyValue forEachList(
                int threshold, BiConsumer<KeyType, List<ValueType>> handler) {
            AssociatedMessage<KeyType, ValueType> m = (AssociatedMessage<KeyType, ValueType>) messages.get(0);
            //TODO check size of messages
            return builder.with(new ActorBehaviorMatchKeyList<>(machKeyEntryId,
                    threshold, m.getValueType(), m.getKeyExtractorFromValue(), handler));
        }
    }

    public static abstract class ActorBehaviorMatchKey<KeyType> implements ActorBehavior {
        protected int matchKeyEntryId;

        public ActorBehaviorMatchKey(int matchKeyEntryId) {
            this.matchKeyEntryId = matchKeyEntryId;
        }
    }

    public static class ActorBehaviorMatchKeyTwo<KeyType, ValueType1, ValueType2> extends ActorBehaviorMatchKey<KeyType> {
        protected int matchKeyEntryId;
        protected Class<ValueType1> valueType1;
        protected Class<ValueType2> valueType2;
        protected Function<ValueType1, KeyType> keyExtractorFromValue1;
        protected Function<ValueType2, KeyType> keyExtractorFromValue2;

        protected BiConsumer<ValueType1, ValueType2> handler;

        public ActorBehaviorMatchKeyTwo(int matchKeyEntryId,
                                        Class<ValueType1> valueType1,
                                        Class<ValueType2> valueType2,
                                        Function<ValueType1, KeyType> keyExtractorFromValue1,
                                        Function<ValueType2, KeyType> keyExtractorFromValue2,
                                        BiConsumer<ValueType1, ValueType2> handler) {
            super(matchKeyEntryId);
            this.matchKeyEntryId = matchKeyEntryId;
            this.valueType1 = valueType1;
            this.valueType2 = valueType2;
            this.keyExtractorFromValue1 = keyExtractorFromValue1;
            this.keyExtractorFromValue2 = keyExtractorFromValue2;
            this.handler = handler;
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            //TODO
            return false;
        }
    }

    public static class ActorBehaviorMatchKeyList<KeyType, ValueType> extends ActorBehaviorMatchKey<KeyType> {
        protected int threshold = 3;
        protected Class<ValueType> type;
        protected Function<ValueType, KeyType> keyExtractorFromValue;
        protected BiConsumer<KeyType, List<ValueType>> handler;

        public ActorBehaviorMatchKeyList(int matchKeyEntryId, int threshold,
                                         Class<ValueType> type, Function<ValueType, KeyType> keyExtractorFromValue,
                                         BiConsumer<KeyType, List<ValueType>> handler) {
            super(matchKeyEntryId);
            this.threshold = threshold;
            this.type = type;
            this.keyExtractorFromValue = keyExtractorFromValue;
            this.handler = handler;
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            //TODO
            return false;
        }
    }
}
