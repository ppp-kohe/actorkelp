package csl.actor.msgassoc;

import csl.actor.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ActorBehaviorBuilderKeyValue extends ActorBehaviorBuilder {
    protected BiConsumer<List<Supplier<KeyHistograms.Histogram>>, MailboxReplicable.HistogramSelector> histogramFactoriesTarget;
    protected List<Supplier<KeyHistograms.Histogram>> histogramFactories = new ArrayList<>();
    protected List<MailboxReplicable.HistogramSelector> histogramSelectors = new ArrayList<>();

    public ActorBehaviorBuilderKeyValue(
            BiConsumer<List<Supplier<KeyHistograms.Histogram>>,
                       MailboxReplicable.HistogramSelector> histogramFactoriesTarget) {
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
        histogramFactoriesTarget.accept(
                getHistogramFactories(),
                new HistogramSelectorList(histogramSelectors));
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

    public ActorBehaviorBuilderKeyValue addMatchKeySelector(MailboxReplicable.HistogramSelector histogramSelector) {
        histogramSelectors.add(histogramSelector);
        return this;
    }

    public static class RelayToCollect<KeyType> {
        protected ActorBehaviorBuilderKeyValue builder;
        protected List<KeyExtractor<KeyType, ?>> messages;
        protected Supplier<KeyHistograms.Histogram> histogramFactory;

        public RelayToCollect(ActorBehaviorBuilderKeyValue builder) {
            this.builder = builder;
            this.messages = new ArrayList<>();
            this.histogramFactory = KeyHistograms.HistogramNonComparable::new;
        }

        public <ValueType> RelayToCollect<KeyType> or(
                Class<ValueType> valueType,
                Function<ValueType, KeyType> keyExtractorFromValue) {
            messages.add(new KeyExtractorClass<>(valueType, keyExtractorFromValue));
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

    public static class RelayToBehavior<KeyType> {
        protected ActorBehaviorBuilderKeyValue builder;
        protected List<KeyExtractor<KeyType, ?>> messages;
        protected int machKeyEntryId;

        public RelayToBehavior(ActorBehaviorBuilderKeyValue builder, List<KeyExtractor<KeyType, ?>> messages,
                               Supplier<KeyHistograms.Histogram> histogramFactory) {
            this.builder = builder;
            this.messages = messages;
            this.machKeyEntryId = builder.nextMatchKeyEntry(histogramFactory); //determines the entry id here
        }

        @SuppressWarnings("unchecked")
        public <ValueType1, ValueType2> ActorBehaviorBuilderKeyValue forEachPair(
                BiConsumer<ValueType1, ValueType2> handler) {
            KeyExtractor<KeyType, ValueType1> m1 = (KeyExtractor<KeyType, ValueType1>) messages.get(0);
            KeyExtractor<KeyType, ValueType2> m2 = (KeyExtractor<KeyType, ValueType2>) messages.get(1);
            //TODO check size of messages
            return builder.with(new ActorBehaviorMatchKeyTwo<>(machKeyEntryId, m1, m2, handler))
                        .addMatchKeySelector(new HistogramSelectorList(machKeyEntryId, m1, m2));
        }

        @SuppressWarnings("unchecked")
        public <ValueType> ActorBehaviorBuilderKeyValue forEachList(
                int threshold, BiConsumer<KeyType, List<ValueType>> handler) {
            KeyExtractor<KeyType, ValueType> m = (KeyExtractor<KeyType, ValueType>) messages.get(0);
            //TODO check size of messages
            return builder.with(new ActorBehaviorMatchKeyList<>(machKeyEntryId, threshold, m, handler))
                    .addMatchKeySelector(new HistogramSelectorDefault(machKeyEntryId, m));
        }
    }

    public static class HistogramSelectorDefault implements MailboxReplicable.HistogramSelector {
        protected int matchKeyEntryId;
        protected KeyExtractor<?,?> keyExtractor;

        public HistogramSelectorDefault(int matchKeyEntryId, KeyExtractor<?, ?> keyExtractor) {
            this.matchKeyEntryId = matchKeyEntryId;
            this.keyExtractor = keyExtractor;
        }

        public boolean match(Object value) {
            return keyExtractor.matchValue(value);
        }

        @Override
        public int select(Object value) {
            if (match(value)) {
                return matchKeyEntryId;
            } else {
                return -1;
            }
        }
    }

    public static class HistogramSelectorList implements MailboxReplicable.HistogramSelector {
        protected List<MailboxReplicable.HistogramSelector> selectors;

        public HistogramSelectorList(List<MailboxReplicable.HistogramSelector> selectors) {
            this.selectors = selectors;
        }

        public HistogramSelectorList(int matchKeyEntryId, KeyExtractor<?,?>... keyExtractors) {
            selectors = Arrays.stream(keyExtractors)
                    .map(ke -> new HistogramSelectorDefault(matchKeyEntryId, ke))
                    .collect(Collectors.toList());
        }

        @Override
        public int select(Object value) {
            for (MailboxReplicable.HistogramSelector s : selectors) {
                int m = s.select(s);
                if (m != -1) {
                    return m;
                }
            }
            return -1;
        }
    }

    public static abstract class ActorBehaviorMatchKey<KeyType> implements ActorBehavior {
        protected int matchKeyEntryId;

        public ActorBehaviorMatchKey(int matchKeyEntryId) {
            this.matchKeyEntryId = matchKeyEntryId;
        }

        protected void put(Actor self, KeyType key, Object value) {
            MailboxReplicable m = ((ActorReplicable) self).getMailboxAsReplicable();
            m.putMessageTable(matchKeyEntryId, key, value);
            m.addActiveAssociation(this);
        }

        public abstract boolean processTable(MailboxReplicable m);
    }

    public static class ActorBehaviorMatchKeyTwo<KeyType, ValueType1, ValueType2> extends ActorBehaviorMatchKey<KeyType>
                    implements MailboxReplicable.ValueInTableMatcher {
        protected KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1;
        protected KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2;

        protected BiConsumer<ValueType1, ValueType2> handler;

        public ActorBehaviorMatchKeyTwo(int matchKeyEntryId,
                                        KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1,
                                        KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2,
                                        BiConsumer<ValueType1, ValueType2> handler) {
            super(matchKeyEntryId);
            this.keyExtractorFromValue1 = keyExtractorFromValue1;
            this.keyExtractorFromValue2 = keyExtractorFromValue2;
            this.handler = handler;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object value = message.getData();
            KeyType key;
            if (keyExtractorFromValue1.matchValue(value)) {
                key = keyExtractorFromValue1.toKey((ValueType1) value);
            } else if (keyExtractorFromValue2.matchValue(value)) {
                key = keyExtractorFromValue2.toKey((ValueType2) value);
            } else {
                return false;
            }

            put(self, key, value);
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean processTable(MailboxReplicable m) {
            Object[] values = m.takeFromTable(matchKeyEntryId, this);
            if (values != null) {
                handler.accept((ValueType1) values[0], (ValueType2) values[1]);
                return true;
            }
            return false;
        }

        @Override
        public int valueSizeInTable() {
            return 2;
        }

        @Override
        public boolean matchValueInTable(int index, Object value) {
            if (index == 0) {
                return keyExtractorFromValue1.matchValue(value);
            } else if (index == 1) {
                return keyExtractorFromValue2.matchValue(value);
            } else {
                return false;
            }
        }
    }

    public static class ActorBehaviorMatchKeyList<KeyType, ValueType> extends ActorBehaviorMatchKey<KeyType>
            implements MailboxReplicable.ValueInTableMatcher {
        protected int threshold = 3;
        protected KeyExtractor<KeyType, ValueType> keyExtractorFromValue;
        protected BiConsumer<KeyType, List<ValueType>> handler;

        public ActorBehaviorMatchKeyList(int matchKeyEntryId, int threshold,
                                         KeyExtractor<KeyType, ValueType> keyExtractorFromValue,
                                         BiConsumer<KeyType, List<ValueType>> handler) {
            super(matchKeyEntryId);
            this.threshold = threshold;
            this.keyExtractorFromValue = keyExtractorFromValue;
            this.handler = handler;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object value = message.getData();
            KeyType key;
            if (keyExtractorFromValue.matchValue(value)) {
                key = keyExtractorFromValue.toKey((ValueType) value);
            } else {
                return false;
            }
            put(self, key, value);
            return true;
        }

        @Override
        public boolean processTable(MailboxReplicable m) {
            Object[] values = m.takeFromTable(matchKeyEntryId, this);
            //TODO custom extraction with key
            return false;
        }

        @Override
        public int valueSizeInTable() {
            return threshold;
        }

        @Override
        public boolean matchValueInTable(int index, Object value) {
            return keyExtractorFromValue.matchValue(value);
        }

    }
}
