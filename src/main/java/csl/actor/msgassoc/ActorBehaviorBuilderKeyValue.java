package csl.actor.msgassoc;

import csl.actor.*;

import java.util.*;
import java.util.function.BiConsumer;
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

    public <ValueType, KeyType> RelayToCollect<KeyType> matchKey(
            Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
        return new RelayToCollect<KeyType>(this).or(valueType, keyExtractorFromValue);
    }

    public <ValueType, KeyType extends Comparable<KeyType>> RelayToCollect<KeyType> matchKeyOrdered(
            Class<ValueType> valueType, Function<ValueType, KeyType> keyExtractorFromValue) {
        return new RelayToCollectOrdered<KeyType>(this).or(valueType, keyExtractorFromValue);
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
        protected List<KeyExtractor<KeyType, ?>> messages;

        public RelayToCollect(ActorBehaviorBuilderKeyValue builder) {
            this.builder = builder;
            this.messages = new ArrayList<>();
        }

        public <ValueType> RelayToCollect<KeyType> or(
                Class<ValueType> valueType,
                Function<ValueType, KeyType> keyExtractorFromValue) {
            messages.add(new KeyExtractorClass<>(valueType, keyExtractorFromValue));
            return this;
        }

        public RelayToBehavior<KeyType> collect() {
            return collect(new KeyComparatorDefault<>());
        }

        public RelayToBehavior<KeyType> collect(KeyHistograms.KeyComparator<KeyType> keyComparator) {
            return new RelayToBehavior<>(builder, messages, keyComparator);
        }
    }

    public static class RelayToCollectOrdered<KeyType extends Comparable<KeyType>> extends RelayToCollect<KeyType> {
        public RelayToCollectOrdered(ActorBehaviorBuilderKeyValue builder) {
            super(builder);
        }

        @Override
        public RelayToBehavior<KeyType> collect() {
            return super.collect(new KeyComparatorOrdered<>());
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
            return builder.withProcessor(machKeyEntryId, new ActorBehaviorMatchKeyTwo<>(machKeyEntryId, keyComparator, m1, m2, handler));
        }

        @SuppressWarnings("unchecked")
        public <ValueType1, ValueType2, ValueType3> ActorBehaviorBuilderKeyValue forEachTri(
                TriConsumer<ValueType1, ValueType2, ValueType3> handler) {
            KeyExtractor<KeyType, ValueType1> m1 = (KeyExtractor<KeyType, ValueType1>) messages.get(0);
            KeyExtractor<KeyType, ValueType2> m2 = (KeyExtractor<KeyType, ValueType2>) messages.get(1);
            KeyExtractor<KeyType, ValueType3> m3 = (KeyExtractor<KeyType, ValueType3>) messages.get(2);
            //TODO check size of messages
            return builder.withProcessor(machKeyEntryId, new ActorBehaviorMatchKeyThree<>(machKeyEntryId, keyComparator, m1, m2, m3, handler));
        }

        @SuppressWarnings("unchecked")
        public <ValueType1, ValueType2, ValueType3, ValueType4> ActorBehaviorBuilderKeyValue forEachQuad(
                QuadConsumer<ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            KeyExtractor<KeyType, ValueType1> m1 = (KeyExtractor<KeyType, ValueType1>) messages.get(0);
            KeyExtractor<KeyType, ValueType2> m2 = (KeyExtractor<KeyType, ValueType2>) messages.get(1);
            KeyExtractor<KeyType, ValueType3> m3 = (KeyExtractor<KeyType, ValueType3>) messages.get(2);
            KeyExtractor<KeyType, ValueType4> m4 = (KeyExtractor<KeyType, ValueType4>) messages.get(3);
            //TODO check size of messages
            return builder.withProcessor(machKeyEntryId, new ActorBehaviorMatchKeyFour<>(machKeyEntryId, keyComparator, m1, m2, m3, m4, handler));
        }

        @SuppressWarnings("unchecked")
        public <ValueType> ActorBehaviorBuilderKeyValue forEachList(
                int threshold, BiConsumer<KeyType, List<ValueType>> handler) {
            KeyExtractor<KeyType, ValueType> m = (KeyExtractor<KeyType, ValueType>) messages.get(0);
            //TODO check size of messages
            return builder.withProcessor(machKeyEntryId, new ActorBehaviorMatchKeyList<>(machKeyEntryId, threshold, keyComparator, m, handler));
        }
    }


    public static abstract class ActorBehaviorMatchKey<KeyType> extends KeyHistograms.HistogramPutContext
            implements ActorBehavior, MailboxAggregation.HistogramProcessor {
        protected int matchKeyEntryId;
        protected KeyHistograms.KeyComparator<KeyType> keyComparator;

        public ActorBehaviorMatchKey(int matchKeyEntryId, int requiredSize, KeyHistograms.KeyComparator<KeyType> keyComparator) {
            this.putRequiredSize = requiredSize;
            this.matchKeyEntryId = matchKeyEntryId;
            this.keyComparator = keyComparator;
        }

        protected void put(Actor self, KeyType key, Comparable<?> position, Object value) {
            MailboxAggregation m = (MailboxAggregation) self.getMailbox();
            this.putPosition = position;
            this.putValue = value;
            m.getTable(matchKeyEntryId).put(key, this);
        }

        @Override
        public KeyHistograms.KeyComparator<?> getKeyComparator() {
            return keyComparator;
        }

    }

    public static class KeyComparatorOrdered<KeyType extends Comparable<KeyType>> implements KeyHistograms.KeyComparator<KeyType> {
        @Override
        public int compare(KeyType key1, KeyType key2) {
            return key1.compareTo(key2);
        }
    }

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
    }

    public static class ActorBehaviorMatchKeyTwo<KeyType, ValueType1, ValueType2> extends ActorBehaviorMatchKey<KeyType> {
        protected KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1;
        protected KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2;

        protected BiConsumer<ValueType1, ValueType2> handler;

        public ActorBehaviorMatchKeyTwo(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                        KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1,
                                        KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2,
                                        BiConsumer<ValueType1, ValueType2> handler) {
            super(matchKeyEntryId, 2, keyComparator);
            this.keyExtractorFromValue1 = keyExtractorFromValue1;
            this.keyExtractorFromValue2 = keyExtractorFromValue2;
            this.handler = handler;
        }

        @Override
        public KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeafTwo(key, this, height);
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object value = message.getData();
            KeyType key;
            Comparable<?> pos;
            if (keyExtractorFromValue1.matchValue(value)) {
                key = keyExtractorFromValue1.toKey((ValueType1) value);
                pos = 0;
            } else if (keyExtractorFromValue2.matchValue(value)) {
                key = keyExtractorFromValue2.toKey((ValueType2) value);
                pos = 1;
            } else {
                return false;
            }

            put(self, key, pos, value);
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean processTable(MailboxAggregation m) {
            HistogramNodeLeafTwo next = ((HistogramNodeLeafTwo) m.getTable(matchKeyEntryId).takeCompleted());
            if (next != null) {
                next.consume((BiConsumer<Object,Object>) handler);
                return true;
            }
            return false;
        }

        @Override
        public Object selectFromValue(Object value) {
            if (keyExtractorFromValue1.matchValue(value)) {
                return 0;
            } else if (keyExtractorFromValue2.matchValue(value)) {
                return 1;
            } else {
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object extractKeyFromValue(Object value, Object position) {
            if (position.equals(0)) {
                return keyExtractorFromValue1.toKey((ValueType1) value);
            } else if (position.equals(1)) {
                return keyExtractorFromValue2.toKey((ValueType2) value);
            } else {
                return null;
            }
        }
    }

    public static abstract class HistogramNodeLeafN extends KeyHistograms.HistogramNodeLeaf {
        public HistogramNodeLeafN(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        public abstract List<KeyHistograms.HistogramLeafList> getValueList();
    }

    public static class HistogramNodeLeafTwo extends HistogramNodeLeafN {
        protected KeyHistograms.HistogramLeafList values1;
        protected KeyHistograms.HistogramLeafList values2;

        public HistogramNodeLeafTwo(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        protected void initStruct() {
            values1 = new KeyHistograms.HistogramLeafList();
            values2 = new KeyHistograms.HistogramLeafList();
        }

        @Override
        protected void putValueStruct(KeyHistograms.HistogramPutContext context) {
            if (context.putPosition.equals(0)) {
                values1.add(context.putValue);
            } else {
                values2.add(context.putValue);
            }
        }

        @Override
        protected boolean completedAfterPut(KeyHistograms.HistogramPutContext context) {
            return !values1.isEmpty() && !values2.isEmpty();
        }

        public void consume(BiConsumer<Object,Object> handler) {
            afterTake(2);
            handler.accept(values1.poll(), values2.poll());
        }

        public List<KeyHistograms.HistogramLeafList> getValueList() {
            return Arrays.asList(values1, values2);
        }
    }


    public static class ActorBehaviorMatchKeyThree<KeyType, ValueType1, ValueType2, ValueType3> extends ActorBehaviorMatchKey<KeyType> {
        protected KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1;
        protected KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2;
        protected KeyExtractor<KeyType, ValueType3> keyExtractorFromValue3;

        protected TriConsumer<ValueType1, ValueType2, ValueType3> handler;

        public ActorBehaviorMatchKeyThree(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                        KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1,
                                        KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2,
                                        KeyExtractor<KeyType, ValueType3> keyExtractorFromValue3,
                                        TriConsumer<ValueType1, ValueType2, ValueType3> handler) {
            super(matchKeyEntryId, 3, keyComparator);
            this.keyExtractorFromValue1 = keyExtractorFromValue1;
            this.keyExtractorFromValue2 = keyExtractorFromValue2;
            this.keyExtractorFromValue3 = keyExtractorFromValue3;
            this.handler = handler;
        }

        @Override
        public KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeafThree(key, this, height);
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object value = message.getData();
            KeyType key;
            Comparable<?> pos;
            if (keyExtractorFromValue1.matchValue(value)) {
                key = keyExtractorFromValue1.toKey((ValueType1) value);
                pos = 0;
            } else if (keyExtractorFromValue2.matchValue(value)) {
                key = keyExtractorFromValue2.toKey((ValueType2) value);
                pos = 1;
            } else if (keyExtractorFromValue3.matchValue(value)) {
                key = keyExtractorFromValue3.toKey((ValueType3) value);
                pos = 2;
            } else {
                return false;
            }

            put(self, key, pos, value);
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean processTable(MailboxAggregation m) {
            HistogramNodeLeafThree next = ((HistogramNodeLeafThree) m.getTable(matchKeyEntryId).takeCompleted());
            if (next != null) {
                next.consume((TriConsumer<Object,Object,Object>) handler);
                return true;
            }
            return false;
        }

        @Override
        public Object selectFromValue(Object value) {
            if (keyExtractorFromValue1.matchValue(value)) {
                return 0;
            } else if (keyExtractorFromValue2.matchValue(value)) {
                return 1;
            } else if (keyExtractorFromValue3.matchValue(value)) {
                return 2;
            } else {
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object extractKeyFromValue(Object value, Object position) {
            if (position.equals(0)) {
                return keyExtractorFromValue1.toKey((ValueType1) value);
            } else if (position.equals(1)) {
                return keyExtractorFromValue2.toKey((ValueType2) value);
            } else if (position.equals(2)) {
                return keyExtractorFromValue3.toKey((ValueType3) value);
            } else {
                return null;
            }
        }
    }


    public static class HistogramNodeLeafThree extends HistogramNodeLeafN {
        protected KeyHistograms.HistogramLeafList values1;
        protected KeyHistograms.HistogramLeafList values2;
        protected KeyHistograms.HistogramLeafList values3;

        public HistogramNodeLeafThree(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        protected void initStruct() {
            values1 = new KeyHistograms.HistogramLeafList();
            values2 = new KeyHistograms.HistogramLeafList();
            values3 = new KeyHistograms.HistogramLeafList();
        }

        @Override
        protected void putValueStruct(KeyHistograms.HistogramPutContext context) {
            Comparable<?> pos = context.putPosition;
            if (pos.equals(0)) {
                values1.add(context.putValue);
            } else if (pos.equals(1)) {
                values2.add(context.putValue);
            } else {
                values3.add(context.putValue);
            }
        }

        @Override
        protected boolean completedAfterPut(KeyHistograms.HistogramPutContext context) {
            return !values1.isEmpty() && !values2.isEmpty() && !values3.isEmpty();
        }

        public void consume(TriConsumer<Object,Object,Object> handler) {
            afterTake(3);
            handler.accept(values1.poll(), values2.poll(), values3.poll());
        }

        public List<KeyHistograms.HistogramLeafList> getValueList() {
            return Arrays.asList(values1, values2, values3);
        }
    }

    public static class ActorBehaviorMatchKeyFour<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> extends ActorBehaviorMatchKey<KeyType> {
        protected KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1;
        protected KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2;
        protected KeyExtractor<KeyType, ValueType3> keyExtractorFromValue3;
        protected KeyExtractor<KeyType, ValueType4> keyExtractorFromValue4;

        protected QuadConsumer<ValueType1, ValueType2, ValueType3, ValueType4> handler;

        public ActorBehaviorMatchKeyFour(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                          KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1,
                                          KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2,
                                          KeyExtractor<KeyType, ValueType3> keyExtractorFromValue3,
                                          KeyExtractor<KeyType, ValueType4> keyExtractorFromValue4,
                                          QuadConsumer<ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            super(matchKeyEntryId, 4, keyComparator);
            this.keyExtractorFromValue1 = keyExtractorFromValue1;
            this.keyExtractorFromValue2 = keyExtractorFromValue2;
            this.keyExtractorFromValue3 = keyExtractorFromValue3;
            this.keyExtractorFromValue4 = keyExtractorFromValue4;
            this.handler = handler;
        }

        @Override
        public KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeafFour(key, this, height);
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object value = message.getData();
            KeyType key;
            Comparable<?> pos;
            if (keyExtractorFromValue1.matchValue(value)) {
                key = keyExtractorFromValue1.toKey((ValueType1) value);
                pos = 0;
            } else if (keyExtractorFromValue2.matchValue(value)) {
                key = keyExtractorFromValue2.toKey((ValueType2) value);
                pos = 1;
            } else if (keyExtractorFromValue3.matchValue(value)) {
                key = keyExtractorFromValue3.toKey((ValueType3) value);
                pos = 2;
            } else if (keyExtractorFromValue4.matchValue(value)) {
                key = keyExtractorFromValue4.toKey((ValueType4) value);
                pos = 3;
            } else {
                return false;
            }

            put(self, key, pos, value);
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean processTable(MailboxAggregation m) {
            HistogramNodeLeafFour next = ((HistogramNodeLeafFour) m.getTable(matchKeyEntryId).takeCompleted());
            if (next != null) {
                next.consume((QuadConsumer<Object,Object,Object,Object>) handler);
                return true;
            }
            return false;
        }

        @Override
        public Object selectFromValue(Object value) {
            if (keyExtractorFromValue1.matchValue(value)) {
                return 0;
            } else if (keyExtractorFromValue2.matchValue(value)) {
                return 1;
            } else if (keyExtractorFromValue3.matchValue(value)) {
                return 2;
            } else if (keyExtractorFromValue4.matchValue(value)) {
                return 3;
            } else {
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object extractKeyFromValue(Object value, Object position) {
            if (position.equals(0)) {
                return keyExtractorFromValue1.toKey((ValueType1) value);
            } else if (position.equals(1)) {
                return keyExtractorFromValue2.toKey((ValueType2) value);
            } else if (position.equals(2)) {
                return keyExtractorFromValue3.toKey((ValueType3) value);
            } else if (position.equals(3)) {
                return keyExtractorFromValue4.toKey((ValueType4) value);
            } else {
                return null;
            }
        }
    }


    public static class HistogramNodeLeafFour extends HistogramNodeLeafN {
        protected KeyHistograms.HistogramLeafList values1;
        protected KeyHistograms.HistogramLeafList values2;
        protected KeyHistograms.HistogramLeafList values3;
        protected KeyHistograms.HistogramLeafList values4;

        public HistogramNodeLeafFour(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        protected void initStruct() {
            values1 = new KeyHistograms.HistogramLeafList();
            values2 = new KeyHistograms.HistogramLeafList();
            values3 = new KeyHistograms.HistogramLeafList();
            values4 = new KeyHistograms.HistogramLeafList();
        }

        @Override
        protected void putValueStruct(KeyHistograms.HistogramPutContext context) {
            Comparable<?> pos = context.putPosition;
            if (pos.equals(0)) {
                values1.add(context.putValue);
            } else if (pos.equals(1)) {
                values2.add(context.putValue);
            } else if (pos.equals(2)) {
                values3.add(context.putValue);
            } else {
                values4.add(context.putValue);
            }
        }

        @Override
        protected boolean completedAfterPut(KeyHistograms.HistogramPutContext context) {
            return !values1.isEmpty() && !values2.isEmpty() && !values3.isEmpty() && !values4.isEmpty();
        }

        public void consume(QuadConsumer<Object,Object,Object,Object> handler) {
            afterTake(4);
            handler.accept(values1.poll(), values2.poll(), values3.poll(), values4.poll());
        }

        public List<KeyHistograms.HistogramLeafList> getValueList() {
            return Arrays.asList(values1, values2, values3, values4);
        }
    }

    @FunctionalInterface
    public interface TriConsumer<V1,V2,V3> {
        void accept(V1 a, V2 b, V3 c);
    }

    @FunctionalInterface
    public interface QuadConsumer<V1,V2,V3,V4> {
        void accept(V1 a, V2 b, V3 c, V4 d);
    }

    public static class ActorBehaviorMatchKeyList<KeyType, ValueType> extends ActorBehaviorMatchKey<KeyType> {
        protected KeyExtractor<KeyType, ValueType> keyExtractorFromValue;
        protected BiConsumer<KeyType, List<ValueType>> handler;

        public ActorBehaviorMatchKeyList(int matchKeyEntryId, int threshold, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                         KeyExtractor<KeyType, ValueType> keyExtractorFromValue,
                                         BiConsumer<KeyType, List<ValueType>> handler) {
            super(matchKeyEntryId, threshold, keyComparator);
            this.keyExtractorFromValue = keyExtractorFromValue;
            this.handler = handler;
        }

        @Override
        public KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeafList(key, this, height);
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
            put(self, key, null, value);
            return true;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public boolean processTable(MailboxAggregation m) {
            HistogramNodeLeafList next = ((HistogramNodeLeafList) m.getTable(matchKeyEntryId).takeCompleted());
            if (next != null) {
                next.consume(putRequiredSize, (BiConsumer) handler);
                return true;
            }
            return false;
        }

        @Override
        public Object selectFromValue(Object value) {
            if (keyExtractorFromValue.matchValue(value)) {
                return true;
            } else {
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object extractKeyFromValue(Object value, Object position) {
            return keyExtractorFromValue.toKey((ValueType) value);
        }
    }

    public static class HistogramNodeLeafList extends HistogramNodeLeafN {
        protected KeyHistograms.HistogramLeafList values;
        public HistogramNodeLeafList(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        protected void initStruct() {
            values = new KeyHistograms.HistogramLeafList();
        }

        @Override
        protected void putValueStruct(KeyHistograms.HistogramPutContext context) {
            values.add(context.putValue);
        }

        @Override
        protected boolean completedAfterPut(KeyHistograms.HistogramPutContext context) {
            return context.putRequiredSize >= size;
        }

        public void consume(int requiredSize, BiConsumer<Object,List<Object>> handler) {
            List<Object> vs = new ArrayList<>(requiredSize);
            for (int i = 0; i < requiredSize; ++i) {
                vs.add(values.poll());
            }
            afterTake(requiredSize);
            handler.accept(key, vs);
        }

        public List<KeyHistograms.HistogramLeafList> getValueList() {
            return Collections.singletonList(values);
        }
    }
}
