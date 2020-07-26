package csl.actor.kelp_old;

import csl.actor.Actor;
import csl.actor.ActorBehavior;
import csl.actor.Message;
import csl.actor.kelp_old.ActorBehaviorBuilderKelp.QuadConsumer;
import csl.actor.kelp_old.ActorBehaviorBuilderKelp.QuintConsumer;
import csl.actor.kelp_old.ActorBehaviorBuilderKelp.TriConsumer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ActorBehaviorKelp {
    @SuppressWarnings("unchecked")
    public static <KeyType> KeyType centerPointPrimitive(KeyType leftEnd, KeyType rightStart) {
        if (leftEnd instanceof Number && rightStart instanceof Number) {
            if (leftEnd instanceof Long && rightStart instanceof Long) {
                long r = (Long) rightStart;
                long l = (Long) leftEnd;
                return (KeyType) (Long) (Math.max(1L, (r - l) / 2L) + l);
            } else if (leftEnd instanceof Integer && rightStart instanceof Integer) {
                int r = (Integer) rightStart;
                int l = (Integer) leftEnd;
                return (KeyType) (Integer) (Math.max(1, (r - l) / 2) + l);
            } else if (leftEnd instanceof Double && rightStart instanceof Double) {
                double r = (Double) rightStart;
                double l = (Double) leftEnd;
                return (KeyType) (Double) (r - (r - l) / 2.0);
            } else if (leftEnd instanceof Float && rightStart instanceof Float) {
                float r = (Float) rightStart;
                float l = (Float) leftEnd;
                return (KeyType) (Float) (r - (r - l) / 2f);
            } else if (leftEnd instanceof BigDecimal && rightStart instanceof BigDecimal) {
                BigDecimal r = (BigDecimal) rightStart;
                BigDecimal l = (BigDecimal) leftEnd;
                return (KeyType) r.subtract(r.subtract(l).divide(BigDecimal.valueOf(2), RoundingMode.FLOOR));
            } else if (leftEnd instanceof BigInteger && rightStart instanceof BigInteger) {
                BigInteger r = (BigInteger) rightStart;
                BigInteger l = (BigInteger) leftEnd;
                return (KeyType) r.subtract(r.subtract(l).divide(BigInteger.valueOf(2)));
            } else {
                return rightStart;
            }
        } else if (leftEnd instanceof UUID && rightStart instanceof UUID) {
            UUID r = (UUID) rightStart;
            UUID l = (UUID) leftEnd;
            if (r.getMostSignificantBits() == l.getMostSignificantBits()) {
                return (KeyType) new UUID(r.getMostSignificantBits(),
                        centerPointPrimitive(l.getLeastSignificantBits(), r.getLeastSignificantBits()));
            } else {
                return (KeyType) new UUID(
                        centerPointPrimitive(l.getMostSignificantBits(), r.getMostSignificantBits()),
                        centerPointPrimitive(l.getLeastSignificantBits(), r.getLeastSignificantBits()));
            }
        } else {
            return rightStart;
        }
    }

    public static abstract class ActorBehaviorMatchKey<KeyType> extends KeyHistograms.HistogramPutContext
            implements ActorBehavior, MailboxKelp.HistogramProcessor {
        protected int matchKeyEntryId;
        protected KeyHistograms.KeyComparator<KeyType> keyComparator;

        public ActorBehaviorMatchKey(int matchKeyEntryId, int requiredSize, KeyHistograms.KeyComparator<KeyType> keyComparator) {
            this.putRequiredSize = requiredSize;
            this.matchKeyEntryId = matchKeyEntryId;
            this.keyComparator = keyComparator;
        }

        protected void put(Actor self, KeyType key, Comparable<?> position, Object value) {
            MailboxKelp m = (MailboxKelp) self.getMailbox();
            this.putPosition = position;
            this.putValue = value;
            m.getHistogram(matchKeyEntryId).put(key, this);
        }

        @Override
        public KeyHistograms.KeyComparator<?> getKeyComparator() {
            return keyComparator;
        }

        public abstract List<ActorBehaviorBuilderKelp.KeyExtractor<KeyType,?>> getKeyExtractors();
        public abstract Object getHandler();
    }

    public static class KeyComparatorOrdered<KeyType extends Comparable<KeyType>> implements KeyHistograms.KeyComparator<KeyType> {
        public static final long serialVersionUID = 1L;
        @Override
        public int compare(KeyType key1, KeyType key2) {
            return key1.compareTo(key2);
        }

        @Override
        public KeyType centerPoint(KeyType leftEnd, KeyType rightStart) {
            return centerPointPrimitive(leftEnd, rightStart);
        }
    }

    public static class ActorBehaviorMatchKey1<KeyType, ParamType1, ValueType1> extends ActorBehaviorMatchKey<KeyType> {
        protected ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1;
        protected Function<ParamType1, ValueType1> valueExtractorFromValue1;

        protected BiConsumer<KeyType, ValueType1> handler;

        public ActorBehaviorMatchKey1(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                      ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                      Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                      BiConsumer<KeyType, ValueType1> handler) {
            super(matchKeyEntryId, 1, keyComparator);
            this.keyExtractorFromValue1 = keyExtractorFromValue1;
            this.valueExtractorFromValue1 = valueExtractorFromValue1;
            this.handler = handler;
        }

        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeaf1(key, this, height);
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object value = message.getData();
            KeyType key;
            Comparable<?> pos;
            if (keyExtractorFromValue1.matchValue(value)) {
                key = keyExtractorFromValue1.toKey((ParamType1) value);
                pos = 0;
                value = valueExtractorFromValue1.apply((ParamType1) value);
            } else {
                return false;
            }

            put(self, key, pos, value);
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean processHistogram(Actor self, MailboxKelp m) {
            KeyHistograms.HistogramTree tree = m.getHistogram(matchKeyEntryId);
            HistogramNodeLeaf1 next = ((HistogramNodeLeaf1) tree.takeCompleted());
            if (next != null) {
                return next.consume(tree, (BiConsumer<Object,Object>) handler);
            }
            return false;
        }

        @Override
        public Object selectFromValue(Object value) {
            if (keyExtractorFromValue1.matchValue(value)) {
                return 0;
            } else {
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object extractKeyFromValue(Object value, Object position) {
            if (position.equals(0)) {
                return keyExtractorFromValue1.toKey((ParamType1) value);
            } else {
                return null;
            }
        }

        /** @return implementation field getter */
        public List<ActorBehaviorBuilderKelp.KeyExtractor<KeyType,?>> getKeyExtractors() {
            return Collections.singletonList(keyExtractorFromValue1);
        }

        /** @return implementation field getter */
        public BiConsumer<KeyType, ValueType1> getHandler() {
            return handler;
        }
    }

    public static abstract class HistogramNodeLeafN extends KeyHistograms.HistogramNodeLeaf {
        public static final long serialVersionUID = 1L;
        public HistogramNodeLeafN(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }
    }


    public static class HistogramNodeLeaf1 extends HistogramNodeLeafN {
        public static final long serialVersionUID = 1L;
        protected KeyHistograms.HistogramLeafList values1;

        public HistogramNodeLeaf1(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        protected void initStruct(KeyHistograms.HistogramPutContext context) {
            values1 = context.createEmptyList();
        }

        @Override
        protected void putValueStruct(KeyHistograms.HistogramPutContext context) {
            values1.add(context.putTree, context.putValue);
        }

        @Override
        protected boolean completedAfterPut(KeyHistograms.HistogramPutContext context) {
            return !values1.isEmpty();
        }

        public boolean consume(KeyHistograms.HistogramTree tree, BiConsumer<Object, Object> handler) {
            if (completedAfterPut(null)) {  //currently, it can complete before consume, and then it might not be able to consume 2 or more times
                afterTake(1, tree);
                handler.accept(getKey(), values1.poll(tree));
                return true;
            } else {
                return false;
            }
        }

        public List<KeyHistograms.HistogramLeafList> getStructList() {
            return Collections.singletonList(values1);
        }

        @Override
        public void setStructList(int i, KeyHistograms.HistogramLeafList list) {
            if (i == 0) { this.values1 = list; }
        }
    }

    public static class ActorBehaviorMatchKey2<KeyType, ParamType1, ParamType2, ValueType1, ValueType2> extends ActorBehaviorMatchKey<KeyType> {
        protected ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1;
        protected ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2;
        protected Function<ParamType1, ValueType1> valueExtractorFromValue1;
        protected Function<ParamType2, ValueType2> valueExtractorFromValue2;

        protected TriConsumer<KeyType, ValueType1, ValueType2> handler;


        public ActorBehaviorMatchKey2(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                      ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                      ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2,
                                      Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                      Function<ParamType2, ValueType2> valueExtractorFromValue2,
                                      TriConsumer<KeyType, ValueType1, ValueType2> handler) {
            super(matchKeyEntryId, 2, keyComparator);
            this.keyExtractorFromValue1 = keyExtractorFromValue1;
            this.keyExtractorFromValue2 = keyExtractorFromValue2;
            this.valueExtractorFromValue1 = valueExtractorFromValue1;
            this.valueExtractorFromValue2 = valueExtractorFromValue2;
            this.handler = handler;
        }

        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeaf2(key, this, height);
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object value = message.getData();
            KeyType key;
            Comparable<?> pos;
            if (keyExtractorFromValue1.matchValue(value)) {
                key = keyExtractorFromValue1.toKey((ParamType1) value);
                pos = 0;
                value = valueExtractorFromValue1.apply((ParamType1) value);
            } else if (keyExtractorFromValue2.matchValue(value)) {
                key = keyExtractorFromValue2.toKey((ParamType2) value);
                value = valueExtractorFromValue2.apply((ParamType2) value);
                pos = 1;
            } else {
                return false;
            }

            put(self, key, pos, value);
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean processHistogram(Actor self, MailboxKelp m) {
            KeyHistograms.HistogramTree tree = m.getHistogram(matchKeyEntryId);
            HistogramNodeLeaf2 next = ((HistogramNodeLeaf2) tree.takeCompleted());
            if (next != null) {
                return next.consume(tree, (TriConsumer<Object, Object ,Object>) handler);
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
                return keyExtractorFromValue1.toKey((ParamType1) value);
            } else if (position.equals(1)) {
                return keyExtractorFromValue2.toKey((ParamType2) value);
            } else {
                return null;
            }
        }

        /** @return implementation field getter */
        public List<ActorBehaviorBuilderKelp.KeyExtractor<KeyType,?>> getKeyExtractors() {
            return Arrays.asList(keyExtractorFromValue1, keyExtractorFromValue2);
        }

        /** @return implementation field getter */
        @Override
        public TriConsumer<KeyType, ValueType1, ValueType2> getHandler() {
            return handler;
        }
    }

    public static class HistogramNodeLeaf2 extends HistogramNodeLeafN {
        public static final long serialVersionUID = 1L;
        protected KeyHistograms.HistogramLeafList values1;
        protected KeyHistograms.HistogramLeafList values2;

        public HistogramNodeLeaf2(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        protected void initStruct(KeyHistograms.HistogramPutContext context) {
            values1 = context.createEmptyList();
            values2 = context.createEmptyList();
        }

        @Override
        protected void putValueStruct(KeyHistograms.HistogramPutContext context) {
            if (context.putPosition.equals(0)) {
                values1.add(context.putTree, context.putValue);
            } else {
                values2.add(context.putTree, context.putValue);
            }
        }

        @Override
        protected boolean completedAfterPut(KeyHistograms.HistogramPutContext context) {
            return !values1.isEmpty() && !values2.isEmpty();
        }

        public boolean consume(KeyHistograms.HistogramTree tree, TriConsumer<Object, Object, Object> handler) {
            if (completedAfterPut(null)) {  //currently, it can complete before consume, and then it might not be able to consume 2 or more times
                afterTake(2, tree);
                handler.accept(getKey(), values1.poll(tree), values2.poll(tree));
                return true;
            } else {
                return false;
            }
        }

        public List<KeyHistograms.HistogramLeafList> getStructList() {
            return Arrays.asList(values1, values2);
        }

        @Override
        public void setStructList(int i, KeyHistograms.HistogramLeafList list) {
            if (i == 0) { this.values1 = list; }
            else if (i == 1) { this.values2 = list; }
        }
    }

    public static class ActorBehaviorMatchKey3<KeyType, ParamType1, ParamType2, ParamType3, ValueType1, ValueType2, ValueType3> extends ActorBehaviorMatchKey<KeyType> {
        protected ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1;
        protected ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2;
        protected ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType3> keyExtractorFromValue3;
        protected Function<ParamType1, ValueType1> valueExtractorFromValue1;
        protected Function<ParamType2, ValueType2> valueExtractorFromValue2;
        protected Function<ParamType3, ValueType3> valueExtractorFromValue3;

        protected QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> handler;

        public ActorBehaviorMatchKey3(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                      ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                      ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2,
                                      ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType3> keyExtractorFromValue3,
                                      Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                      Function<ParamType2, ValueType2> valueExtractorFromValue2,
                                      Function<ParamType3, ValueType3> valueExtractorFromValue3,
                                      QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> handler) {
            super(matchKeyEntryId, 3, keyComparator);
            this.keyExtractorFromValue1 = keyExtractorFromValue1;
            this.keyExtractorFromValue2 = keyExtractorFromValue2;
            this.keyExtractorFromValue3 = keyExtractorFromValue3;
            this.valueExtractorFromValue1 = valueExtractorFromValue1;
            this.valueExtractorFromValue2 = valueExtractorFromValue2;
            this.valueExtractorFromValue3 = valueExtractorFromValue3;
            this.handler = handler;
        }

        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeaf3(key, this, height);
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object value = message.getData();
            KeyType key;
            Comparable<?> pos;
            if (keyExtractorFromValue1.matchValue(value)) {
                key = keyExtractorFromValue1.toKey((ParamType1) value);
                pos = 0;
                value = valueExtractorFromValue1.apply((ParamType1) value);
            } else if (keyExtractorFromValue2.matchValue(value)) {
                key = keyExtractorFromValue2.toKey((ParamType2) value);
                pos = 1;
                value = valueExtractorFromValue2.apply((ParamType2) value);
            } else if (keyExtractorFromValue3.matchValue(value)) {
                key = keyExtractorFromValue3.toKey((ParamType3) value);
                pos = 2;
                value = valueExtractorFromValue3.apply((ParamType3) value);
            } else {
                return false;
            }

            put(self, key, pos, value);
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean processHistogram(Actor self, MailboxKelp m) {
            KeyHistograms.HistogramTree tree = m.getHistogram(matchKeyEntryId);
            HistogramNodeLeaf3 next = ((HistogramNodeLeaf3) tree.takeCompleted());
            if (next != null) {
                return next.consume(tree, (QuadConsumer<Object,Object,Object,Object>) handler);
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
                return keyExtractorFromValue1.toKey((ParamType1) value);
            } else if (position.equals(1)) {
                return keyExtractorFromValue2.toKey((ParamType2) value);
            } else if (position.equals(2)) {
                return keyExtractorFromValue3.toKey((ParamType3) value);
            } else {
                return null;
            }
        }

        /** @return implementation field getter */
        public List<ActorBehaviorBuilderKelp.KeyExtractor<KeyType,?>> getKeyExtractors() {
            return Arrays.asList(keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3);
        }

        /** @return implementation field getter */
        @Override
        public QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> getHandler() {
            return handler;
        }
    }

    public static class HistogramNodeLeaf3 extends HistogramNodeLeafN {
        public static final long serialVersionUID = 1L;
        protected KeyHistograms.HistogramLeafList values1;
        protected KeyHistograms.HistogramLeafList values2;
        protected KeyHistograms.HistogramLeafList values3;

        public HistogramNodeLeaf3(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        protected void initStruct(KeyHistograms.HistogramPutContext context) {
            values1 = context.createEmptyList();
            values2 = context.createEmptyList();
            values3 = context.createEmptyList();
        }

        @Override
        protected void putValueStruct(KeyHistograms.HistogramPutContext context) {
            Comparable<?> pos = context.putPosition;
            if (pos.equals(0)) {
                values1.add(context.putTree, context.putValue);
            } else if (pos.equals(1)) {
                values2.add(context.putTree, context.putValue);
            } else {
                values3.add(context.putTree, context.putValue);
            }
        }

        @Override
        protected boolean completedAfterPut(KeyHistograms.HistogramPutContext context) {
            return !values1.isEmpty() && !values2.isEmpty() && !values3.isEmpty();
        }

        public boolean consume(KeyHistograms.HistogramTree tree, QuadConsumer<Object, Object, Object, Object> handler) {
            if (completedAfterPut(null)) {
                afterTake(3, tree);
                handler.accept(getKey(), values1.poll(tree), values2.poll(tree), values3.poll(tree));
                return true;
            } else {
                return false;
            }
        }

        public List<KeyHistograms.HistogramLeafList> getStructList() {
            return Arrays.asList(values1, values2, values3);
        }

        @Override
        public void setStructList(int i, KeyHistograms.HistogramLeafList list) {
            if (i == 0) { this.values1 = list; }
            else if (i == 1) { this.values2 = list; }
            else if (i == 2) { this.values3 = list; }
        }
    }

    public static class ActorBehaviorMatchKey4<KeyType, ParamType1, ParamType2, ParamType3, ParamType4, ValueType1, ValueType2, ValueType3, ValueType4> extends ActorBehaviorMatchKey<KeyType> {
        protected ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1;
        protected ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2;
        protected ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType3> keyExtractorFromValue3;
        protected ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType4> keyExtractorFromValue4;
        protected Function<ParamType1, ValueType1> valueExtractorFromValue1;
        protected Function<ParamType2, ValueType2> valueExtractorFromValue2;
        protected Function<ParamType3, ValueType3> valueExtractorFromValue3;
        protected Function<ParamType4, ValueType4> valueExtractorFromValue4;

        protected QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> handler;

        public ActorBehaviorMatchKey4(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                      ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                      ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2,
                                      ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType3> keyExtractorFromValue3,
                                      ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType4> keyExtractorFromValue4,
                                      Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                      Function<ParamType2, ValueType2> valueExtractorFromValue2,
                                      Function<ParamType3, ValueType3> valueExtractorFromValue3,
                                      Function<ParamType4, ValueType4> valueExtractorFromValue4,
                                      QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            super(matchKeyEntryId, 4, keyComparator);
            this.keyExtractorFromValue1 = keyExtractorFromValue1;
            this.keyExtractorFromValue2 = keyExtractorFromValue2;
            this.keyExtractorFromValue3 = keyExtractorFromValue3;
            this.keyExtractorFromValue4 = keyExtractorFromValue4;
            this.valueExtractorFromValue1 = valueExtractorFromValue1;
            this.valueExtractorFromValue2 = valueExtractorFromValue2;
            this.valueExtractorFromValue3 = valueExtractorFromValue3;
            this.valueExtractorFromValue4 = valueExtractorFromValue4;
            this.handler = handler;
        }

        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeaf4(key, this, height);
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object value = message.getData();
            KeyType key;
            Comparable<?> pos;
            if (keyExtractorFromValue1.matchValue(value)) {
                key = keyExtractorFromValue1.toKey((ParamType1) value);
                pos = 0;
                value = valueExtractorFromValue1.apply((ParamType1) value);
            } else if (keyExtractorFromValue2.matchValue(value)) {
                key = keyExtractorFromValue2.toKey((ParamType2) value);
                pos = 1;
                value = valueExtractorFromValue2.apply((ParamType2) value);
            } else if (keyExtractorFromValue3.matchValue(value)) {
                key = keyExtractorFromValue3.toKey((ParamType3) value);
                pos = 2;
                value = valueExtractorFromValue3.apply((ParamType3) value);
            } else if (keyExtractorFromValue4.matchValue(value)) {
                key = keyExtractorFromValue4.toKey((ParamType4) value);
                pos = 3;
                value = valueExtractorFromValue4.apply((ParamType4) value);
            } else {
                return false;
            }

            put(self, key, pos, value);
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean processHistogram(Actor self, MailboxKelp m) {
            KeyHistograms.HistogramTree tree = m.getHistogram(matchKeyEntryId);
            HistogramNodeLeaf4 next = ((HistogramNodeLeaf4) tree.takeCompleted());
            if (next != null) {
                return next.consume(tree, (QuintConsumer<Object, Object,Object,Object,Object>) handler);
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
                return keyExtractorFromValue1.toKey((ParamType1) value);
            } else if (position.equals(1)) {
                return keyExtractorFromValue2.toKey((ParamType2) value);
            } else if (position.equals(2)) {
                return keyExtractorFromValue3.toKey((ParamType3) value);
            } else if (position.equals(3)) {
                return keyExtractorFromValue4.toKey((ParamType4) value);
            } else {
                return null;
            }
        }

        /** @return implementation field getter */
        public List<ActorBehaviorBuilderKelp.KeyExtractor<KeyType,?>> getKeyExtractors() {
            return Arrays.asList(keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3, keyExtractorFromValue4);
        }

        /** @return implementation field getter */
        public QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> getHandler() {
            return handler;
        }
    }

    public static class HistogramNodeLeaf4 extends HistogramNodeLeafN {
        public static final long serialVersionUID = 1L;
        protected KeyHistograms.HistogramLeafList values1;
        protected KeyHistograms.HistogramLeafList values2;
        protected KeyHistograms.HistogramLeafList values3;
        protected KeyHistograms.HistogramLeafList values4;

        public HistogramNodeLeaf4(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        protected void initStruct(KeyHistograms.HistogramPutContext context) {
            values1 = context.createEmptyList();
            values2 = context.createEmptyList();
            values3 = context.createEmptyList();
            values4 = context.createEmptyList();
        }

        @Override
        protected void putValueStruct(KeyHistograms.HistogramPutContext context) {
            Comparable<?> pos = context.putPosition;
            if (pos.equals(0)) {
                values1.add(context.putTree, context.putValue);
            } else if (pos.equals(1)) {
                values2.add(context.putTree, context.putValue);
            } else if (pos.equals(2)) {
                values3.add(context.putTree, context.putValue);
            } else {
                values4.add(context.putTree, context.putValue);
            }
        }

        @Override
        protected boolean completedAfterPut(KeyHistograms.HistogramPutContext context) {
            return !values1.isEmpty() && !values2.isEmpty() && !values3.isEmpty() && !values4.isEmpty();
        }

        public boolean consume(KeyHistograms.HistogramTree tree, QuintConsumer<Object, Object, Object, Object, Object> handler) {
            if (completedAfterPut(null)) {
                afterTake(4, tree);
                handler.accept(getKey(), values1.poll(tree), values2.poll(tree), values3.poll(tree), values4.poll(tree));
                return true;
            } else {
                return false;
            }
        }

        public List<KeyHistograms.HistogramLeafList> getStructList() {
            return Arrays.asList(values1, values2, values3, values4);
        }

        @Override
        public void setStructList(int i, KeyHistograms.HistogramLeafList list) {
            if (i == 0) { this.values1 = list; }
            else if (i == 1) { this.values2 = list; }
            else if (i == 2) { this.values3 = list; }
            else if (i == 3) { this.values4 = list; }
        }
    }

    public static class ActorBehaviorMatchKeyList<KeyType, ParamType, ValueType> extends ActorBehaviorMatchKey<KeyType> {
        protected ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType> keyExtractorFromValue;
        protected Function<ParamType, ValueType> valueExtractorFromValue;
        protected BiConsumer<KeyType, List<ValueType>> handler;

        public ActorBehaviorMatchKeyList(int matchKeyEntryId, int threshold, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                         ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                         Function<ParamType, ValueType> valueExtractorFromValue,
                                         BiConsumer<KeyType, List<ValueType>> handler) {
            super(matchKeyEntryId, threshold, keyComparator);
            this.keyExtractorFromValue = keyExtractorFromValue;
            this.valueExtractorFromValue = valueExtractorFromValue;
            this.handler = handler;
        }

        public ActorBehaviorMatchKey<KeyType> withKeyValuesReducers(List<BiFunction<KeyType, List<ValueType>, Iterable<ValueType>>> keyValuesReducers) {
            if (keyValuesReducers.isEmpty()) {
                return this;
            } else {
                return new ActorBehaviorMatchKeyListFuture<>(matchKeyEntryId, this.putRequiredSize, keyComparator,
                        new ActorBehaviorBuilderKelp.KeyValuesReducerList<>(keyValuesReducers), keyExtractorFromValue, valueExtractorFromValue, handler);
            }
        }

        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeafList(key, this, height);
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object value = message.getData();
            KeyType key;
            if (keyExtractorFromValue.matchValue(value)) {
                key = keyExtractorFromValue.toKey((ParamType) value);
                value = valueExtractorFromValue.apply((ParamType) value);
            } else {
                return false;
            }
            put(self, key, true, value);
            return true;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public boolean processHistogram(Actor self, MailboxKelp m) {
            KeyHistograms.HistogramTree tree = m.getHistogram(matchKeyEntryId);
            HistogramNodeLeafList next = (HistogramNodeLeafList) tree.takeCompleted();
            if (next != null) {
                return next.consume(putRequiredSize, tree, (BiConsumer) handler);
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
            return keyExtractorFromValue.toKey((ParamType) value);
        }

        /** @return implementation field getter */
        public List<ActorBehaviorBuilderKelp.KeyExtractor<KeyType,?>> getKeyExtractors() {
            return Collections.singletonList(keyExtractorFromValue);
        }

        /** @return implementation field getter */
        public BiConsumer<KeyType, List<ValueType>> getHandler() {
            return handler;
        }
    }

    public static class HistogramNodeLeafList extends HistogramNodeLeafN {
        public static final long serialVersionUID = 1L;
        protected KeyHistograms.HistogramLeafList values;
        public HistogramNodeLeafList(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        protected void initStruct(KeyHistograms.HistogramPutContext context) {
            values = context.createEmptyList();
        }

        @Override
        protected void putValueStruct(KeyHistograms.HistogramPutContext context) {
            values.add(context.putTree, context.putValue);
        }

        @Override
        protected boolean completedAfterPut(KeyHistograms.HistogramPutContext context) {
            return completed(context.putRequiredSize);
        }

        protected boolean completed(int r) {
            return r <= size;
        }

        public boolean consume(int requiredSize, KeyHistograms.HistogramTree tree, BiConsumer<Object,List<Object>> handler) {
            if (completed(requiredSize)) {
                List<Object> vs = new ArrayList<>(requiredSize);
                for (int i = 0; i < requiredSize; ++i) {
                    vs.add(values.poll(tree));
                }
                afterTake(requiredSize, tree);
                handler.accept(key, vs);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public List<KeyHistograms.HistogramLeafList> getStructList() {
            return Collections.singletonList(values);
        }

        @Override
        public void setStructList(int i, KeyHistograms.HistogramLeafList list) {
            if (i == 0) { this.values = list; }
        }
    }

    public static class ActorBehaviorMatchKeyListFuture<KeyType, ParamType, ValueType>
            extends ActorBehaviorMatchKey<KeyType> {
        protected BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer;
        protected ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType> keyExtractorFromValue;
        protected Function<ParamType, ValueType> valueExtractorFromValue;
        protected BiConsumer<KeyType, List<ValueType>> handler;

        public ActorBehaviorMatchKeyListFuture(int matchKeyEntryId, int requiredSize,
                                               KeyHistograms.KeyComparator<KeyType> keyComparator,
                                               BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                               ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                               Function<ParamType, ValueType> valueExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            super(matchKeyEntryId, requiredSize, keyComparator);
            this.keyValuesReducer = keyValuesReducer;
            this.keyExtractorFromValue = keyExtractorFromValue;
            this.valueExtractorFromValue = valueExtractorFromValue;
            this.handler = handler;
        }

        @Override
        public List<ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ?>> getKeyExtractors() {
            return Collections.singletonList(keyExtractorFromValue);
        }

        @Override
        public BiConsumer<KeyType, List<ValueType>> getHandler() {
            return handler;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object value = message.getData();
            KeyType key;
            if (keyExtractorFromValue.matchValue(value)) {
                key = keyExtractorFromValue.toKey((ParamType) value);
                value = valueExtractorFromValue.apply((ParamType) value);
            } else {
                return false;
            }
            MailboxKelp mailbox = (MailboxKelp) self.getMailbox();
            mailbox.processPersistableTraversalBeforePut(self, matchKeyEntryId);
            put(self, key, true, value);
            mailbox.updateScheduledTraversalProcess(self, this.matchKeyEntryId);
            return true;
        }

        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeafListReducible(key, this, height);
        }

        @Override
        public boolean processHistogram(Actor self, MailboxKelp m) {
            return false; //instead, consuming is done by TraversalProcess
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
            return keyExtractorFromValue.toKey((ParamType) value);
        }

        @Override
        public boolean needToProcessTraversal(Actor self, KeyHistograms.HistogramTree tree) {
            this.putTree = tree;
            return true;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void processTraversal(Actor self, MailboxKelp.ReducedSize reducedSize, KeyHistograms.HistogramNodeLeaf leaf) {
            HistogramNodeLeafListReducible list = completedLeaf(putRequiredSize, HistogramNodeLeafListReducible.class, leaf);
            if (list != null && list.consume(putRequiredSize, putTree, reducedSize, (BiFunction) keyValuesReducer, (BiConsumer) handler)) {
                self.tell(new MailboxKelp.TraversalProcess(matchKeyEntryId), self);
            }
        }

        @SuppressWarnings("unchecked")
        public <LeafType extends KeyHistograms.HistogramNodeLeaf> LeafType completedLeaf(int requiredSize, Class<LeafType> type,
                                                                                         KeyHistograms.HistogramNodeLeaf l) {
            if (l.size() >= requiredSize) {
                if (!type.isInstance(l)) {
                    return (LeafType) l.load(this);
                } else {
                    return (LeafType) l;
                }
            } else {
                return null;
            }
        }
    }

    public static class HistogramNodeLeafListReducible extends HistogramNodeLeafList {
        public static final long serialVersionUID = 1L;
        public HistogramNodeLeafListReducible(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        protected boolean completedAfterPut(KeyHistograms.HistogramPutContext context) {
            return false; //processed by processTraversal
        }

        public boolean consume(int requiredSize,
                               KeyHistograms.HistogramTree tree,
                               MailboxKelp.ReducedSize reducedSize,
                               BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer,
                               BiConsumer<Object, List<Object>> handler) {
            if (completed(requiredSize)) {
                List<Object> vs = poll(requiredSize, tree, reducedSize);
                int consuming = vs.size();
                try {
                    consuming = reduceAndHandle(requiredSize, tree, reducedSize, keyValuesReducer, handler, vs);
                } finally {
                    afterTake(consuming, tree);
                }
                return completed(requiredSize);
            } else {
                return false;
            }
        }

        protected List<Object> poll(int requiredSize, KeyHistograms.HistogramTree tree, MailboxKelp.ReducedSize reducedSize) {
            int consuming = Math.max(requiredSize, reducedSize.nextReducedSize(size()));
            List<Object> vs = new ArrayList<>(consuming);
            try {
                for (int i = 0; i < consuming; ++i) {
                    vs.add(values.poll(tree));
                }
            } catch (Exception ex) {
                throw new RuntimeException(String.format("size=%,d, consuming=%,d actual=%,d required=%,d", size(), consuming, vs.size(), requiredSize), ex);
            }
            return vs;
        }

        protected int reduceAndHandle(int requiredSize,
                                      KeyHistograms.HistogramTree tree,
                                      MailboxKelp.ReducedSize reducedSize,
                                      BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer,
                                      BiConsumer<Object, List<Object>> handler,
                                      List<Object> vs) {
            int consuming = vs.size();
            Object key = getKey();
            List<Object> rs = toList(keyValuesReducer.apply(key, vs));
            if (!rs.isEmpty()) {
                handler.accept(key, rs);
            }
            return consuming;
        }

        protected List<Object> toList(Iterable<Object> is) {
            if (is instanceof List<?>) {
                return (List<Object>) is;
            } else {
                ArrayList<Object> vs = new ArrayList<>();
                for (Object v:  is) {
                    vs.add(v);
                }
                return vs;
            }
        }
    }

    public static class ActorBehaviorMatchKeyListFuturePhase<KeyType, ParamType, ValueType>
            extends ActorBehaviorMatchKeyListFuture<KeyType, ParamType, ValueType> {

        public ActorBehaviorMatchKeyListFuturePhase(int matchKeyEntryId, int requiredSize, KeyHistograms.KeyComparator<KeyType> keyComparator,
                                                    BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                                    ActorBehaviorBuilderKelp.KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                                    Function<ParamType, ValueType> valueExtractorFromValue,
                                                    BiConsumer<KeyType, List<ValueType>> handler) {
            super(matchKeyEntryId, requiredSize, keyComparator, keyValuesReducer, keyExtractorFromValue, valueExtractorFromValue, handler);
        }

        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeafListReducibleForPhase(key, this, height);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void processTraversal(Actor self, MailboxKelp.ReducedSize reducedSize, KeyHistograms.HistogramNodeLeaf leaf) {
            HistogramNodeLeafListReducible list = completedLeaf(putRequiredSize + 1, HistogramNodeLeafListReducible.class, leaf);
            if (list != null && list.consume(putRequiredSize + 1, putTree, reducedSize, (BiFunction) keyValuesReducer, (BiConsumer) handler)) {
                self.tell(new MailboxKelp.TraversalProcess(matchKeyEntryId), self);
            }
        }

        @Override
        public boolean needToProcessPhase(Actor self, Object phaseKey, KeyHistograms.HistogramTree tree) {
            this.putTree = tree;
            return true;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void processPhase(Actor self, Object phaseKey, MailboxKelp.ReducedSize reducedSize, KeyHistograms.HistogramNodeLeaf leaf) {
            long prevSize = leaf.size();
            HistogramNodeLeafListReducibleForPhase list = completedLeaf(putRequiredSize, HistogramNodeLeafListReducibleForPhase.class, leaf);
            if (list != null) {
                while (list.consumePhase(putRequiredSize, putTree, reducedSize, (BiFunction) keyValuesReducer, (BiConsumer) handler)) {
                    if (prevSize <= leaf.size()) { //no consumption
                        break;
                    }
                }
            }
        }
    }

    public static class HistogramNodeLeafListReducibleForPhase extends HistogramNodeLeafListReducible {
        public static final long serialVersionUID = 1L;
        public HistogramNodeLeafListReducibleForPhase(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        protected int reduceAndHandle(int requiredSize, KeyHistograms.HistogramTree tree, MailboxKelp.ReducedSize reducedSize,
                                      BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer,
                                      BiConsumer<Object, List<Object>> handler, List<Object> vs) {
            Object key = getKey();
            int consuming = vs.size();
            Iterable<Object> rs = keyValuesReducer.apply(key, vs);
            for (Object r : rs) {
                values.add(tree, r);
                consuming--;
            }
            return consuming;
        }

        public boolean consumePhase(int requiredSize,
                               KeyHistograms.HistogramTree tree,
                               MailboxKelp.ReducedSize reducedSize,
                               BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer,
                               BiConsumer<Object, List<Object>> handler) {
            if (completed(requiredSize)) {
                List<Object> vs = poll(requiredSize, tree, reducedSize);
                int consuming = vs.size();
                try {
                    consuming = super.reduceAndHandle(requiredSize, tree, reducedSize, keyValuesReducer, handler, vs);
                } finally {
                    afterTake(consuming, tree);
                }
                return completed(requiredSize);
            } else {
                return false;
            }
        }

    }
}
