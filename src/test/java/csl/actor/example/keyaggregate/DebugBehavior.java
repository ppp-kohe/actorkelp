package csl.actor.example.keyaggregate;

import csl.actor.Actor;
import csl.actor.ActorBehavior;
import csl.actor.Message;
import csl.actor.keyaggregate.ActorBehaviorBuilderKeyAggregation;
import csl.actor.keyaggregate.ActorBehaviorKeyAggregation;
import csl.actor.keyaggregate.KeyHistograms;
import csl.actor.keyaggregate.MailboxKeyAggregation;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class DebugBehavior {
    public static class DebugFactory extends ActorBehaviorBuilderKeyAggregation.ActorBehaviorMatchKeyFactory {
        @Override
        public <KeyType, ValueType1> ActorBehavior get1(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1, BiConsumer<KeyType, ValueType1> handler) {
            return new DebugMatchKey1<>(matchKeyEntryId, keyComparator, keyExtractorFromValue1, handler);
        }

        @Override
        public <KeyType, ValueType1, ValueType2> ActorBehavior get2(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2, ActorBehaviorBuilderKeyAggregation.TriConsumer<KeyType, ValueType1, ValueType2> handler) {
            return new DebugMatchKey2<>(matchKeyEntryId, keyComparator, keyExtractorFromValue1, keyExtractorFromValue2, handler);
        }

        @Override
        public <KeyType, ValueType1, ValueType2, ValueType3> ActorBehavior get3(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType3> keyExtractorFromValue3, ActorBehaviorBuilderKeyAggregation.QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> handler) {
            return new DebugMatchKey3<>(matchKeyEntryId, keyComparator, keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3, handler);
        }

        @Override
        public <KeyType, ValueType1, ValueType2, ValueType3, ValueType4> ActorBehavior get4(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType3> keyExtractorFromValue3, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType4> keyExtractorFromValue4, ActorBehaviorBuilderKeyAggregation.QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            return new DebugMatchKey4<>(matchKeyEntryId, keyComparator, keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3, keyExtractorFromValue4, handler);
        }

        @Override
        public <KeyType, ValueType> ActorBehaviorKeyAggregation.ActorBehaviorMatchKeyList<KeyType, ValueType> getList(int matchKeyEntryId, int threshold, KeyHistograms.KeyComparator<KeyType> keyComparator, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType> keyExtractorFromValue, BiConsumer<KeyType, List<ValueType>> handler) {
            return new DebugMatchKeyList<>(matchKeyEntryId, threshold, keyComparator, keyExtractorFromValue, handler);
        }

        @Override
        public <KeyType, ValueType> ActorBehaviorKeyAggregation.ActorBehaviorMatchKeyListFuture<KeyType, ValueType> getListFuture(int matchKeyEntryId, int requiredSize, KeyHistograms.KeyComparator<KeyType> keyComparator, BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType> keyExtractorFromValue, BiConsumer<KeyType, List<ValueType>> handler) {
            return new DebugMatchKeyListFuture<>(matchKeyEntryId, requiredSize, keyComparator, keyValuesReducer, keyExtractorFromValue, handler);
        }

        @Override
        public <KeyType, ValueType> ActorBehaviorKeyAggregation.ActorBehaviorMatchKeyListFuturePhase<KeyType, ValueType> getListFuturePhase(int matchKeyEntryId, int requiredSize, KeyHistograms.KeyComparator<KeyType> keyComparator, BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType> keyExtractorFromValue, BiConsumer<KeyType, List<ValueType>> handler) {
            return new DebugMatchKeyListFuturePhase<>(matchKeyEntryId, requiredSize, keyComparator, keyValuesReducer, keyExtractorFromValue, handler);
        }
    }

    public static class DebugMatchKey1<KeyType,ValueType1> extends ActorBehaviorKeyAggregation.ActorBehaviorMatchKey1<KeyType,ValueType1> {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);
        public DebugMatchKey1(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1, BiConsumer<KeyType, ValueType1> handler) {
            super(matchKeyEntryId, keyComparator, keyExtractorFromValue1, handler);
        }

        @Override
        public KeyHistograms.HistogramLeafList createEmptyList() {
            return new DebugLeafList();
        }

        @Override
        public boolean processHistogram(MailboxKeyAggregation m) {
            boolean t = checker.before();
            try {
                return super.processHistogram(m);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            boolean t = checker.before();
            try {
                return super.process(self, message);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new DebugNodeLeaf1(key, this, height);
        }
    }

    public static class DebugMatchKey2<KeyType,ValueType1,ValueType2> extends ActorBehaviorKeyAggregation.ActorBehaviorMatchKey2<KeyType,ValueType1,ValueType2> {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);
        public DebugMatchKey2(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2, ActorBehaviorBuilderKeyAggregation.TriConsumer<KeyType, ValueType1, ValueType2> handler) {
            super(matchKeyEntryId, keyComparator, keyExtractorFromValue1, keyExtractorFromValue2, handler);
        }

        @Override
        public KeyHistograms.HistogramLeafList createEmptyList() {
            return new DebugLeafList();
        }

        @Override
        public boolean processHistogram(MailboxKeyAggregation m) {
            boolean t = checker.before();
            try {
                return super.processHistogram(m);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            boolean t = checker.before();
            try {
                return super.process(self, message);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new DebugNodeLeaf2(key, this, height);
        }
    }

    public static class DebugMatchKey3<KeyType,ValueType1,ValueType2,ValueType3> extends ActorBehaviorKeyAggregation.ActorBehaviorMatchKey3<KeyType,ValueType1,ValueType2,ValueType3> {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);
        public DebugMatchKey3(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType3> keyExtractorFromValue3, ActorBehaviorBuilderKeyAggregation.QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> handler) {
            super(matchKeyEntryId, keyComparator, keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3, handler);
        }

        @Override
        public KeyHistograms.HistogramLeafList createEmptyList() {
            return new DebugLeafList();
        }

        @Override
        public boolean processHistogram(MailboxKeyAggregation m) {
            boolean t = checker.before();
            try {
                return super.processHistogram(m);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            boolean t = checker.before();
            try {
                return super.process(self, message);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new DebugNodeLeaf3(key, this, height);
        }
    }

    public static class DebugMatchKey4<KeyType,ValueType1,ValueType2,ValueType3,ValueType4> extends ActorBehaviorKeyAggregation.ActorBehaviorMatchKey4<KeyType,ValueType1,ValueType2,ValueType3,ValueType4> {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);
        public DebugMatchKey4(int matchKeyEntryId, KeyHistograms.KeyComparator<KeyType> keyComparator, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType1> keyExtractorFromValue1, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType2> keyExtractorFromValue2, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType3> keyExtractorFromValue3, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType4> keyExtractorFromValue4, ActorBehaviorBuilderKeyAggregation.QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> handler) {
            super(matchKeyEntryId, keyComparator, keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3, keyExtractorFromValue4, handler);
        }

        @Override
        public KeyHistograms.HistogramLeafList createEmptyList() {
            return new DebugLeafList();
        }

        @Override
        public boolean processHistogram(MailboxKeyAggregation m) {
            boolean t = checker.before();
            try {
                return super.processHistogram(m);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            boolean t = checker.before();
            try {
                return super.process(self, message);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new DebugNodeLeaf4(key, this, height);
        }
    }

    public static class DebugMatchKeyList<KeyType,ValueType> extends ActorBehaviorKeyAggregation.ActorBehaviorMatchKeyList<KeyType,ValueType> {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);

        public DebugMatchKeyList(int matchKeyEntryId, int threshold, KeyHistograms.KeyComparator<KeyType> keyComparator, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType> keyExtractorFromValue, BiConsumer<KeyType, List<ValueType>> handler) {
            super(matchKeyEntryId, threshold, keyComparator, keyExtractorFromValue, handler);
        }

        @Override
        public KeyHistograms.HistogramLeafList createEmptyList() {
            return new DebugLeafList();
        }

        @Override
        public boolean processHistogram(MailboxKeyAggregation m) {
            boolean t = checker.before();
            try {
                return super.processHistogram(m);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            boolean t = checker.before();
            try {
                return super.process(self, message);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new DebugNodeLeafList(key, this, height);
        }
    }

    public static class DebugMatchKeyListFuture<KeyType,ValueType> extends ActorBehaviorKeyAggregation.ActorBehaviorMatchKeyListFuture<KeyType,ValueType> {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);

        public DebugMatchKeyListFuture(int matchKeyEntryId, int requiredSize, KeyHistograms.KeyComparator<KeyType> keyComparator, BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType> keyExtractorFromValue, BiConsumer<KeyType, List<ValueType>> handler) {
            super(matchKeyEntryId, requiredSize, keyComparator, keyValuesReducer, keyExtractorFromValue, handler);
        }

        @Override
        public KeyHistograms.HistogramLeafList createEmptyList() {
            return new DebugLeafList();
        }

        @Override
        public void processTraversal(Actor self, MailboxKeyAggregation.ReducedSize reducedSize, KeyHistograms.HistogramNodeLeaf leaf) {
            boolean t = checker.before();
            try {
                super.processTraversal(self, reducedSize, leaf);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            boolean t = checker.before();
            try {
                return super.process(self, message);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new DebugNodeLeafListReducible(key, this, height);
        }
    }

    public static class DebugMatchKeyListFuturePhase<KeyType,ValueType> extends ActorBehaviorKeyAggregation.ActorBehaviorMatchKeyListFuturePhase<KeyType,ValueType> {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);
        public DebugMatchKeyListFuturePhase(int matchKeyEntryId, int requiredSize, KeyHistograms.KeyComparator<KeyType> keyComparator, BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer, ActorBehaviorBuilderKeyAggregation.KeyExtractor<KeyType, ValueType> keyExtractorFromValue, BiConsumer<KeyType, List<ValueType>> handler) {
            super(matchKeyEntryId, requiredSize, keyComparator, keyValuesReducer, keyExtractorFromValue, handler);
        }

        @Override
        public KeyHistograms.HistogramLeafList createEmptyList() {
            return new DebugLeafList();
        }

        @Override
        public void processTraversal(Actor self, MailboxKeyAggregation.ReducedSize reducedSize, KeyHistograms.HistogramNodeLeaf leaf) {
            boolean t = checker.before();
            try {
                super.processTraversal(self, reducedSize, leaf);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean process(Actor self, Message<?> message) {
            boolean t = checker.before();
            try {
                return super.process(self, message);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public void processPhase(Actor self, Object phaseKey, MailboxKeyAggregation.ReducedSize reducedSize, KeyHistograms.HistogramNodeLeaf leaf) {
            boolean t = checker.before();
            try {
                super.processPhase(self, phaseKey, reducedSize, leaf);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new DebugNodeLeafListReducibleForPhase(key, this, height);
        }
    }

    public static class DebugNodeLeaf1 extends ActorBehaviorKeyAggregation.HistogramNodeLeaf1 {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);
        public DebugNodeLeaf1(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        public KeyHistograms.HistogramNode put(KeyHistograms.KeyComparator<?> comparator, Object key, KeyHistograms.HistogramPutContext context) {
            boolean t = checker.before();
            try {
                return super.put(comparator, key, context);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean consume(KeyHistograms.HistogramTree tree, BiConsumer<Object, Object> handler) {
            boolean t = checker.before();
            try {
                return super.consume(tree, handler);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
    }

    public static class DebugNodeLeaf2 extends ActorBehaviorKeyAggregation.HistogramNodeLeaf2 {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);
        public DebugNodeLeaf2(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        public KeyHistograms.HistogramNode put(KeyHistograms.KeyComparator<?> comparator, Object key, KeyHistograms.HistogramPutContext context) {
            boolean t = checker.before();
            try {
                return super.put(comparator, key, context);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean consume(KeyHistograms.HistogramTree tree, ActorBehaviorBuilderKeyAggregation.TriConsumer<Object, Object, Object> handler) {
            boolean t = checker.before();
            try {
                return super.consume(tree, handler);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
    }

    public static class DebugNodeLeaf3 extends ActorBehaviorKeyAggregation.HistogramNodeLeaf3 {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);
        public DebugNodeLeaf3(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }
        @Override
        public KeyHistograms.HistogramNode put(KeyHistograms.KeyComparator<?> comparator, Object key, KeyHistograms.HistogramPutContext context) {
            boolean t = checker.before();
            try {
                return super.put(comparator, key, context);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean consume(KeyHistograms.HistogramTree tree, ActorBehaviorBuilderKeyAggregation.QuadConsumer<Object, Object, Object, Object> handler) {
            boolean t = checker.before();
            try {
                return super.consume(tree, handler);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
    }

    public static class DebugNodeLeaf4 extends ActorBehaviorKeyAggregation.HistogramNodeLeaf4 {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);
        public DebugNodeLeaf4(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }
        @Override
        public KeyHistograms.HistogramNode put(KeyHistograms.KeyComparator<?> comparator, Object key, KeyHistograms.HistogramPutContext context) {
            boolean t = checker.before();
            try {
                return super.put(comparator, key, context);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean consume(KeyHistograms.HistogramTree tree, ActorBehaviorBuilderKeyAggregation.QuintConsumer<Object, Object, Object, Object, Object> handler) {
            boolean t = checker.before();
            try {
                return super.consume(tree, handler);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
    }

    public static class DebugNodeLeafList extends ActorBehaviorKeyAggregation.HistogramNodeLeafList {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);
        public DebugNodeLeafList(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }
        @Override
        public KeyHistograms.HistogramNode put(KeyHistograms.KeyComparator<?> comparator, Object key, KeyHistograms.HistogramPutContext context) {
            boolean t = checker.before();
            try {
                return super.put(comparator, key, context);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean consume(int requiredSize, KeyHistograms.HistogramTree tree, BiConsumer<Object, List<Object>> handler) {
            boolean t = checker.before();
            try {
                return super.consume(requiredSize, tree, handler);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
    }

    public static class DebugNodeLeafListReducible extends ActorBehaviorKeyAggregation.HistogramNodeLeafListReducible {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);
        public DebugNodeLeafListReducible(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }
        @Override
        public KeyHistograms.HistogramNode put(KeyHistograms.KeyComparator<?> comparator, Object key, KeyHistograms.HistogramPutContext context) {
            boolean t = checker.before();
            try {
                return super.put(comparator, key, context);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean consume(int requiredSize, KeyHistograms.HistogramTree tree, MailboxKeyAggregation.ReducedSize reducedSize, BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer, BiConsumer<Object, List<Object>> handler) {
            boolean t = checker.before();
            try {
                return super.consume(requiredSize, tree, reducedSize, keyValuesReducer, handler);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean consume(int requiredSize, KeyHistograms.HistogramTree tree, BiConsumer<Object, List<Object>> handler) {
            boolean t = checker.before();
            try {
                return super.consume(requiredSize, tree, handler);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
    }

    public static class DebugNodeLeafListReducibleForPhase extends ActorBehaviorKeyAggregation.HistogramNodeLeafListReducibleForPhase {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);
        public DebugNodeLeafListReducibleForPhase(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        public boolean consume(int requiredSize, KeyHistograms.HistogramTree tree, MailboxKeyAggregation.ReducedSize reducedSize, BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer, BiConsumer<Object, List<Object>> handler) {
            boolean t = checker.before();
            try {
                return super.consume(requiredSize, tree, reducedSize, keyValuesReducer, handler);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean consume(int requiredSize, KeyHistograms.HistogramTree tree, BiConsumer<Object, List<Object>> handler) {
            boolean t = checker.before();
            try {
                return super.consume(requiredSize, tree, handler);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }

        @Override
        public boolean consumePhase(int requiredSize, KeyHistograms.HistogramTree tree, MailboxKeyAggregation.ReducedSize reducedSize, BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer, BiConsumer<Object, List<Object>> handler) {
            boolean t = checker.before();
            try {
                return super.consumePhase(requiredSize, tree, reducedSize, keyValuesReducer, handler);
            } catch (Exception ex) {
                checker.error(t, ex);
                throw ex;
            } finally {
                checker.after(t);
            }
        }
    }

    public static class DebugLeafList extends KeyHistograms.HistogramLeafList {
        private final DebugThreadChecker checker = new DebugThreadChecker(this);

        @Override
        public void add(KeyHistograms.HistogramTree tree, Object value) {
            boolean tid = checker.before();
            try {
                super.add(tree, value);
            } catch (Exception e) {
                checker.error(tid, e);
                throw e;
            } finally {
                checker.after(tid);
            }
        }

        @Override
        public Object poll(KeyHistograms.HistogramTree tree) {
            boolean tid = checker.before();
            try {
                return super.poll(tree);
            } catch (Exception e) {
                checker.error(tid, e);
                throw e;
            } finally {
                checker.after(tid);
            }
        }
    }

    public static final class DebugThreadChecker {
        private volatile Thread thread;
        private Object target;
        private StackTraceElement[] lastError;

        public DebugThreadChecker(Object target) {
            this.target = target;
        }

        public boolean before() {
            Thread current = Thread.currentThread();
            Thread t = thread;
            if (t == current) {
                return false; //recursion
            } else if (t == null) {
                thread = current;
                return true;
            } else {
                thread = current;
                lastError = t.getStackTrace();
                return false;
            }
        }

        public boolean after(boolean i) {
            Thread current = Thread.currentThread();
            Thread t = thread;
            if (i) {
                if (t != current) {
                    System.err.println("!!! DebugThreadChecker " + objStr() + ": after error: " + current);
                    print(current);
                    print(t);
                    i = false;
                }
                thread = null;
            } else {
                //error
                if (t == current) {
                    System.err.println("!!! DebugThreadChecker " + objStr() + ": recursion: " + current);
                    print(t);
                } else {
                    System.err.println("!!! DebugThreadChecker " + objStr() + ": before error: " + current + " vs " + t);
                    print(current);
                    print(t);
                }
            }
            return i;
        }
        private String objStr() {
            return target.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(target));
        }

        private void print(Thread t) {
            StringBuilder buf = new StringBuilder();
            if (t != null) {
                buf.append(t).append(":\n");
                for (StackTraceElement e : t.getStackTrace()) {
                    buf.append("  ").append(e).append("\n");
                }
            } else {
                buf.append(" null\n");
            }
            buf.append("==========");
            System.err.println(buf);
        }

        public void error(boolean i, Exception e) {
            Thread current = Thread.currentThread();
            Thread t = thread;
            System.err.println("!!! DebugThreadChecker " + objStr() + ": exception, flag=" + i + " : " + e);
            print(current);
            print(t);
        }

    }
}
