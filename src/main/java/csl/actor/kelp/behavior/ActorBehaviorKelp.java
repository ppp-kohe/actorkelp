package csl.actor.kelp.behavior;

import csl.actor.Actor;
import csl.actor.ActorBehavior;
import csl.actor.Message;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ActorKelpFunctions;
import csl.actor.kelp.ActorKelpFunctions.*;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ActorBehaviorKelp {
    public interface SelectiveDispatcherFactory {
        KelpDispatcher.SelectiveDispatcher createSelectiveDispatcher(ActorKelp<?> self);
    }

    public static abstract class ActorBehaviorMatchKey<KeyType> extends KeyHistograms.HistogramPutContext
            implements ActorBehavior, HistogramProcessor, SelectiveDispatcherFactory {
        protected int matchKeyEntryId;
        protected KeyComparator<KeyType> keyComparator;
        protected ActorKelpFunctions.DispatcherFactory dispatcher;

        public ActorBehaviorMatchKey(int matchKeyEntryId, int requiredSize, KeyComparator<KeyType> keyComparator) {
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
        public KeyComparator<?> getKeyComparator() {
            return keyComparator;
        }

        public abstract List<KeyExtractor<KeyType,?>> getKeyExtractors();
        public abstract Object getHandler();

        public ActorKelpFunctions.DispatcherFactory getDispatcher() {
            return dispatcher;
        }

        public void setDispatcher(ActorKelpFunctions.DispatcherFactory dispatcher) {
            this.dispatcher = dispatcher;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public KelpDispatcher.SelectiveDispatcher createSelectiveDispatcher(ActorKelp<?> self) {
            return new KelpDispatcher.SelectiveDispatcher((List<KeyExtractor<?,?>>) (List) getKeyExtractors(),
                    self.createDispatcher(getDispatcher()));
        }
    }

    public static class ActorBehaviorMatchKey1<KeyType, ParamType1, ValueType1> extends ActorBehaviorMatchKey<KeyType> {
        protected KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1;
        protected Function<ParamType1, ValueType1> valueExtractorFromValue1;

        protected BiConsumer<KeyType, ValueType1> handler;

        public ActorBehaviorMatchKey1(int matchKeyEntryId, KeyComparator<KeyType> keyComparator,
                                      KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
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
                if (next.consume(tree, (BiConsumer<Object,Object>) handler)) {
                    if (tree.getTreeSize() == 0) {
                        tree.prune(); //always prune for clearing root node
                    }
                    return true;
                }
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
        public List<KeyExtractor<KeyType,?>> getKeyExtractors() {
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

        @Override
        public HistogramNodeLeafN copy(Map<KeyHistograms.HistogramNode, KeyHistograms.HistogramNode> oldToNew) {
            return (HistogramNodeLeafN) super.copy(oldToNew);
        }
    }


    public static class HistogramNodeLeaf1 extends HistogramNodeLeafN {
        public static final long serialVersionUID = 1L;
        public KeyHistograms.HistogramLeafList values1;

        public HistogramNodeLeaf1(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        public HistogramNodeLeaf1 copy(Map<KeyHistograms.HistogramNode, KeyHistograms.HistogramNode> oldToNew) {
            HistogramNodeLeaf1 node = (HistogramNodeLeaf1) super.copy(oldToNew);
            node.values1 = (values1 == null ? null : values1.copy());
            return node;
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
        protected KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1;
        protected KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2;
        protected Function<ParamType1, ValueType1> valueExtractorFromValue1;
        protected Function<ParamType2, ValueType2> valueExtractorFromValue2;

        protected TriConsumer<KeyType, ValueType1, ValueType2> handler;


        public ActorBehaviorMatchKey2(int matchKeyEntryId, KeyComparator<KeyType> keyComparator,
                                      KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                      KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2,
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
        public List<KeyExtractor<KeyType,?>> getKeyExtractors() {
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
        public KeyHistograms.HistogramLeafList values1;
        public KeyHistograms.HistogramLeafList values2;

        public HistogramNodeLeaf2(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        public HistogramNodeLeaf2 copy(Map<KeyHistograms.HistogramNode, KeyHistograms.HistogramNode> oldToNew) {
            HistogramNodeLeaf2 node = (HistogramNodeLeaf2) super.copy(oldToNew);
            node.values1 = (values1 == null ? null : values1.copy());
            node.values2 = (values2 == null ? null : values2.copy());
            return node;
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
        protected KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1;
        protected KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2;
        protected KeyExtractor<KeyType, ParamType3> keyExtractorFromValue3;
        protected Function<ParamType1, ValueType1> valueExtractorFromValue1;
        protected Function<ParamType2, ValueType2> valueExtractorFromValue2;
        protected Function<ParamType3, ValueType3> valueExtractorFromValue3;

        protected QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> handler;

        public ActorBehaviorMatchKey3(int matchKeyEntryId, KeyComparator<KeyType> keyComparator,
                                      KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                      KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2,
                                      KeyExtractor<KeyType, ParamType3> keyExtractorFromValue3,
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
        public List<KeyExtractor<KeyType,?>> getKeyExtractors() {
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
        public KeyHistograms.HistogramLeafList values1;
        public KeyHistograms.HistogramLeafList values2;
        public KeyHistograms.HistogramLeafList values3;

        public HistogramNodeLeaf3(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        public HistogramNodeLeaf3 copy(Map<KeyHistograms.HistogramNode, KeyHistograms.HistogramNode> oldToNew) {
            HistogramNodeLeaf3 node = (HistogramNodeLeaf3) super.copy(oldToNew);
            node.values1 = (values1 == null ? null : values1.copy());
            node.values2 = (values2 == null ? null : values2.copy());
            node.values3 = (values3 == null ? null : values3.copy());
            return node;
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
        protected KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1;
        protected KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2;
        protected KeyExtractor<KeyType, ParamType3> keyExtractorFromValue3;
        protected KeyExtractor<KeyType, ParamType4> keyExtractorFromValue4;
        protected Function<ParamType1, ValueType1> valueExtractorFromValue1;
        protected Function<ParamType2, ValueType2> valueExtractorFromValue2;
        protected Function<ParamType3, ValueType3> valueExtractorFromValue3;
        protected Function<ParamType4, ValueType4> valueExtractorFromValue4;

        protected QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> handler;

        public ActorBehaviorMatchKey4(int matchKeyEntryId, KeyComparator<KeyType> keyComparator,
                                      KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                      KeyExtractor<KeyType, ParamType2> keyExtractorFromValue2,
                                      KeyExtractor<KeyType, ParamType3> keyExtractorFromValue3,
                                      KeyExtractor<KeyType, ParamType4> keyExtractorFromValue4,
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
        public List<KeyExtractor<KeyType,?>> getKeyExtractors() {
            return Arrays.asList(keyExtractorFromValue1, keyExtractorFromValue2, keyExtractorFromValue3, keyExtractorFromValue4);
        }

        /** @return implementation field getter */
        public QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> getHandler() {
            return handler;
        }
    }

    public static class HistogramNodeLeaf4 extends HistogramNodeLeafN {
        public static final long serialVersionUID = 1L;
        public KeyHistograms.HistogramLeafList values1;
        public KeyHistograms.HistogramLeafList values2;
        public KeyHistograms.HistogramLeafList values3;
        public KeyHistograms.HistogramLeafList values4;

        public HistogramNodeLeaf4(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        public HistogramNodeLeaf4 copy(Map<KeyHistograms.HistogramNode, KeyHistograms.HistogramNode> oldToNew) {
            HistogramNodeLeaf4 node = (HistogramNodeLeaf4) super.copy(oldToNew);
            node.values1 = (values1 == null ? null : values1.copy());
            node.values2 = (values2 == null ? null : values2.copy());
            node.values3 = (values3 == null ? null : values3.copy());
            node.values4 = (values4 == null ? null : values4.copy());
            return node;
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
        protected KeyExtractor<KeyType, ParamType> keyExtractorFromValue;
        protected Function<ParamType, ValueType> valueExtractorFromValue;
        protected BiConsumer<KeyType, List<ValueType>> handler;

        public ActorBehaviorMatchKeyList(int matchKeyEntryId, int threshold, KeyComparator<KeyType> keyComparator,
                                         KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
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
                        new KeyValuesReducerList<>(keyValuesReducers), keyExtractorFromValue, valueExtractorFromValue, handler);
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
        public List<KeyExtractor<KeyType,?>> getKeyExtractors() {
            return Collections.singletonList(keyExtractorFromValue);
        }

        /** @return implementation field getter */
        public BiConsumer<KeyType, List<ValueType>> getHandler() {
            return handler;
        }
    }

    public static class HistogramNodeLeafList extends HistogramNodeLeafN {
        public static final long serialVersionUID = 1L;
        public KeyHistograms.HistogramLeafList values;
        public HistogramNodeLeafList(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }

        @Override
        public HistogramNodeLeafList copy(Map<KeyHistograms.HistogramNode, KeyHistograms.HistogramNode> oldToNew) {
            HistogramNodeLeafList node = (HistogramNodeLeafList) super.copy(oldToNew);
            node.values = (values == null ? null : values.copy());
            return node;
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
        protected KeyExtractor<KeyType, ParamType> keyExtractorFromValue;
        protected Function<ParamType, ValueType> valueExtractorFromValue;
        protected BiConsumer<KeyType, List<ValueType>> handler;

        public ActorBehaviorMatchKeyListFuture(int matchKeyEntryId, int requiredSize,
                                               KeyComparator<KeyType> keyComparator,
                                               BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                               KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                               Function<ParamType, ValueType> valueExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler) {
            super(matchKeyEntryId, requiredSize, keyComparator);
            this.keyValuesReducer = keyValuesReducer;
            this.keyExtractorFromValue = keyExtractorFromValue;
            this.valueExtractorFromValue = valueExtractorFromValue;
            this.handler = handler;
        }

        @Override
        public List<KeyExtractor<KeyType, ?>> getKeyExtractors() {
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
                self.tell(new HistogramEntry.TraversalProcess(matchKeyEntryId), self);
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
        public HistogramNodeLeafListReducible copy(Map<KeyHistograms.HistogramNode, KeyHistograms.HistogramNode> oldToNew) {
            HistogramNodeLeafListReducible node = (HistogramNodeLeafListReducible) super.copy(oldToNew);
            return node;
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

    public static class ActorBehaviorMatchKeyListFutureStageEnd<KeyType, ParamType, ValueType>
            extends ActorBehaviorMatchKeyListFuture<KeyType, ParamType, ValueType> {

        public ActorBehaviorMatchKeyListFutureStageEnd(int matchKeyEntryId, int requiredSize, KeyComparator<KeyType> keyComparator,
                                                       BiFunction<KeyType, List<ValueType>, Iterable<ValueType>> keyValuesReducer,
                                                       KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                                       Function<ParamType, ValueType> valueExtractorFromValue,
                                                       BiConsumer<KeyType, List<ValueType>> handler) {
            super(matchKeyEntryId, requiredSize, keyComparator, keyValuesReducer, keyExtractorFromValue, valueExtractorFromValue, handler);
        }

        @Override
        protected KeyHistograms.HistogramNodeLeaf createLeaf(Object key, int height) {
            return new HistogramNodeLeafListReducibleForStageEnd(key, this, height);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void processTraversal(Actor self, MailboxKelp.ReducedSize reducedSize, KeyHistograms.HistogramNodeLeaf leaf) {
            HistogramNodeLeafListReducible list = completedLeaf(putRequiredSize + 1, HistogramNodeLeafListReducible.class, leaf);
            if (list != null && list.consume(putRequiredSize + 1, putTree, reducedSize, (BiFunction) keyValuesReducer, (BiConsumer) handler)) {
                self.tell(new HistogramEntry.TraversalProcess(matchKeyEntryId), self);
            }
        }

        @Override
        public boolean needToProcessStageEnd(Actor self, Object stageKey, KeyHistograms.HistogramTree tree) {
            this.putTree = tree;
            return true;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void processStageEnd(Actor self, Object stageKey, MailboxKelp.ReducedSize reducedSize, KeyHistograms.HistogramNodeLeaf leaf) {
            long prevSize = leaf.size();
            HistogramNodeLeafListReducibleForStageEnd list = completedLeaf(putRequiredSize, HistogramNodeLeafListReducibleForStageEnd.class, leaf);
            if (list != null) {
                while (list.consumeStageEnd(putRequiredSize, putTree, reducedSize, (BiFunction) keyValuesReducer, (BiConsumer) handler)) {
                    if (prevSize <= leaf.size()) { //no consumption
                        break;
                    }
                }
            }
        }
    }

    public static class HistogramNodeLeafListReducibleForStageEnd extends HistogramNodeLeafListReducible {
        public static final long serialVersionUID = 1L;
        public HistogramNodeLeafListReducibleForStageEnd(Object key, KeyHistograms.HistogramPutContext context, int height) {
            super(key, context, height);
        }
        @Override
        public HistogramNodeLeafListReducibleForStageEnd copy(Map<KeyHistograms.HistogramNode, KeyHistograms.HistogramNode> oldToNew) {
            HistogramNodeLeafListReducibleForStageEnd node = (HistogramNodeLeafListReducibleForStageEnd) super.copy(oldToNew);
            return node;
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

        public boolean consumeStageEnd(int requiredSize,
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
