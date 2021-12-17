package csl.actor.kelp.behavior;

import csl.actor.Actor;
import csl.actor.ActorBehavior;
import csl.actor.Message;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ActorKelpFunctions;
import csl.actor.kelp.ActorKelpFunctions.*;
import csl.actor.kelp.persist.HistogramTreePersistable;
import csl.actor.kelp.persist.KeyHistogramsPersistable;
import csl.actor.kelp.persist.PersistentConditionActor;
import csl.actor.kelp.persist.TreeMerger;
import csl.actor.persist.PersistentFileManager;
import csl.actor.util.ObjectsList;

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
        protected Class<KeyType> keyType;

        public ActorBehaviorMatchKey(int matchKeyEntryId, int requiredSize, KeyComparator<KeyType> keyComparator,
                                     Class<KeyType> keyType) {
            this.putRequiredSize = requiredSize;
            this.matchKeyEntryId = matchKeyEntryId;
            this.keyComparator = keyComparator;
            this.keyType = keyType;
        }

        protected void put(Actor self, KeyType key, Comparable<?> position, Object value) {
            MailboxKelp m = (MailboxKelp) self.getMailbox();
            this.putActor = self;
            this.putPosition = position;
            this.putValue = value;
            m.getHistogram(matchKeyEntryId).put(key, this); //putTree will be set
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

        @Override
        public Class<?> keyType() {
            return keyType == null ? Object.class : keyType;
        }

        protected Map<Object, Class<?>> createValueTypesForPositions(Class<?>... types) {
            Map<Object,Class<?>> m = new HashMap<>();
            for (int i = 0; i < types.length; ++i) {
                Class<?> c = types[i];
                if (c != null) {
                    m.put(i, c);
                } else {
                    m.put(i, Object.class);
                }
            }
            return m;
        }
    }

    public static class ActorBehaviorMatchKey1<KeyType, ParamType1, ValueType1> extends ActorBehaviorMatchKey<KeyType> {
        protected KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1;
        protected Function<ParamType1, ValueType1> valueExtractorFromValue1;

        protected BiConsumer<KeyType, ValueType1> handler;
        protected Class<ValueType1> valueType;


        public ActorBehaviorMatchKey1(int matchKeyEntryId, KeyComparator<KeyType> keyComparator,
                                      KeyExtractor<KeyType, ParamType1> keyExtractorFromValue1,
                                      Function<ParamType1, ValueType1> valueExtractorFromValue1,
                                      BiConsumer<KeyType, ValueType1> handler, Class<KeyType> keyType, Class<ValueType1> valueType) {
            super(matchKeyEntryId, 1, keyComparator, keyType);
            this.keyExtractorFromValue1 = keyExtractorFromValue1;
            this.valueExtractorFromValue1 = valueExtractorFromValue1;
            this.handler = handler;
            this.valueType = valueType;
        }

        @Override
        public Map<Object, Class<?>> valueTypesForPositions() {
            return createValueTypesForPositions(valueType);
        }

        @Override
        protected HistogramTreeNodeLeaf createLeaf(Object key) {
            return new HistogramNodeLeaf1(key, this);
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean process(Actor self, Message<?> message) {
            Object value = message.getData();
            KeyType key;
            //Comparable<?> pos;
            if (keyExtractorFromValue1.matchValue(value)) {
                key = keyExtractorFromValue1.toKey((ParamType1) value);
                //pos = 0;
                value = valueExtractorFromValue1.apply((ParamType1) value);
            } else {
                return false;
            }
            /*
            put(self, key, pos, value);
            if (putTree.getTreeSize() == 0) {
                putTree.prune(); //always prune for clearing root node
            }*/
            handler.accept(key, (ValueType1) value); //optimize for no-using tree
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void complete(HistogramTreeNodeLeaf leaf) {
            ((HistogramNodeLeaf1) leaf).consume(putTree, (BiConsumer<Object, Object>) handler);
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

    public static abstract class HistogramNodeLeafN extends HistogramTreeNodeLeaf {
        public static final long serialVersionUID = 1L;

        public HistogramNodeLeafN() {}

        public HistogramNodeLeafN(Object key, KeyHistograms.HistogramPutContext context) {
            super(key, context);
        }

        @Override
        public HistogramNodeLeafN copy(Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> oldToNew) {
            return (HistogramNodeLeafN) super.copy(oldToNew);
        }
    }


    public static class HistogramNodeLeaf1 extends HistogramNodeLeafN {
        public static final long serialVersionUID = 1L;
        public KeyHistograms.HistogramLeafList values1;

        public HistogramNodeLeaf1() {}

        public HistogramNodeLeaf1(Object key, KeyHistograms.HistogramPutContext context) {
            super(key, context);
        }

        @Override
        public HistogramNodeLeaf1 copy(Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> oldToNew) {
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

        public boolean consume(HistogramTree tree, BiConsumer<Object, Object> handler) {
            if (completedAfterPut(null)) {  //currently, it can complete before consume, and then it might not be able to consume 2 or more times
                afterTake(1, tree);
                handler.accept(getKey(), values1.poll(tree, 0, this));
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
                                      TriConsumer<KeyType, ValueType1, ValueType2> handler, Class<KeyType> keyType) {
            super(matchKeyEntryId, 2, keyComparator, keyType);
            this.keyExtractorFromValue1 = keyExtractorFromValue1;
            this.keyExtractorFromValue2 = keyExtractorFromValue2;
            this.valueExtractorFromValue1 = valueExtractorFromValue1;
            this.valueExtractorFromValue2 = valueExtractorFromValue2;
            this.handler = handler;
        }

        @Override
        public Map<Object, Class<?>> valueTypesForPositions() {
            return createValueTypesForPositions(null, null);
        }

        @Override
        protected HistogramTreeNodeLeaf createLeaf(Object key) {
            return new HistogramNodeLeaf2(key, this);
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
        public void complete(HistogramTreeNodeLeaf leaf) {
            ((HistogramNodeLeaf2) leaf).consume(putTree, (TriConsumer<Object, Object ,Object>) handler);
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

        public HistogramNodeLeaf2() {}

        public HistogramNodeLeaf2(Object key, KeyHistograms.HistogramPutContext context) {
            super(key, context);
        }

        @Override
        public HistogramNodeLeaf2 copy(Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> oldToNew) {
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

        public boolean consume(HistogramTree tree, TriConsumer<Object, Object, Object> handler) {
            if (completedAfterPut(null)) {  //currently, it can complete before consume, and then it might not be able to consume 2 or more times
                afterTake(2, tree);
                handler.accept(getKey(), values1.poll(tree, 0, this), values2.poll(tree, 1, this));
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
                                      QuadConsumer<KeyType, ValueType1, ValueType2, ValueType3> handler, Class<KeyType> keyType) {
            super(matchKeyEntryId, 3, keyComparator, keyType);
            this.keyExtractorFromValue1 = keyExtractorFromValue1;
            this.keyExtractorFromValue2 = keyExtractorFromValue2;
            this.keyExtractorFromValue3 = keyExtractorFromValue3;
            this.valueExtractorFromValue1 = valueExtractorFromValue1;
            this.valueExtractorFromValue2 = valueExtractorFromValue2;
            this.valueExtractorFromValue3 = valueExtractorFromValue3;
            this.handler = handler;
        }

        @Override
        public Map<Object, Class<?>> valueTypesForPositions() {
            return createValueTypesForPositions(null, null, null);
        }

        @Override
        protected HistogramTreeNodeLeaf createLeaf(Object key) {
            return new HistogramNodeLeaf3(key, this);
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
        public void complete(HistogramTreeNodeLeaf leaf) {
            ((HistogramNodeLeaf3) leaf).consume(putTree, (QuadConsumer<Object,Object,Object,Object>) handler);
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

        public HistogramNodeLeaf3() {}

        public HistogramNodeLeaf3(Object key, KeyHistograms.HistogramPutContext context) {
            super(key, context);
        }

        @Override
        public HistogramNodeLeaf3 copy(Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> oldToNew) {
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

        public boolean consume(HistogramTree tree, QuadConsumer<Object, Object, Object, Object> handler) {
            if (completedAfterPut(null)) {
                afterTake(3, tree);
                handler.accept(getKey(), values1.poll(tree, 0, this), values2.poll(tree, 1, this), values3.poll(tree, 2,this));
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
                                      QuintConsumer<KeyType, ValueType1, ValueType2, ValueType3, ValueType4> handler,
                                      Class<KeyType> keyType) {
            super(matchKeyEntryId, 4, keyComparator, keyType);
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
        public Map<Object, Class<?>> valueTypesForPositions() {
            return createValueTypesForPositions(null, null, null, null);
        }

        @Override
        protected HistogramTreeNodeLeaf createLeaf(Object key) {
            return new HistogramNodeLeaf4(key, this);
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
        public void complete(HistogramTreeNodeLeaf leaf) {
            ((HistogramNodeLeaf4) leaf).consume(putTree, (QuintConsumer<Object, Object,Object,Object,Object>) handler);
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

        public HistogramNodeLeaf4() {}

        public HistogramNodeLeaf4(Object key, KeyHistograms.HistogramPutContext context) {
            super(key, context);
        }

        @Override
        public HistogramNodeLeaf4 copy(Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> oldToNew) {
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

        public boolean consume(HistogramTree tree, QuintConsumer<Object, Object, Object, Object, Object> handler) {
            if (completedAfterPut(null)) {
                afterTake(4, tree);
                handler.accept(getKey(), values1.poll(tree, 0, this), values2.poll(tree, 1, this), values3.poll(tree, 2, this), values4.poll(tree, 3,this));
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

        protected Class<ValueType> valueType;

        public ActorBehaviorMatchKeyList(int matchKeyEntryId, int threshold, KeyComparator<KeyType> keyComparator,
                                         KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                         Function<ParamType, ValueType> valueExtractorFromValue,
                                         BiConsumer<KeyType, List<ValueType>> handler,
                                         Class<KeyType> keyType, Class<ValueType> valueType) {
            super(matchKeyEntryId, threshold, keyComparator, keyType);
            this.keyExtractorFromValue = keyExtractorFromValue;
            this.valueExtractorFromValue = valueExtractorFromValue;
            this.handler = handler;
            this.valueType = valueType;
        }

        @Override
        public Map<Object, Class<?>> valueTypesForPositions() {
            return createValueTypesForPositions(valueType);
        }

        public ActorBehaviorMatchKey<KeyType> withKeyValuesReducers(List<KeyValuesReducer<KeyType,ValueType>> keyValuesReducers) {
            if (keyValuesReducers.isEmpty()) {
                return this;
            } else {
                return new ActorBehaviorMatchKeyListFuture<>(matchKeyEntryId, this.putRequiredSize, keyComparator,
                        new KeyValuesReducerList<>(keyValuesReducers), keyExtractorFromValue, valueExtractorFromValue, handler,
                        keyType, valueType);
            }
        }

        @Override
        protected HistogramTreeNodeLeaf createLeaf(Object key) {
            return new HistogramNodeLeafList(key, this);
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
            put(self, key, 0, value);
            return true;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void complete(HistogramTreeNodeLeaf leaf) {
            ((HistogramNodeLeafList) leaf).consume(putRequiredSize, putTree, 0, (BiConsumer) handler);
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

        public HistogramNodeLeafList() {}

        public HistogramNodeLeafList(Object key, KeyHistograms.HistogramPutContext context) {
            super(key, context);
        }

        @Override
        public HistogramNodeLeafList copy(Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> oldToNew) {
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

        public boolean consume(int requiredSize, HistogramTree tree, Object position, BiConsumer<Object,List<Object>> handler) {
            if (completed(requiredSize)) {
                ObjectsList vs = new ObjectsList(requiredSize);
                for (int i = 0; i < requiredSize; ++i) {
                    vs.add(values.poll(tree, position, this));
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
        protected KeyValuesReducer<KeyType, ValueType> keyValuesReducer;
        protected KeyExtractor<KeyType, ParamType> keyExtractorFromValue;
        protected Function<ParamType, ValueType> valueExtractorFromValue;
        protected BiConsumer<KeyType, List<ValueType>> handler;
        protected Class<ValueType> valueType;

        public ActorBehaviorMatchKeyListFuture(int matchKeyEntryId, int requiredSize,
                                               KeyComparator<KeyType> keyComparator,
                                               KeyValuesReducer<KeyType, ValueType> keyValuesReducer,
                                               KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                               Function<ParamType, ValueType> valueExtractorFromValue,
                                               BiConsumer<KeyType, List<ValueType>> handler,
                                               Class<KeyType> keyType, Class<ValueType> valueType) {
            super(matchKeyEntryId, requiredSize, keyComparator, keyType);
            this.keyValuesReducer = keyValuesReducer;
            this.keyExtractorFromValue = keyExtractorFromValue;
            this.valueExtractorFromValue = valueExtractorFromValue;
            this.handler = handler;
            this.valueType = valueType;
        }

        @Override
        public Map<Object, Class<?>> valueTypesForPositions() {
            return createValueTypesForPositions(valueType);
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
            mailbox.processPersistableTraversalBeforePut(self, matchKeyEntryId, getReducedSize());
            put(self, key, 0, value);
//            mailbox.updateScheduledTraversalProcess(self, this.matchKeyEntryId);
            return true;
        }

        @Override
        protected HistogramTreeNodeLeaf createLeaf(Object key) {
            return new HistogramNodeLeafListReducible(key, this);
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
        public boolean needToProcessTraversal(Actor self, HistogramTree tree, MailboxKelp.ReducedSize reducedSize, boolean reserved) {
            this.putTree = tree;
            if (reserved) {
                return true;
            } else {
                boolean allowPersist = reduceForPersistedData();
                return reducedSize.needToReduceForTraversal(allowPersist, tree.getTreeSize(), tree.getTreeSizeOnMemory(), tree);
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void processTraversal(Actor self, MailboxKelp.ReducedSize reducedSize, HistogramTreeNodeLeaf leaf) {
            HistogramNodeLeafListReducible list = completedLeaf(putRequiredSize, getListType(), leaf);
            if (list != null && list.consume(putRequiredSize, keyValuesReducer.requiredSize(), putTree, 0, reducedSize, false, (BiFunction) keyValuesReducer, (BiConsumer) handler)) {
                ((ActorKelp<?>) self).getMailboxAsKelp().reserveTraversal(self, matchKeyEntryId);
            }
        }

        @SuppressWarnings("unchecked")
        public <LeafType extends HistogramTreeNodeLeaf> LeafType completedLeaf(int requiredSize, Class<LeafType> type,
                                                                               HistogramTreeNodeLeaf l) {
            if (l.size() >= requiredSize) {
                return loadLeaf(type, l);
            } else {
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        public <LeafType extends HistogramTreeNodeLeaf> LeafType loadLeaf(Class<LeafType> type,
                                                                          HistogramTreeNodeLeaf l) {
            if (!type.isInstance(l)) {
                return (LeafType) l.load(this);
            } else {
                return (LeafType) l;
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void complete(HistogramTreeNodeLeaf leaf) {
            if (!complete(leaf, false)) { //first try on memory
                if (reduceForPersistedData()) {  //if failed, retry with persisted loading
                    complete(leaf, true);
                }
            }
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public boolean reduceOnMemory(HistogramTreeNodeLeaf leaf) {
            HistogramNodeLeafListReducible list = leafToReducible(leaf, false);
            return (list != null &&
                    list.consume(putRequiredSize, keyValuesReducer.requiredSize(), putTree, 0, getReducedSize(), true,
                            (BiFunction) keyValuesReducer, (BiConsumer) handler));
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        protected boolean complete(HistogramTreeNodeLeaf leaf, boolean allowPersist) {
            int reduceReq = keyValuesReducer.requiredSize();
            long sizeChecked = allowPersist ? leaf.size() : leaf.sizeOnMemory();
            MailboxKelp.ReducedSize reducedSize = getReducedSize();
            if (reducedSize.needToReduceForComplete(allowPersist, sizeChecked, reduceReq)) {
                HistogramNodeLeafListReducible list = leafToReducible(leaf, allowPersist);
                if (list != null &&
                        list.consume(putRequiredSize, keyValuesReducer.requiredSize(), putTree, 0, reducedSize, !allowPersist,
                                (BiFunction) keyValuesReducer, (BiConsumer) handler)) {
                    if (putActor != null) {
                        ((ActorKelp<?>) putActor).getMailboxAsKelp().reserveTraversal(putActor, matchKeyEntryId);
                    }
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        protected HistogramNodeLeafListReducible leafToReducible(HistogramTreeNodeLeaf leaf, boolean allowPersist) {
            if (allowPersist) {
                return loadLeaf(HistogramNodeLeafListReducible.class, leaf);
            } else {
                return (leaf instanceof HistogramNodeLeafListReducible ? (HistogramNodeLeafListReducible) leaf : null);
            }
        }

        public boolean reduceForPersistedData() {
            return true;
        }

        public MailboxKelp.ReducedSize getReducedSize() {
            if (putActor instanceof ActorKelp<?>) {
                return ((ActorKelp<?>) putActor).getReducedSize();
            } else {
                return PersistentConditionActor.DEFAULT_REDUCED_SIZE;
            }
        }

        public Class<? extends HistogramNodeLeafListReducible> getListType() {
            return HistogramNodeLeafListReducible.class;
        }

        @Override
        public boolean needToProcessStageEnd(Actor self, MailboxKelp.ReducedSize reducedSize, Object stageKey, HistogramTree tree) {
            return true;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void processStageEnd(Actor self, Object stageKey, MailboxKelp.ReducedSize reducedSize, HistogramTreeNodeLeaf leaf) {
            long prevSize = leaf.size();
            HistogramNodeLeafListReducible list = completedLeaf(putRequiredSize, getListType(), leaf);
            if (list != null) {
                list.consumeStageEndReduceAll(putRequiredSize, keyValuesReducer.requiredSize(), putTree, 0, reducedSize, (BiFunction) keyValuesReducer, (BiConsumer) handler);

                while (list.consumeStageEnd(putRequiredSize, keyValuesReducer.requiredSize(), putTree, 0, reducedSize, (BiFunction) keyValuesReducer, (BiConsumer) handler)) {
                    if (prevSize <= leaf.size()) { //no consumption
                        break;
                    }
                }
            }
        }
    }

    public static class HistogramNodeLeafListReducible extends HistogramNodeLeafList {
        public static final long serialVersionUID = 1L;

        public HistogramNodeLeafListReducible() {}

        public HistogramNodeLeafListReducible(Object key, KeyHistograms.HistogramPutContext context) {
            super(key, context);
        }

        @Override
        public HistogramNodeLeafListReducible copy(Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> oldToNew) {
            HistogramNodeLeafListReducible node = (HistogramNodeLeafListReducible) super.copy(oldToNew);
            return node;
        }

        @Override
        protected boolean completedAfterPut(KeyHistograms.HistogramPutContext context) {
            return true;
        }

        public boolean consume(int requiredSize,
                               int reduceRequiredSize,
                               HistogramTree tree,
                               Object position,
                               MailboxKelp.ReducedSize reducedSize,
                               boolean onMemory,
                               BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer,
                               BiConsumer<Object, List<Object>> handler) {
            if (completed(requiredSize)) {
                List<Object> vs = poll(requiredSize, tree, position, reducedSize, onMemory);
                int consuming = vs.size();
                boolean changed;
                try {
                    ReduceResult res = reduceAndHandle(requiredSize, reduceRequiredSize, tree, reducedSize, keyValuesReducer, handler, vs);
                    consuming = res.sizeRemoved();
                    changed = res.isChanged();
                } finally {
                    afterTake(consuming, tree);
                }
                return changed && completed(requiredSize);
            } else {
                return false;
            }
        }

        protected List<Object> poll(int requiredSize, HistogramTree tree, Object position, MailboxKelp.ReducedSize reducedSize,
                                    boolean onMemory) {
            int consuming = Math.max(requiredSize, reducedSize.nextReducedSize(size()));
            Object[] vs = new Object[consuming];
            try {
                int len = values.polls(tree, position, this, consuming, vs, onMemory);
                return new ObjectsList(vs, len);
            } catch (Exception ex) {
                throw new RuntimeException(String.format("size=%,d, consuming=%,d required=%,d", size(), consuming, requiredSize), ex);
            }
        }

        protected ReduceResult reduceAndHandle(int requiredSize, int reduceRequiredSize,
                                      HistogramTree tree,
                                      MailboxKelp.ReducedSize reducedSize,
                                      BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer,
                                      BiConsumer<Object, List<Object>> handler,
                                      List<Object> vs) {
            int consuming = vs.size();
            Object key = getKey();
            List<Object> rs = reduceRequiredSize <= vs.size() ?
                    toList(keyValuesReducer.apply(key, vs)) : vs;
            int reduced = rs.size();
            int handled = 0;
            if (requiredSize <= rs.size()) {
                handler.accept(key, rs);
                handled = rs.size();
            } else {
                for (Object r : rs) {
                    values.add(tree, r);
                }
            }
            return new ReduceResult(consuming, reduced, handled);
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

        public void consumeStageEndReduceAll(int requiredSize,
                                             int reduceRequiredSize,
                                             HistogramTree tree,
                                             Object position,
                                             MailboxKelp.ReducedSize reducedSize,
                                             BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer,
                                             BiConsumer<Object, List<Object>> handler) {
            consumeStageEndReduceAll(requiredSize, reduceRequiredSize, tree, position, reducedSize,  keyValuesReducer, handler, true);
        }

        public void consumeStageEndReduceAll(int requiredSize,
                                             int reduceRequiredSize,
                                             HistogramTree tree,
                                             Object position,
                                             MailboxKelp.ReducedSize reducedSize,
                                             BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer,
                                             BiConsumer<Object, List<Object>> handler, boolean modifyTree) {
            long sizeRemoved = 0;
            long totalConsumed = 0;
            long currentSize = size();
            long startSize = currentSize;
            try {
                while (currentSize >= reduceRequiredSize) {
                    List<Object> vs = poll(requiredSize, tree, position, reducedSize, false);
                    ReduceResult res = reduce(reduceRequiredSize, tree, keyValuesReducer, vs);
                    totalConsumed += res.consumed;
                    sizeRemoved += res.sizeRemoved();
                    currentSize -= res.sizeRemoved();
                    if (!res.isChanged() && totalConsumed >= startSize) { //all items are reduced at least once
                        break;
                    }
                }
            } finally {
                if (modifyTree) {
                    afterTake(sizeRemoved, tree);
                }
            }
        }

        public boolean consumeStageEnd(int requiredSize,
                                       int reduceRequiredSize,
                                       HistogramTree tree,
                                       Object position,
                                       MailboxKelp.ReducedSize reducedSize,
                                       BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer,
                                       BiConsumer<Object, List<Object>> handler) {
            return consumeStageEnd(requiredSize, reduceRequiredSize, tree, position, reducedSize, keyValuesReducer, handler, true);
        }

        public boolean consumeStageEnd(int requiredSize,
                                       int reduceRequiredSize,
                                       HistogramTree tree,
                                       Object position,
                                       MailboxKelp.ReducedSize reducedSize,
                                       BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer,
                                       BiConsumer<Object, List<Object>> handler, boolean modifyTree) {
            List<Object> vs = poll(requiredSize, tree, position, reducedSize, false);
            int consuming = vs.size();
            boolean changed = consuming > 0;
            try {
                if (requiredSize <= vs.size()) {
                    handler.accept(key, vs);
                }
            } finally {
                if (modifyTree) {
                    afterTake(consuming, tree);
                }
            }
            return changed && completed(requiredSize);
        }

        public ReduceResult reduce(int reduceRequiredSize, HistogramTree tree,
                                      BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer,
                                      List<Object> vs) {
            Object key = getKey();
            int consuming = vs.size();
            Iterable<Object> rs = reduceRequiredSize <= vs.size() ?
                    keyValuesReducer.apply(key, vs) : vs;
            int reducedSize = 0;
            for (Object r : rs) {
                values.add(tree, r);
                ++reducedSize;
            }
            return new ReduceResult(consuming, reducedSize, 0);
        }
    }

    public static class ReduceResult {
        public final int consumed;
        public final int reducedSize;
        public final int handled;

        public ReduceResult(int consumed, int reduced, int handled) {
            this.consumed = consumed;
            this.reducedSize = reduced;
            this.handled = handled;
        }

        public boolean isChanged() {
            return !(handled == 0 && //no handling
                     consumed == reducedSize); //and no reducing
        }

        public int sizeRemoved() {
            return consumed  //first consume from the list,
                    - (reducedSize //next, reduced the consumed items
                        - handled); //non-handled values are back to the list
        }
    }

    public static class ActorBehaviorMatchKeyListFutureStageEnd<KeyType, ParamType, ValueType>
            extends ActorBehaviorMatchKeyListFuture<KeyType, ParamType, ValueType> {

        public ActorBehaviorMatchKeyListFutureStageEnd(int matchKeyEntryId, int requiredSize,
                                                       KeyComparator<KeyType> keyComparator,
                                                       KeyValuesReducer<KeyType, ValueType> keyValuesReducer,
                                                       KeyExtractor<KeyType, ParamType> keyExtractorFromValue,
                                                       Function<ParamType, ValueType> valueExtractorFromValue,
                                                       BiConsumer<KeyType, List<ValueType>> handler,
                                                       Class<KeyType> keyType, Class<ValueType> valueType) {
            super(matchKeyEntryId, requiredSize, keyComparator, keyValuesReducer, keyExtractorFromValue, valueExtractorFromValue, handler, keyType, valueType);
        }

        @Override
        protected HistogramTreeNodeLeaf createLeaf(Object key) {
            return new HistogramNodeLeafListReducibleForStageEnd(key, this);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void processTraversal(Actor self, MailboxKelp.ReducedSize reducedSize, HistogramTreeNodeLeaf leaf) {
            HistogramNodeLeafListReducible list = completedLeaf(putRequiredSize, getListType(), leaf);
            if (list != null &&
                    !(keyValuesReducer instanceof KeyValuesReducerNone) && //handler will never happen list.consume(...), so just
                    list.consume(putRequiredSize, keyValuesReducer.requiredSize(), putTree, 0, reducedSize, true, (BiFunction) keyValuesReducer, (BiConsumer) handler)) {
                ((ActorKelp<?>) self).getMailboxAsKelp().reserveTraversal(self, matchKeyEntryId);
            }
        }

        @Override
        public boolean reduceOnMemory(HistogramTreeNodeLeaf leaf) {
            if (keyValuesReducer instanceof KeyValuesReducerNone || //no handler at the time
                    leaf.sizeOnMemory() < keyValuesReducer.requiredSize()) {
                return false;
            } else {
                return super.reduceOnMemory(leaf);
            }
        }

        @Override
        public boolean reduceForPersistedData() {
            return false;
        }

        public static int getLoaderMax(Actor self) {
            if (self.getMailbox() instanceof MailboxKelp) {
                MailboxKelp m = (MailboxKelp) self.getMailbox();
                if (m.getTreeFactory() instanceof KeyHistogramsPersistable) {
                    KeyHistogramsPersistable p = (KeyHistogramsPersistable) m.getTreeFactory();
                    return p.getConfig().getHistogramMergerMax();
                }
            }
            return 64;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean needToProcessStageEnd(Actor self, MailboxKelp.ReducedSize reducedSize, Object stageKey, HistogramTree tree) { //entire tree
            this.putTree = tree;
            if (tree instanceof HistogramTreePersistable) { //there is a full-tree data
                if (((HistogramTreePersistable) tree).getPreviousFullTreeSource() != null) {
                    try {
                        TreeMerger merger = new TreeMerger(tree)
                                .withReducer((KeyValuesReducer<Object, Object>) keyValuesReducer)
                                .withLoaderMax(getLoaderMax(self));
                        if (self instanceof ActorKelp<?>) {
                            ((ActorKelp<?>) self).setStageEndStats(merger::getStats);
                        }
                        //the tree data will be cleared by full-tree-persist
                        PersistentFileManager.PersistentFileReaderSource source = merger.mergeAllFromTree(this);
                        if (source != null) {
                            long length = merger.getLastLength();
                            TreeMerger.MergeLoader mergeLoader = new TreeMerger.MergeLoader(source);
                            HistogramNodeLeafListReducibleForStageEnd tmpList = null;
                            Object key;
                            ActorKelpStats.ActorKelpStageEndStats stats = merger.getStats();

                            while ((key = mergeLoader.nextKey()) != null) {
                                if (tmpList != null && !key.equals(tmpList.keyStart())) {
                                    stats.mergingReadKeys++;
                                    processStageEndList(reducedSize, tmpList);
                                    tmpList = (HistogramNodeLeafListReducibleForStageEnd) createLeaf(key);
                                } else if (tmpList == null) {
                                    tmpList = (HistogramNodeLeafListReducibleForStageEnd) createLeaf(key);
                                }
                                putPosition = mergeLoader.currentListPosition();
                                Object value;
                                long putCount = 0;
                                while ((value = mergeLoader.nextValue()) != null) {
                                    putValue = value;
                                    tmpList.putValueForStageEnd(this); //TODO memory overflow
                                    ++putCount;
                                }
                                stats.mergingReadValues += putCount;
                            }
                            if (tmpList != null) {
                                stats.mergingReadKeys++;
                                processStageEndList(reducedSize, tmpList);
                            }
                        }
                        return false; //no super call
                    } catch (Exception ex) {
                        tree.getPersistent().getLogger().log(true, KeyHistogramsPersistable.logPersistColor,
                                ex, "merge error");
                    }
                }
            }
            return true;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        protected void processStageEndList(MailboxKelp.ReducedSize reducedSize, HistogramNodeLeafListReducibleForStageEnd list) {
            list.consumeStageEndReduceAll(putRequiredSize, keyValuesReducer.requiredSize(), putTree, 0, reducedSize, (BiFunction) keyValuesReducer, (BiConsumer) handler, false);

            long prevSize = list.size();
            while (list.consumeStageEnd(putRequiredSize, keyValuesReducer.requiredSize(), putTree, 0, reducedSize, (BiFunction) keyValuesReducer, (BiConsumer) handler, false)) {
                long nextSize = list.size();
                if (prevSize <= nextSize) { //no consumption
                    break;
                }
                prevSize = nextSize;
            }
        }

        /**
         * @return true: at the stageEnd, eventually processed the entire tree
         */
        @Override
        public boolean allowFullTreePersist() {
            return true;
        }
    }

    public static class HistogramNodeLeafListReducibleForStageEnd extends HistogramNodeLeafListReducible {
        public static final long serialVersionUID = 1L;

        public HistogramNodeLeafListReducibleForStageEnd() {}

        public HistogramNodeLeafListReducibleForStageEnd(Object key, KeyHistograms.HistogramPutContext context) {
            super(key, context);
        }
        @Override
        public HistogramNodeLeafListReducibleForStageEnd copy(Map<KeyHistograms.HistogramTreeNode, KeyHistograms.HistogramTreeNode> oldToNew) {
            HistogramNodeLeafListReducibleForStageEnd node = (HistogramNodeLeafListReducibleForStageEnd) super.copy(oldToNew);
            return node;
        }

        public void putValueForStageEnd(KeyHistograms.HistogramPutContext context) {
            putValueStruct(context);
            ++size;
        }

        @Override
        protected ReduceResult reduceAndHandle(int requiredSize, int reduceRequiredSize, HistogramTree tree, MailboxKelp.ReducedSize reducedSize,
                                               BiFunction<Object, List<Object>, Iterable<Object>> keyValuesReducer,
                                               BiConsumer<Object, List<Object>> handler, List<Object> vs) {
            Object key = getKey();
            int consuming = vs.size();
            Iterable<Object> rs = reduceRequiredSize <= vs.size() ?
                    keyValuesReducer.apply(key, vs) : vs;
            int reduced = 0;
            for (Object r : rs) {
                values.add(tree, r);
                ++reduced;
            }
            return new ReduceResult(consuming, reduced, 0);
        }


    }
}
