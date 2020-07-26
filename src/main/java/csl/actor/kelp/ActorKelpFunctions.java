package csl.actor.kelp;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ActorKelpFunctions {

    public interface KeyExtractor<KeyType, ValueType> extends Serializable {
        long serialVersionUID = 1L;
        boolean matchValue(Object value);
        KeyType toKey(ValueType v);
    }

    public interface KeyExtractorFunction<ValueType, KeyType> extends Function<ValueType, KeyType>, Serializable {
        long serialVersionUID = 1L;
    }

    public static class KeyExtractorClass<KeyType, ValueType> implements KeyExtractor<KeyType, ValueType> {
        public static final long serialVersionUID = 1L;

        protected Class<ValueType> valueType;
        protected KeyExtractorFunction<ValueType, KeyType> keyExtractorFromValue;

        public KeyExtractorClass(Class<ValueType> valueType, KeyExtractorFunction<ValueType, KeyType> keyExtractorFromValue) {
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
        public static final long serialVersionUID = 1L;
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

    public interface KeyComparator<KeyType> extends Serializable, Comparator<KeyType> {
        int compare(KeyType key1, KeyType key2);

        default KeyType centerPoint(KeyType leftEnd, KeyType rightStart) {
            return rightStart;
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
                return centerPointPrimitive(leftEnd, rightStart);
            } else {
                return rightStart;
            }
        }
    }

    public static class KeyComparatorOrdered<KeyType extends Comparable<KeyType>> implements KeyComparator<KeyType> {
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
}
