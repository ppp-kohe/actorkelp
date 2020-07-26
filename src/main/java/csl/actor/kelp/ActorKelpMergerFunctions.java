package csl.actor.kelp;

import java.io.Serializable;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.DoubleBinaryOperator;
import java.util.function.IntBinaryOperator;
import java.util.function.LongBinaryOperator;

public class ActorKelpMergerFunctions {
    @SuppressWarnings("unchecked")
    public static MergerFunction<?> getMergerFunction(ActorKelp.MergerOpType m, Type gType) {
        Class<?> type = toClass(gType);
        if (type.equals(Map.class) && gType instanceof ParameterizedType) {
            return new MergerMap((MergerFunction<Object>) getMergerFunction(m,
                    ((ParameterizedType) gType).getActualTypeArguments()[1]));

        } else if (m.equals(ActorKelp.MergerOpType.Default)) {
            if (Mergeable.class.isAssignableFrom(type)) {
                return new MergerMergeable(type);
            } else {
                return new MergerNone();
            }
        } else if (m.equals(ActorKelp.MergerOpType.Add)) {
            if (Integer.class.equals(type) || int.class.equals(type)) {
                return new MergerAddInt();
            } else if (Float.class.equals(type) || float.class.equals(type)) {
                return new MergerAddFloat();
            } else if (Long.class.equals(type) || long.class.equals(type)) {
                return new MergerAddLong();
            } else if (Double.class.equals(type) || double.class.equals(type)) {
                return new MergerAddDouble();
            } else if (Boolean.class.equals(type) || boolean.class.equals(type)) {
                return new MergerAddBoolean();
            } else if (int[].class.equals(type)) {
                return new MergerAddIntArray();
            } else if (float[].class.equals(type)) {
                return new MergerAddFloatArray();
            } else if (long[].class.equals(type)) {
                return new MergerAddLongArray();
            } else if (double[].class.equals(type)) {
                return new MergerAddDoubleArray();
            } else if (boolean[].class.equals(type)) {
                return new MergerAddBooleanArray();
            } else if (List.class.equals(type)) {
                return new MergerAddList();
            } else if (Set.class.equals(type)) {
                return new MergerAddSet();
            }
        } else if (m.equals(ActorKelp.MergerOpType.Multiply)) {
            if (Integer.class.equals(type) || int.class.equals(type)) {
                return new MergerMulInt();
            } else if (Float.class.equals(type) || float.class.equals(type)) {
                return new MergerMulFloat();
            } else if (Long.class.equals(type) || long.class.equals(type)) {
                return new MergerMulLong();
            } else if (Double.class.equals(type) || double.class.equals(type)) {
                return new MergerMulDouble();
            } else if (Boolean.class.equals(type) || boolean.class.equals(type)) {
                return new MergerMulBoolean();
            } else if (int[].class.equals(type)) {
                return new MergerMulIntArray();
            } else if (float[].class.equals(type)) {
                return new MergerMulFloatArray();
            } else if (long[].class.equals(type)) {
                return new MergerMulLongArray();
            } else if (double[].class.equals(type)) {
                return new MergerMulDoubleArray();
            } else if (boolean[].class.equals(type)) {
                return new MergerMulBooleanArray();
            }
        } else if (m.equals(ActorKelp.MergerOpType.Mean)) {
            if (Integer.class.equals(type) || int.class.equals(type)) {
                return new MergerMeanInt();
            } else if (Float.class.equals(type) || float.class.equals(type)) {
                return new MergerMeanFloat();
            } else if (Long.class.equals(type) || long.class.equals(type)) {
                return new MergerMeanLong();
            } else if (Double.class.equals(type) || double.class.equals(type)) {
                return new MergerMeanDouble();
            } else if (int[].class.equals(type)) {
                return new MergerMeanIntArray();
            } else if (float[].class.equals(type)) {
                return new MergerMeanFloatArray();
            } else if (long[].class.equals(type)) {
                return new MergerMeanLongArray();
            } else if (double[].class.equals(type)) {
                return new MergerMeanDoubleArray();
            }
        } else if (m.equals(ActorKelp.MergerOpType.Max)) {
            if (Integer.class.equals(type) || int.class.equals(type)) {
                return new MergerMaxInt();
            } else if (Float.class.equals(type) || float.class.equals(type)) {
                return new MergerMaxFloat();
            } else if (Long.class.equals(type) || long.class.equals(type)) {
                return new MergerMaxLong();
            } else if (Double.class.equals(type) || double.class.equals(type)) {
                return new MergerMaxDouble();
            } else if (int[].class.equals(type)) {
                return new MergerMaxIntArray();
            } else if (float[].class.equals(type)) {
                return new MergerMaxFloatArray();
            } else if (long[].class.equals(type)) {
                return new MergerMaxLongArray();
            } else if (double[].class.equals(type)) {
                return new MergerMaxDoubleArray();
            } else if (Comparable.class.isAssignableFrom(type)) {
                return new MergerMaxComparable();
            }
        } else if (m.equals(ActorKelp.MergerOpType.Min)) {
            if (Integer.class.equals(type) || int.class.equals(type)) {
                return new MergerMinInt();
            } else if (Float.class.equals(type) || float.class.equals(type)) {
                return new MergerMinFloat();
            } else if (Long.class.equals(type) || long.class.equals(type)) {
                return new MergerMinLong();
            } else if (Double.class.equals(type) || double.class.equals(type)) {
                return new MergerMinDouble();
            } else if (int[].class.equals(type)) {
                return new MergerMinIntArray();
            } else if (float[].class.equals(type)) {
                return new MergerMinFloatArray();
            } else if (long[].class.equals(type)) {
                return new MergerMinLongArray();
            } else if (double[].class.equals(type)) {
                return new MergerMinDoubleArray();
            } else if (Comparable.class.isAssignableFrom(type)) {
                return new MergerMinComparable();
            }
        } else if (m.equals(ActorKelp.MergerOpType.None)) {
            return new MergerNone();
        }
        throw new RuntimeException("invalid: " + m + " " + type);
    }

    public static Class<?> toClass(Type t) {
        if (t instanceof Class<?>) {
            return (Class<?>) t;
        } else if (t instanceof ParameterizedType) {
            return toClass(((ParameterizedType) t).getRawType());
        } else if (t instanceof WildcardType) {
            Type[] up = ((WildcardType) t).getUpperBounds();
            if (up != null && up.length >= 1) {
                return toClass(up[0]);
            } else {
                return Object.class;
            }
        } else if (t instanceof TypeVariable<?>) {
            Type[] up = ((TypeVariable<?>) t).getBounds();
            if (up != null && up.length >= 1) {
                return toClass(up[0]);
            } else {
                return Object.class;
            }
        } else if (t instanceof GenericArrayType) {
            return Object[].class;
        } else {
            return Object.class;
        }
    }

    public interface Mergeable<SelfType extends Mergeable<SelfType>> extends Serializable {
        SelfType merge(ActorKelpSerializable.MergingContext context, SelfType another);
    }

    /**
     * the implementing class must be have a static method
     * <pre>
     *    public static MergerFunction get({@link ActorKelp.MergerOpType}, {@link Type})
     * </pre>
     */
    public interface MergerFunction<ValueType> {
        Object merge(ActorKelpSerializable.MergingContext context, ValueType l, ValueType r);
    }

    @FunctionalInterface
    public interface FloatBinaryOperator {
        float applyAsFloat(float l, float r);
    }

    @FunctionalInterface
    public interface BooleanBinaryOperator {
        boolean applyAsBoolean(boolean l, boolean r);
    }

    public static class MergerNone implements MergerFunction<Object> {
        public static MergerNone get(ActorKelp.MergerOpType op, Type type) {
            return new MergerNone();
        }
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return l == null ? r : l;
        }
    }

    public static class MergerMergeable implements MergerFunction<Mergeable<?>> {
        protected Class<?> type;
        public static MergerMergeable get(ActorKelp.MergerOpType op, Type type) {
            return new MergerMergeable(toClass(type));
        }

        public MergerMergeable(Class<?> type) {
            this.type = type;
            if (!Mergeable.class.isAssignableFrom(type)) {
                throw new RuntimeException("invalid type: " + type + " is not " + Mergeable.class.getName());
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Mergeable<?> l, Mergeable<?> r) {
            if (l == null) {
                return r;
            } else if (r == null) {
                return l;
            } else {
                return ((Mergeable) l).merge(context, r);
            }
        }
    }

    public static class MergerAddList implements MergerFunction<Collection<?>> {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Collection<?> l, Collection<?> r) {
            List<Object> list = new ArrayList<>(l);
            list.addAll(r);
            return list;
        }
    }

    public static class MergerAddSet implements MergerFunction<Collection<?>> {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Collection<?> l, Collection<?> r) {
            Set<Object> list = new HashSet<>(l);
            list.addAll(r);
            return list;
        }
    }

    public static class MergerMap implements MergerFunction<Map<Object,Object>> {
        protected MergerFunction<Object> entry;

        public MergerMap(MergerFunction<Object> entry) {
            this.entry = entry;
        }

        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Map<Object,Object> l, Map<Object,Object> r) {
            Map<Object,Object> lm = (l == null ? Collections.emptyMap() : l);
            Map<Object,Object> rm = (r == null ? Collections.emptyMap() : r);

            Map<Object,Object> m = new HashMap<>((lm.size() + rm.size()) / 2);
            Set<Object> ks = new HashSet<>(lm.keySet());
            ks.addAll(rm.keySet());
            for (Object k : ks) {
                m.put(k, entry.merge(context, lm.get(k), rm.get(k)));
            }
            return m;
        }
    }

    public static abstract class MergerFunctionDefault implements MergerFunction<Object> {
        public static MergerFunction<?> get(ActorKelp.MergerOpType opType, Type type) {
            return getMergerFunction(opType, type);
        }

        public int toInt(Object o) {
            return o == null ? 0 : (Integer) o;
        }
        public float toFloat(Object o) {
            return o == null ? 0 : (Float) o;
        }
        public long toLong(Object o) {
            return o == null ? 0 : (Long) o;
        }
        public double toDouble(Object o) {
            return o == null ? 0 : (Double) o;
        }
        public boolean toBoolean(Object o) {
            return o == null ? false : (Boolean) o;
        }

        public int[] intArray(Object l, Object r, IntBinaryOperator op) {
            int[] la = (int[]) l;
            int[] ra = (int[]) r;
            int[] a;
            if (la == null && ra == null) {
                return null;
            } else if (la == null) {
                la = new int[ra.length];
            } else if (ra == null) {
                ra = new int[la.length];
            }
            if (la.length < ra.length) {
                a = ra;
            } else {
                a = la;
            }
            for (int i = 0, s = Math.max(la.length, ra.length); i < s; ++i) {
                a[i] = op.applyAsInt((i < la.length ? la[i] : 0), (i < ra.length ? ra[i] : 0));
            }
            return a;
        }

        public float[] floatArray(Object l, Object r, FloatBinaryOperator op) {
            float[] la = (float[]) l;
            float[] ra = (float[]) r;
            float[] a;
            if (la == null && ra == null) {
                return null;
            } else if (la == null) {
                la = new float[ra.length];
            } else if (ra == null) {
                ra = new float[la.length];
            }
            if (la.length < ra.length) {
                a = ra;
            } else {
                a = la;
            }
            for (int i = 0, s = Math.max(la.length, ra.length); i < s; ++i) {
                a[i] = op.applyAsFloat((i < la.length ? la[i] : 0), (i < ra.length ? ra[i] : 0));
            }
            return a;
        }

        public long[] longArray(Object l, Object r, LongBinaryOperator op) {
            long[] la = (long[]) l;
            long[] ra = (long[]) r;
            long[] a;
            if (la == null && ra == null) {
                return null;
            } else if (la == null) {
                la = new long[ra.length];
            } else if (ra == null) {
                ra = new long[la.length];
            }
            if (la.length < ra.length) {
                a = ra;
            } else {
                a = la;
            }
            for (int i = 0, s = Math.max(la.length, ra.length); i < s; ++i) {
                a[i] = op.applyAsLong((i < la.length ? la[i] : 0), (i < ra.length ? ra[i] : 0));
            }
            return a;
        }
        public double[] doubleArray(Object l, Object r, DoubleBinaryOperator op) {
            double[] la = (double[]) l;
            double[] ra = (double[]) r;
            double[] a;
            if (la == null && ra == null) {
                return null;
            } else if (la == null) {
                la = new double[ra.length];
            } else if (ra == null) {
                ra = new double[la.length];
            }
            if (la.length < ra.length) {
                a = ra;
            } else {
                a = la;
            }
            for (int i = 0, s = Math.max(la.length, ra.length); i < s; ++i) {
                a[i] = op.applyAsDouble((i < la.length ? la[i] : 0), (i < ra.length ? ra[i] : 0));
            }
            return a;
        }
        public boolean[] booleanArray(Object l, Object r, BooleanBinaryOperator op) {
            boolean[] la = (boolean[]) l;
            boolean[] ra = (boolean[]) r;
            boolean[] a;
            if (la == null && ra == null) {
                return null;
            } else if (la == null) {
                la = new boolean[ra.length];
            } else if (ra == null) {
                ra = new boolean[la.length];
            }
            if (la.length < ra.length) {
                a = ra;
            } else {
                a = la;
            }
            for (int i = 0, s = Math.max(la.length, ra.length); i < s; ++i) {
                a[i] = op.applyAsBoolean((i < la.length && la[i]), (i < ra.length && ra[i]));
            }
            return a;
        }
    }

    public static class MergerAddInt extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return toInt(l) + toInt(r);
        }
    }

    public static class MergerAddFloat extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return toFloat(l) + toFloat(r);
        }
    }

    public static class MergerAddLong extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return toLong(l) + toLong(r);
        }
    }

    public static class MergerAddDouble extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return toDouble(l) + toDouble(r);
        }
    }

    public static class MergerAddBoolean extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return toBoolean(l) || toBoolean(r);
        }
    }

    public static class MergerAddIntArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return intArray(l, r, Integer::sum);
        }
    }

    public static class MergerAddFloatArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return floatArray(l, r, Float::sum);
        }
    }

    public static class MergerAddLongArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return longArray(l, r, Long::sum);
        }
    }

    public static class MergerAddDoubleArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return doubleArray(l, r, Double::sum);
        }
    }

    public static class MergerAddBooleanArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return booleanArray(l, r, (lv, rv) -> lv || rv);
        }
    }

    public static class MergerMulInt extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return toInt(l) * toInt(r);
        }
    }

    public static class MergerMulFloat extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return toFloat(l) * toFloat(r);
        }
    }

    public static class MergerMulLong extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return toLong(l) * toLong(r);
        }
    }

    public static class MergerMulDouble extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return toDouble(l) * toDouble(r);
        }
    }

    public static class MergerMulBoolean extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return toBoolean(l) && toBoolean(r);
        }
    }

    public static class MergerMulIntArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return intArray(l, r, (lv, rv) -> lv * rv);
        }
    }

    public static class MergerMulFloatArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return floatArray(l, r, (lv, rv) -> lv * rv);
        }
    }

    public static class MergerMulLongArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return longArray(l, r, (lv, rv) -> lv * rv);
        }
    }

    public static class MergerMulDoubleArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return doubleArray(l, r, (lv, rv) -> lv * rv);
        }
    }

    public static class MergerMulBooleanArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return booleanArray(l, r, (lv, rv) -> lv && rv);
        }
    }

    public static class MergerMeanInt extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return context.mean(toInt(l), toInt(r));
        }
    }

    public static class MergerMeanFloat extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return context.mean(toFloat(l), toFloat(r));
        }
    }

    public static class MergerMeanLong extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return context.mean(toLong(l), toLong(r));
        }
    }

    public static class MergerMeanDouble extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return context.mean(toDouble(l), toDouble(r));
        }
    }

    public static class MergerMeanIntArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return intArray(l, r, context::mean);
        }
    }

    public static class MergerMeanFloatArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return floatArray(l, r, context::mean);
        }
    }

    public static class MergerMeanLongArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return longArray(l, r, context::mean);
        }
    }

    public static class MergerMeanDoubleArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return doubleArray(l, r, context::mean);
        }
    }

    public static class MergerMaxInt extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return Math.max(toInt(l), toInt(r));
        }
    }

    public static class MergerMaxFloat extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return Math.max(toFloat(l), toFloat(r));
        }
    }

    public static class MergerMaxLong extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return Math.max(toLong(l), toLong(r));
        }
    }

    public static class MergerMaxDouble extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return Math.max(toDouble(l), toDouble(r));
        }
    }

    public static class MergerMaxIntArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return intArray(l, r, Math::max);
        }
    }

    public static class MergerMaxFloatArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return floatArray(l, r, Math::max);
        }
    }

    public static class MergerMaxLongArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return longArray(l, r, Math::max);
        }
    }

    public static class MergerMaxDoubleArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return doubleArray(l, r, Math::max);
        }
    }

    public static class MergerMaxComparable extends MergerFunctionDefault {
        @SuppressWarnings("unchecked")
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            if (l == null) {
                return r;
            } else if (r == null) {
                return l;
            } else {
                int n = ((Comparable<Object>) l).compareTo(r);
                if (n >= 0) {
                    return l;
                } else {
                    return r;
                }
            }
        }
    }

    public static class MergerMinInt extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return Math.min(toInt(l), toInt(r));
        }
    }

    public static class MergerMinFloat extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return Math.min(toFloat(l), toFloat(r));
        }
    }

    public static class MergerMinLong extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return Math.min(toLong(l), toLong(r));
        }
    }

    public static class MergerMinDouble extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return Math.min(toDouble(l), toDouble(r));
        }
    }

    public static class MergerMinIntArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return intArray(l, r, Math::min);
        }
    }

    public static class MergerMinFloatArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return floatArray(l, r, Math::min);
        }
    }

    public static class MergerMinLongArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return longArray(l, r, Math::min);
        }
    }

    public static class MergerMinDoubleArray extends MergerFunctionDefault {
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            return doubleArray(l, r, Math::min);
        }
    }

    public static class MergerMinComparable extends MergerFunctionDefault {
        @SuppressWarnings("unchecked")
        @Override
        public Object merge(ActorKelpSerializable.MergingContext context, Object l, Object r) {
            if (l == null) {
                return r;
            } else if (r == null) {
                return l;
            } else {
                int n = ((Comparable<Object>) l).compareTo(r);
                if (n < 0) {
                    return l;
                } else {
                    return r;
                }
            }
        }
    }
}
