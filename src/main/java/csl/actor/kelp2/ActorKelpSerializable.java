package csl.actor.kelp2;

import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.kelp2.behavior.KeyHistograms;
import csl.actor.remote.KryoBuilder;

import java.io.Serializable;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.IntBinaryOperator;
import java.util.function.LongBinaryOperator;

public class ActorKelpSerializable<SelfType extends ActorKelp<SelfType>> implements Serializable {
    public static final long serialVersionUID = 1L;
    public Class<SelfType> actorType;
    public String name;
    public ConfigKelp config;
    public Message<?>[] messages;
    public List<KeyHistograms.HistogramTree> histograms;
    public Serializable internalState;

    public ActorKelpSerializable() {}

    public ActorKelpSerializable(SelfType actor) {
        init(actor);
    }

    protected void init(SelfType actor) {
        initActorType(actor);
        initName(actor);
        initConfig(actor);
        initInternalState(actor);
    }

    @SuppressWarnings("unchecked")
    protected void initActorType(SelfType actor) {
        actorType = (Class<SelfType>) actor.getClass();
    }

    protected void initName(SelfType actor) {
        name = actor.getName();
    }

    protected void initConfig(SelfType actor) {
        config = actor.getConfig();
    }

    protected void initInternalState(SelfType actor) {
        internalState = actor.toInternalState();
    }

    public void setMessages(Message<?>[] messages) {
        this.messages = messages;
    }

    public void setHistograms(List<KeyHistograms.HistogramTree> histograms) {
        this.histograms = histograms;
    }

    public SelfType restore(ActorSystem system, long num, ConfigKelp config) throws Exception {
        SelfType a = create(system, restoreName(num), config);
        restoreSetNonOriginal(a);
        restoreSetShuffleIndex(a, num);
        restoreInit(a);
        return a;
    }

    protected String restoreName(long num) {
        return name == null ? ("$" + num) : name + "$" + num;
    }

    @SuppressWarnings("unchecked")
    protected SelfType create(ActorSystem system, String name, ConfigKelp config) throws Exception {
        return (SelfType) getConstructor(actorType).create(system, name, config);
    }

    protected void restoreSetNonOriginal(SelfType actor) {
        actor.setOriginal(false);
    }

    protected void restoreSetShuffleIndex(SelfType actor, long num) {
        actor.setShuffleIndex((int) num);
    }

    protected void restoreInit(SelfType actor) {
        actor.setInternalState(internalState);
    }


    //////////

    protected static Map<Class<?>, KelpConstructor> typeToConstructor = new ConcurrentHashMap<>();

    public static KelpConstructor getConstructor(Class<?> type) {
        return typeToConstructor.computeIfAbsent(type, KelpConstructor::new);
    }

    public static class KelpConstructor {
        protected Class<?> type;
        protected Constructor<?> constructor;

        public KelpConstructor(Class<?> type) {
            this.type = type;
            init();
        }

        protected void init() {
            //select from 3 constructors
            Constructor<?> cons3 = null; //(ActorSystem, String, ConfigKelp)
            Constructor<?> cons2 = null; //(ActorSystem, ConfigKelp)
            Constructor<?> cons1 = null; //(ActorSystem)
            for (Constructor<?> constructor : type.getConstructors()) {
                Class<?>[] argTypes = constructor.getParameterTypes();
                if (argTypes.length == 3 &&
                        argTypes[0].equals(ActorSystem.class) &&
                        argTypes[1].equals(String.class) &&
                        ConfigKelp.class.isAssignableFrom(argTypes[2])) {
                    cons3 = constructor;
                } else if (argTypes.length == 2 &&
                        argTypes[0].equals(ActorSystem.class) &&
                        ConfigKelp.class.isAssignableFrom(argTypes[1])) {
                    cons2 = constructor;
                } else if (argTypes.length == 1 &&
                        argTypes[0].equals(ActorSystem.class)) {
                    cons1 = constructor;
                }
            }

            if (cons3 != null) {
                this.constructor = cons3;
            } else if (cons2 != null) {
                this.constructor = cons2;
            } else if (cons1 != null) {
                this.constructor = cons1;
            } else {
                throw new RuntimeException("no constructor: " + type);
            }
        }

        public Object create(ActorSystem system, String name, ConfigKelp config) throws Exception {
            int pc = constructor.getParameterCount();
            if (pc == 3) {
                return constructor.newInstance(system, name, config);
            } else if (pc == 2) {
                ActorKelp<?> obj = (ActorKelp<?>) constructor.newInstance(system, config);
                obj.setNameInternal(name);
                return obj;
            } else if (pc == 1) {
                ActorKelp<?> obj = (ActorKelp<?>) constructor.newInstance(system);
                //config will be discarded
                obj.setNameInternal(name);
                return obj;
            } else {
                throw new RuntimeException("invalid constructor: " + constructor);
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + constructor + ")";
        }
    }


    //////////


    protected static Map<Class<?>, InternalStateBuilder> typeToBuilder = new ConcurrentHashMap<>();

    public static InternalStateBuilder getBuilder(Class<?> type) {
        return typeToBuilder.computeIfAbsent(type, InternalStateBuilder::new);
    }

    public static class InternalStateBuilder {
        protected Class<?> type;
        protected List<InternalStateField> fields;

        public InternalStateBuilder(Class<?> type) {
            this.type = type;
            init();
        }

        protected void init() {
            fields = new ArrayList<>();
            for (Field fld: type.getDeclaredFields()) {
                initField(fld);
            }
            fields.sort(Comparator.comparing(InternalStateField::getTypeAndName));
        }

        protected void initField(Field fld) {
            ActorKelp.TransferredState mark = fld.getAnnotation(ActorKelp.TransferredState.class);
            if (mark != null) {
                fields.add(new InternalStateField(fld, mark));
            }
        }

        public Serializable toState(KryoBuilder.SerializerFunction serializer, Object obj) throws Exception {
            Object[] data = new Object[fields.size()];
            for (int i = 0, l = fields.size(); i < l; ++i) {
                data[i] = fields.get(i).toState(serializer, obj);
            }
            return data;
        }

        public void setState(Object obj, Serializable d) throws Exception {
            Object[] data = (Object[]) d;
            for (int i = 0, l = fields.size(); i < l; ++i) {
                fields.get(i).setState(obj, (Serializable) data[i]);
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + type.getName() + ", fields[" + fields.size() + "]" + ")";
        }
    }

    public static class InternalStateField {
        protected ActorKelp.TransferredState mark;
        protected Field field;
        protected MergerFunction merger;

        public InternalStateField(Field field, ActorKelp.TransferredState mark) {
            this.field = field;
            this.mark = mark;
            init();
        }

        protected void init() {
            field.setAccessible(true);
            ActorKelp.Merger mergerOp = mark.merger();
            if (mergerOp instanceof ActorKelp.MergerDefault) {
                merger = getMergerFunction((ActorKelp.MergerDefault) mergerOp, field.getGenericType());
            } else if (mergerOp instanceof ActorKelp.MergerClass) {
                try {
                    merger = ((ActorKelp.MergerClass) mergerOp).value().getConstructor(field.getType()).newInstance();
                } catch (Exception ex) {
                    throw new RuntimeException("invalid merger: " + mergerOp + " : " + field);
                }
            }
        }

        public Serializable toState(KryoBuilder.SerializerFunction serializer, Object obj) throws Exception {
            return (Serializable) serializer.copy(field.get(obj));
        }

        public void setState(Object obj, Serializable data) throws Exception {
            field.set(obj, data);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + field + ")";
        }

        public String getTypeAndName() {
            return field.getDeclaringClass().getName() + ":" + field.getName();
        }

        public void merge(MergingContext context, Object obj, Serializable data) throws Exception {
            field.set(obj, merger.merge(context, field.get(obj), data));
        }
    }

    public static MergerFunction getMergerFunction(ActorKelp.MergerDefault m, Type gType) {
        Class<?> type = toClass(gType);
        if (type.equals(Map.class) && gType instanceof ParameterizedType) {
            return new MergerMap(getMergerFunction(m,
                    ((ParameterizedType) gType).getActualTypeArguments()[1]));

        } else if (m.equals(ActorKelp.MergerDefault.Default)) {
            if (Mergeable.class.isAssignableFrom(type)) {
                return new MergerMergeable(type);
            } else {
                return new MergerNone();
            }
        } else if (m.equals(ActorKelp.MergerDefault.Add)) {
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
        } else if (m.equals(ActorKelp.MergerDefault.Multiply)) {
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
        } else if (m.equals(ActorKelp.MergerDefault.Mean)) {
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
        } else if (m.equals(ActorKelp.MergerDefault.Max)) {
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
            }
        } else if (m.equals(ActorKelp.MergerDefault.Min)) {
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
            }
        } else if (m.equals(ActorKelp.MergerDefault.None)) {
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

    public interface Mergeable<SelfType extends Mergeable<SelfType>> {
        SelfType merge(MergingContext context, SelfType another);
    }

    /**
     * the implementing class must be have a constructor taking the field type: MergerFunction(Class cls)
     */
    public interface MergerFunction {
        Object merge(MergingContext context, Object l, Object r);
    }

    public static class MergingContext {
        protected int left;
        protected int right;

        public int mergeCountLeft() {
            return left;
        }
        public int mergeCountRight() {
            return left;
        }
        public double mergeRatioLeft() {
            return left / (double) (left + right);
        }
        public double mergeRatioRight() {
            return right / (double) (left + right);
        }

        @Override
        public String toString() {
            return String.format("(%,d:%.2f, %,d:%.2f)",
                    mergeCountLeft(), mergeRatioLeft(), mergeCountRight(), mergeRatioRight());
        }

        public int mean(int l, int r) {
            return (int) (l * mergeRatioLeft() + r * mergeRatioRight());
        }

        public float mean(float l, float r) {
            return (float) (l * mergeRatioLeft() + r * mergeRatioRight());
        }

        public long mean(long l, long r) {
            return (long) (l * mergeRatioLeft() + r * mergeRatioRight());
        }

        public double mean(double l, double r) {
            return (l * mergeRatioLeft() + r * mergeRatioRight());
        }
    }

    public static class MergerNone implements MergerFunction {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return l == null ? r : l;
        }
    }

    public static class MergerMergeable implements MergerFunction {
        protected Class<?> type;

        public MergerMergeable(Class<?> type) {
            this.type = type;
            if (!Mergeable.class.isAssignableFrom(type)) {
                throw new RuntimeException("invalid type: " + type + " is not " + Mergeable.class.getName());
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            if (l == null) {
                return r;
            } else if (r == null) {
                return l;
            } else {
                return ((Mergeable) l).merge(context, (Mergeable) r);
            }
        }
    }

    public static class MergerAddList implements MergerFunction {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            List<Object> list = new ArrayList<>((Collection<?>) l);
            list.addAll((Collection<?>) r);
            return list;
        }
    }

    public static class MergerAddSet implements MergerFunction {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            Set<Object> list = new HashSet<>((Collection<?>) l);
            list.addAll((Collection<?>) r);
            return list;
        }
    }

    public static class MergerMap implements MergerFunction {
        protected MergerFunction entry;

        public MergerMap(MergerFunction entry) {
            this.entry = entry;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            Map<Object,Object> lm = (l == null ? Collections.emptyMap() : (Map<Object,Object>) l);
            Map<Object,Object> rm = (r == null ? Collections.emptyMap() : (Map<Object,Object>) r);

            Map<Object,Object> m = new HashMap<>((lm.size() + rm.size()) / 2);
            Set<Object> ks = new HashSet<>(lm.keySet());
            ks.addAll(rm.keySet());
            for (Object k : ks) {
                m.put(k, entry.merge(context, lm.get(k), rm.get(k)));
            }
            return m;
        }
    }

    public static abstract class MergerFunctionDefault implements MergerFunction {
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

    @FunctionalInterface
    public interface FloatBinaryOperator {
        float applyAsFloat(float l, float r);
    }

    @FunctionalInterface
    public interface BooleanBinaryOperator {
        boolean applyAsBoolean(boolean l, boolean r);
    }

    public static class MergerAddInt extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return toInt(l) + toInt(r);
        }
    }
    public static class MergerAddFloat extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return toFloat(l) + toFloat(r);
        }
    }
    public static class MergerAddLong extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return toLong(l) + toLong(r);
        }
    }
    public static class MergerAddDouble extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return toDouble(l) + toDouble(r);
        }
    }
    public static class MergerAddBoolean extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return toBoolean(l) || toBoolean(r);
        }
    }

    public static class MergerAddIntArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return intArray(l, r, Integer::sum);
        }
    }
    public static class MergerAddFloatArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return floatArray(l, r, Float::sum);
        }
    }
    public static class MergerAddLongArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return longArray(l, r, Long::sum);
        }
    }
    public static class MergerAddDoubleArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return doubleArray(l, r, Double::sum);
        }
    }
    public static class MergerAddBooleanArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return booleanArray(l, r, (lv, rv) -> lv || rv);
        }
    }

    public static class MergerMulInt extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return toInt(l) * toInt(r);
        }
    }
    public static class MergerMulFloat extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return toFloat(l) * toFloat(r);
        }
    }
    public static class MergerMulLong extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return toLong(l) * toLong(r);
        }
    }
    public static class MergerMulDouble extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return toDouble(l) * toDouble(r);
        }
    }
    public static class MergerMulBoolean extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return toBoolean(l) && toBoolean(r);
        }
    }

    public static class MergerMulIntArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return intArray(l, r, (lv, rv) -> lv * rv);
        }
    }
    public static class MergerMulFloatArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return floatArray(l, r, (lv, rv) -> lv * rv);
        }
    }
    public static class MergerMulLongArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return longArray(l, r, (lv, rv) -> lv * rv);
        }
    }
    public static class MergerMulDoubleArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return doubleArray(l, r, (lv, rv) -> lv * rv);
        }
    }
    public static class MergerMulBooleanArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return booleanArray(l, r, (lv, rv) -> lv && rv);
        }
    }


    public static class MergerMeanInt extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return context.mean(toInt(l), toInt(r));
        }
    }
    public static class MergerMeanFloat extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return context.mean(toFloat(l), toFloat(r));
        }
    }
    public static class MergerMeanLong extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return context.mean(toLong(l), toLong(r));
        }
    }
    public static class MergerMeanDouble extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return context.mean(toDouble(l), toDouble(r));
        }
    }

    public static class MergerMeanIntArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return intArray(l, r, context::mean);
        }
    }
    public static class MergerMeanFloatArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return floatArray(l, r, context::mean);
        }
    }
    public static class MergerMeanLongArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return longArray(l, r, context::mean);
        }
    }
    public static class MergerMeanDoubleArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return doubleArray(l, r, context::mean);
        }
    }

    public static class MergerMaxInt extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return Math.max(toInt(l), toInt(r));
        }
    }
    public static class MergerMaxFloat extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return Math.max(toFloat(l), toFloat(r));
        }
    }
    public static class MergerMaxLong extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return Math.max(toLong(l), toLong(r));
        }
    }
    public static class MergerMaxDouble extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return Math.max(toDouble(l), toDouble(r));
        }
    }

    public static class MergerMaxIntArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return intArray(l, r, Math::max);
        }
    }
    public static class MergerMaxFloatArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return floatArray(l, r, Math::max);
        }
    }
    public static class MergerMaxLongArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return longArray(l, r, Math::max);
        }
    }
    public static class MergerMaxDoubleArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return doubleArray(l, r, Math::max);
        }
    }

    public static class MergerMinInt extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return Math.min(toInt(l), toInt(r));
        }
    }
    public static class MergerMinFloat extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return Math.min(toFloat(l), toFloat(r));
        }
    }
    public static class MergerMinLong extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return Math.min(toLong(l), toLong(r));
        }
    }
    public static class MergerMinDouble extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return Math.min(toDouble(l), toDouble(r));
        }
    }

    public static class MergerMinIntArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return intArray(l, r, Math::min);
        }
    }
    public static class MergerMinFloatArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return floatArray(l, r, Math::min);
        }
    }
    public static class MergerMinLongArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return longArray(l, r, Math::min);
        }
    }
    public static class MergerMinDoubleArray extends MergerFunctionDefault {
        @Override
        public Object merge(MergingContext context, Object l, Object r) {
            return doubleArray(l, r, Math::min);
        }
    }
}
