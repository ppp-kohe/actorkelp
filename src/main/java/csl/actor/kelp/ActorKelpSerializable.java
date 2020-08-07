package csl.actor.kelp;

import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.kelp.behavior.KeyHistograms;
import csl.actor.remote.KryoBuilder;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ActorKelpSerializable<SelfType extends ActorKelp<SelfType>> implements Serializable {
    public static final long serialVersionUID = 1L;
    public Class<SelfType> actorType;
    public String name;
    public ConfigKelp config;
    public Message<?>[] messages;
    public List<KeyHistograms.HistogramTree> histograms;
    /** it is not Serializable, but it needs to have support of reading/writing/copying by a serializer */
    public Object internalState;
    public int mergedCount;
    public Set<String> mergedActorNames;
    public int shuffleIndex;
    public Set<ActorRef> shuffleOriginals;

    public boolean includeMailbox;

    public transient volatile boolean internalStateUsed = false;

    public ActorKelpSerializable() {}

    public ActorKelpSerializable(SelfType actor, boolean includeMailbox) {
        this.includeMailbox = includeMailbox;
        init(actor);
    }

    protected void init(SelfType actor) {
        initActorType(actor);
        initName(actor);
        initConfig(actor);
        initMergedCount(actor);
        initMergedActorNames(actor);
        initShuffleIndex(actor);
        initShuffleOriginals(actor);
        initMailbox(actor);
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

    protected void initMergedCount(SelfType actor) {
        mergedCount = actor.getMergedCount();
    }

    protected void initMergedActorNames(SelfType actor) {
        mergedActorNames = new HashSet<>(actor.getMergedActorNames());
    }

    protected void initShuffleIndex(SelfType actor) {
        shuffleIndex = actor.getShuffleIndex();
    }

    protected void initMailbox(SelfType actor) {
        if (includeMailbox) {
            actor.getMailboxAsKelp().serializeTo(actor, this);
        }
    }

    protected void initShuffleOriginals(SelfType actor) {
        shuffleOriginals = new HashSet<>(actor.getShuffleOriginals());
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

    //////// restore

    public SelfType restorePlace(ActorSystem sytem, long num, ConfigKelp config) throws Exception {
        SelfType a = restore(sytem, num, config);
        restoreInitPlace(a);
        return a;
    }

    public SelfType restoreShuffle(ActorSystem system, long num, ConfigKelp config) throws Exception {
        SelfType a = restore(system, num, config);
        restoreInitShuffle(a);
        return a;
    }

    public SelfType restoreMerge(ActorSystem sytem, ConfigKelp config) throws Exception {
        SelfType a = restore(sytem, -1, config);
        restoreInitMerge(a);
        return a;
    }

    public SelfType restore(ActorSystem system, long num, ConfigKelp config) throws Exception {
        SelfType a = create(system, restoreName(num), config);
        restoreSetNonOriginal(a);
        restoreSetShuffleIndex(a, num);
        restoreMergedCount(a);
        restoreShuffleOriginals(a);
        restoreMailbox(a);
        restoreInternalState(a);
        return a;
    }

    public void restore(SelfType a) {
        restoreInternalState(a);
    }


    protected String restoreName(long num) {
        return name == null ? ("$" + num) : name + "$" + num;
    }

    @SuppressWarnings("unchecked")
    public SelfType create(ActorSystem system, String name, ConfigKelp config) throws Exception {
        return (SelfType) getConstructor(actorType).create(system, name, config);
    }

    protected void restoreSetNonOriginal(SelfType actor) {
        actor.setUnit(true);
    }

    protected void restoreSetShuffleIndex(SelfType actor, long num) {
        if (num == -1) {
            num = shuffleIndex;
        }
        actor.setShuffleIndex((int) num);
    }

    protected void restoreShuffleOriginals(SelfType actor) {
        actor.getShuffleOriginals().addAll(shuffleOriginals);
    }

    protected void restoreMailbox(SelfType actor) {
        if (includeMailbox) {
            actor.getMailboxAsKelp().deserializeFrom(actor, this);
        }
    }

    protected void restoreInternalState(SelfType actor) {
        boolean b = internalStateUsed;
        internalStateUsed = true;
        actor.setInternalState(internalState, b);
    }

    protected void restoreMergedCount(SelfType actor) {
        actor.setMergedCount(mergedCount);
    }

    protected void restoreMergedActorNames(SelfType actor) {
        actor.setMergedActorNames(mergedActorNames);
    }

    protected void restoreInitShuffle(SelfType actor) {
        actor.initRestoreShuffle();
    }

    protected void restoreInitMerge(SelfType actor) {
        actor.initRestoreMerge();
    }

    protected void restoreInitPlace(SelfType actor) {
        actor.initRestorePlace();
    }

    public void mergeTo(SelfType actor) {
        mergeToInternalState(actor);
        mergeToCount(actor);
        mergeToActorNames(actor);
    }

    protected void mergeToInternalState(SelfType actor) {
        actor.mergeInternalState(this, new MergingContext(actor, actor.getMergedCount(), mergedCount), internalState);
    }

    protected void mergeToCount(SelfType actor) {
        actor.setMergedCount(actor.getMergedCount() + mergedCount);
    }

    protected void mergeToActorNames(SelfType actor) {
        Set<String> names = new HashSet<>(actor.getMergedActorNames().size() + mergedActorNames.size() + 1);
        if (name != null) {
            names.add(name);
        }
        names.addAll(actor.getMergedActorNames());
        names.addAll(mergedActorNames);
        actor.setMergedActorNames(names);
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
                if (name != null) {
                    obj.setNameInternal(name);
                }
                return obj;
            } else if (pc == 1) {
                ActorKelp<?> obj = (ActorKelp<?>) constructor.newInstance(system);
                //config will be discarded
                if (name != null) {
                    obj.setNameInternal(name);
                }
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

        public Object toState(KryoBuilder.SerializerFunction serializer, Object obj) throws Exception {
            Object[] data = new Object[fields.size()];
            for (int i = 0, l = fields.size(); i < l; ++i) {
                data[i] = fields.get(i).toState(serializer, obj);
            }
            return data;
        }

        public void setState(KryoBuilder.SerializerFunction serializer, Object obj, Object d, boolean needToCopy) throws Exception {
            Object[] data = (Object[]) d;
            for (int i = 0, l = fields.size(); i < l; ++i) {
                InternalStateField fld = fields.get(i);
                if (!fld.isMergeOnly()) {
                    if (needToCopy) {
                        fld.setState(serializer, obj, data[i]);
                    } else {
                        fld.setState(obj, data[i]);
                    }
                }
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + type.getName() + ", fields[" + fields.size() + "]" + ")";
        }

        public void merge(MergingContext context, Object obj, Object d) throws Exception {
            Object[] data = (Object[]) d;
            for (int i = 0, l = fields.size(); i < l; ++i) {
                fields.get(i).merge(context, obj, data[i]);
            }
        }
    }

    public static class InternalStateField {
        protected ActorKelp.TransferredState mark;
        protected Field field;
        protected ActorKelpMergerFunctions.MergerFunction<Object> merger;

        public InternalStateField(Field field, ActorKelp.TransferredState mark) {
            this.field = field;
            this.mark = mark;
            init();
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        protected void init() {
            field.setAccessible(true);
            Class<? extends ActorKelpMergerFunctions.MergerFunction> funcType = mark.mergeFunc();
            try {
                merger = (ActorKelpMergerFunctions.MergerFunction<Object>) funcType.getMethod("get", ActorKelp.MergerOpType.class, Type.class)
                        .invoke(null, mark.mergeType(), field.getGenericType());
            } catch (Exception ex) {
                throw new RuntimeException("invalid merger: " + funcType + " : " + field);
            }
        }

        public Object toState(KryoBuilder.SerializerFunction serializer, Object obj) throws Exception {
            return serializer.copy(field.get(obj));
        }

        public void setState(KryoBuilder.SerializerFunction serializer, Object obj, Object data) throws Exception {
            Object setData = (serializer == null ? data : (serializer.copy(data)));
            setState(obj, setData);
        }

        public void setState(Object obj, Object data) throws Exception {
            field.set(obj, data);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + field + ")";
        }

        public String getTypeAndName() {
            return field.getDeclaringClass().getName() + ":" + field.getName();
        }

        public boolean isMergeOnly() {
            return mark.mergeOnly();
        }

        public void merge(MergingContext context, Object obj, Object data) throws Exception {
            field.set(obj, merger.merge(context, field.get(obj), data));
        }
    }

    public static class MergingContext {
        protected ActorKelp<?> actor;
        protected int left;
        protected int right;

        public MergingContext(ActorKelp<?> actor, int left, int right) {
            this.actor = actor;
            this.left = left;
            this.right = right;
        }

        public ActorKelp<?> getActor() {
            return actor;
        }

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
            return String.format("(%,d:%,d, %.2f:%.2f)",
                    mergeCountLeft(), mergeCountRight(), mergeRatioLeft(), mergeRatioRight());
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


}
