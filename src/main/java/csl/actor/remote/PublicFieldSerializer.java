package csl.actor.remote;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Generics;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PublicFieldSerializer<T> extends Serializer<T> {
    protected Class<?> type;
    protected Generics.GenericsHierarchy genericsHierarchy;
    protected List<FieldEntry> fields = new ArrayList<>();
    public PublicFieldSerializer(Kryo kryo, Class<?> type) {
        this.type = type;
        genericsHierarchy = new Generics.GenericsHierarchy(type);
        build();
    }

    protected void build() {
        if (!type.isInterface()) {
            Class<?> cls = type;
            while (cls != Object.class) {
                List<FieldEntry> es = new ArrayList<>();
                for (Field fld : cls.getFields()) {
                    FieldEntry e = buildField(fld);
                    if (e != null) {
                        es.add(e);
                    }
                }
                es.sort(Comparator.comparing(FieldEntry::getKey));
                fields.addAll(es);
                cls = cls.getSuperclass();
            }
            ((ArrayList<?>) fields).trimToSize();
        }
    }

    protected FieldEntry buildField(Field field) {
        int mod = field.getModifiers();
        if (!Modifier.isStatic(mod) &&
                Modifier.isPublic(mod) &&
                !Modifier.isTransient(mod)) {
            Class<?> declCls = field.getDeclaringClass();
            var genType = new Generics.GenericType(declCls, type, field.getGenericType());
            var fldCls = genType.getType() instanceof Class<?> ?
                    (Class<?>) genType.getType() :
                    field.getType();
            if (Modifier.isPublic(fldCls.getModifiers())) {
                return new FieldEntry(this, field, genType);
            }
        }
        return null;
    }

    @Override
    public void write(Kryo kryo, Output output, T object) {
        int pop = pushGenerics(kryo);
        for (FieldEntry f : fields) {
            try {
                f.write(kryo, output, object);
            } catch (Exception e) {
                throw new KryoException(e);
            }
        }
        popGenerics(kryo, pop);
    }

    @Override
    public T read(Kryo kryo, Input input, Class<? extends T> type) {
        int pop = pushGenerics(kryo);
        try {
            var o = kryo.newInstance(type);
            kryo.reference(o);
            for (FieldEntry f : fields) {
                f.read(kryo, input, o, type);
            }
            return o;
        } catch (Throwable ex) {
            throw new KryoException("" + type.getName(), ex);
        } finally {
            popGenerics(kryo, pop);
        }
    }

    protected int pushGenerics(Kryo kryo) {
        Generics.GenericType[] ts = kryo.getGenerics().nextGenericTypes();
        if (ts == null) {
            return 0;
        } else {
            return kryo.getGenerics().pushTypeVariables(genericsHierarchy, ts);
        }
    }

    protected void popGenerics(Kryo kryo, int pop) {
        Generics g = kryo.getGenerics();
        if (pop > 0) {
            g.popTypeVariables(pop);
        }
        g.popGenericType();
    }

    public static class FieldEntry {
        protected PublicFieldSerializer<?> fieldSerializer;
        protected Field field;
        protected Generics.GenericType genericType;

        public FieldEntry(PublicFieldSerializer<?> serializer, Field field, Generics.GenericType genericType) {
            this.fieldSerializer = serializer;
            this.field = field;
            this.genericType = genericType;
        }

        public String getKey() {
            return field.getName();
        }


        public void write(Kryo kryo, Output output, Object obj) throws Exception {
            var v = field.get(obj);
            Class<?> conType = resolveValueType(kryo);
            if (conType == null) {
                if (v == null) {
                    kryo.writeClass(output, null);
                    return;
                } else {
                    Registration reg = kryo.writeClass(output, v.getClass());
                    kryo.getGenerics().pushGenericType(genericType);
                    kryo.writeObject(output, v, reg.getSerializer());
                }
            } else {
                Serializer<?> serializer = kryo.getSerializer(conType);
                kryo.getGenerics().pushGenericType(genericType);
                kryo.writeObjectOrNull(output, v, serializer);
            }
            kryo.getGenerics().popGenericType();
        }

        protected Class<?> resolveValueType(Kryo kryo) {
            Class<?> conType = genericType.resolve(kryo.getGenerics());
            if (conType != null && kryo.isFinal(conType)) {
                return conType;
            } else {
                return null;
            }
        }

        public void read(Kryo kryo, Input input, Object object, Class<?> type) throws Exception {
            Class<?> conType = resolveValueType(kryo);
            Object v;
            if (conType == null) {
                Registration reg = kryo.readClass(input);
                if (reg == null) {
                    field.set(object, null);
                    return;
                } else {
                    kryo.getGenerics().pushGenericType(genericType);
                    v = kryo.readObject(input, (Class<?>) reg.getType(), reg.getSerializer());
                }
            } else {
                kryo.getGenerics().pushGenericType(genericType);
                v = kryo.readObjectOrNull(input, conType, kryo.getSerializer(conType));
            }
            kryo.getGenerics().popGenericType();
            field.set(object, v);
        }
    }


    public static class SerializerFactoryPublicField<T> implements SerializerFactory<PublicFieldSerializer<T>> {
        @Override
        public PublicFieldSerializer<T> newSerializer(Kryo kryo, Class type) {
            return new PublicFieldSerializer<>(kryo, type);
        }

        @Override
        public boolean isSupported(Class type) {
            return true;
        }
    }
}
