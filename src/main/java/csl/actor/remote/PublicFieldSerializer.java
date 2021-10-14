package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Generics;
import com.esotericsoftware.reflectasm.FieldAccess;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class PublicFieldSerializer<T> extends Serializer<T> {
    protected Class type;
    protected Generics.GenericsHierarchy genericsHierarchy;
    public PublicFieldSerializer(Kryo kryo, Class type) {
        this.type = type;
        genericsHierarchy = new Generics.GenericsHierarchy(type);


    }

    protected void build() {
        if (!type.isInterface()) {
            Class<?> cls = type;
            while (cls != Object.class) {
                for (Field fld : cls.getFields()) {
                    buildField(fld);
                }
                cls = cls.getSuperclass();
            }
        }
    }

    protected void buildField(Field field) {
        int mod = field.getModifiers();
        if (!Modifier.isStatic(mod) &&
                !Modifier.isPublic(mod) &&
                !Modifier.isTransient(mod)) {
            Class<?> declCls = field.getDeclaringClass();
            var genType = new Generics.GenericType(declCls, type, field.getGenericType());
            var fldCls = genType.getType() instanceof Class<?> ?
                    (Class<?>) genType.getType() :
                    field.getType();
            if (Modifier.isPublic(fldCls.getModifiers())) {
                var acc = FieldAccess.get(fldCls);
                int idx = acc.getIndex(field);
                
            }
        }
    }

    @Override
    public void write(Kryo kryo, Output output, T object) {

    }

    @Override
    public T read(Kryo kryo, Input input, Class<? extends T> type) {
        return null;
    }


}
