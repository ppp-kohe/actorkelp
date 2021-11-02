package csl.actor.kelp;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.objenesis.instantiator.basic.ObjectStreamClassInstantiator;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;

public class KryoBuilderKelp11 extends KryoBuilderKelp {
    @Override
    public Kryo build() {
        Kryo kryo = buildKryoGetOrCreate();
        buildKryoInit(kryo);
        buildRegisterBasic(kryo);
        buildRegisterLambda(kryo);
        buildRegisterBasicAdditional(kryo);
        buildRegisterActor(kryo);
        return kryo;
    }
    @Override
    protected void buildKryoInitFields(Kryo kryo) {
//        var conf = new FieldSerializer.FieldSerializerConfig();
//        conf.setFieldsAsAccessible(false);
//        kryo.setDefaultSerializer(new com.esotericsoftware.kryo.SerializerFactory.FieldSerializerFactory(conf));
    }

    @Override
    protected void buildRegisterLambda(Kryo kryo) {
        kryo.register(SerializedLambda.class);
        kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
    }

    @Override
    protected void registerObjectStreamError(Kryo kryo, Class<? extends Serializable> cls, Registration r, Exception ex) {
        r.setInstantiator(new ObjectStreamClassInstantiator<>(cls));
        r.setSerializer(new JavaSerializer());
    }
}
