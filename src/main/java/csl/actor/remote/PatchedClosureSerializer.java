package csl.actor.remote;
//the code is copied from ClosureSerializer of the kryo project in order to patch a line in the write method

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;

import static com.esotericsoftware.kryo.util.Util.className;

public class PatchedClosureSerializer extends Serializer {
    static private Method readResolve;

    public PatchedClosureSerializer () {
        if (readResolve == null) {
            try {
                readResolve = SerializedLambda.class.getDeclaredMethod("readResolve");
                readResolve.setAccessible(true);
            } catch (Exception ex) {
                throw new KryoException("Unable to obtain SerializedLambda#readResolve via reflection.", ex);
            }
        }
    }

    public void write (Kryo kryo, Output output, Object object) {
        SerializedLambda serializedLambda = toSerializedLambda(object);
        int count = serializedLambda.getCapturedArgCount();
        output.writeVarInt(count, true);
        for (int i = 0; i < count; i++)
            kryo.writeClassAndObject(output, serializedLambda.getCapturedArg(i));
        try {
            kryo.writeClass(output, Class.forName(serializedLambda.getCapturingClass().replace('/', '.')));
        } catch (ClassNotFoundException ex) {
            throw new KryoException("Error writing closure.", ex);
        }
        output.writeString(serializedLambda.getFunctionalInterfaceClass());
        output.writeString(serializedLambda.getFunctionalInterfaceMethodName());
        output.writeString(serializedLambda.getFunctionalInterfaceMethodSignature());
        output.writeVarInt(serializedLambda.getImplMethodKind(), true);
        output.writeString(serializedLambda.getImplClass());
        output.writeString(serializedLambda.getImplMethodName());
        output.writeString(serializedLambda.getImplMethodSignature());
        output.writeString(serializedLambda.getInstantiatedMethodType());
    }

    public Object read (Kryo kryo, Input input, Class type) {
        int count = input.readVarInt(true);
        Object[] capturedArgs = new Object[count];
        for (int i = 0; i < count; i++)
            capturedArgs[i] = kryo.readClassAndObject(input);
        SerializedLambda serializedLambda = new SerializedLambda(kryo.readClass(input).getType(), input.readString(),
                input.readString(), input.readString(), input.readVarInt(true), input.readString(), input.readString(),
                input.readString(), input.readString(), capturedArgs);
        try {
            return readResolve.invoke(serializedLambda);
        } catch (Exception ex) {
            throw new KryoException("Error reading closure.", ex);
        }
    }

    public Object copy (Kryo kryo, Object original) {
        try {
            return readResolve.invoke(toSerializedLambda(original));
        } catch (Exception ex) {
            throw new KryoException("Error copying closure.", ex);
        }
    }

    private SerializedLambda toSerializedLambda (Object object) {
        Object replacement;
        try {
            Method writeReplace = object.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);
            replacement = writeReplace.invoke(object);
        } catch (Exception ex) {
            if (object instanceof Serializable) throw new KryoException("Error serializing closure.", ex);
            throw new KryoException("Closure must implement java.io.Serializable.", ex);
        }
        try {
            return (SerializedLambda)replacement;
        } catch (Exception ex) {
            throw new KryoException(
                    "writeReplace must return a SerializedLambda: " + (replacement == null ? null : className(replacement.getClass())),
                    ex);
        }
    }
}