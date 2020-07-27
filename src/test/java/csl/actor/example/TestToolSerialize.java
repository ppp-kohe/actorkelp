package csl.actor.example;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.remote.KryoBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;

public class TestToolSerialize {

    public <E> void writeRead(KryoBuilder.SerializerFunction k, E obj) {
        writeRead(k, obj, true, Objects::equals);
    }

    public <E> void writeRead(KryoBuilder.SerializerFunction k, E obj, BiPredicate<E,E> p) {
        writeRead(k, obj, true, p);
    }

    @SuppressWarnings("unchecked")
    public <E> void writeRead(KryoBuilder.SerializerFunction k, E obj, boolean printAll, BiPredicate<E,E> p) {
        System.err.println("----------- " + (obj == null ? "null" : obj.getClass().getName()));
        byte[] data = write(o -> k.write(o, obj));
        if (printAll) {
            print(data);
        } else {
            System.err.print(String.format("[%,d] ", data.length));
            print(Arrays.copyOf(data, Math.min(30, data.length)));
        }
        E r = (E) read(data, k::read);
        TestTool.assertEquals("writeRead", obj, r, p);
    }

    public byte[] write(Consumer<Output> p) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            Output output = new Output(out);
            p.accept(output);
            output.flush();
            out.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return out.toByteArray();
    }

    public void print(byte[] data) {
        StringBuilder buf = new StringBuilder();
        for (byte b : data) {
            buf.append(String.format("%2h", 0xFF & b)).append(":");
            char c = (char) b;
            if (Character.isLetterOrDigit(c)) {
                buf.append("'");
                buf.append(c);
                buf.append("'");
            } else {
                buf.append(b);
            }
            buf.append("  ");
        }
        System.err.println(buf);
    }

    public Object read(byte[] d, Function<Input, Object> gen) {
        Input input = new Input(new ByteArrayInputStream(d));
        return gen.apply(input);
    }
}
