package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.File;
import java.nio.*;
import java.util.*;
import java.util.regex.Pattern;

public class KyroBaseSerializer {
    public static void register(Kryo kryo) {
        kryo.addDefaultSerializer(File.class, new FileSerializer());
        kryo.addDefaultSerializer(Pattern.class, new PatternSerializer());
        kryo.addDefaultSerializer(EnumMap.class, new EnumMapSerializer());
        kryo.addDefaultSerializer(EnumSet.class, new EnumSetSerializer());
        kryo.addDefaultSerializer(ByteBuffer.class, new BufferSerializerByte());
        kryo.addDefaultSerializer(IntBuffer.class, new BufferSerializerInt());
        kryo.addDefaultSerializer(CharBuffer.class, new BufferSerializerChar());
        kryo.addDefaultSerializer(LongBuffer.class, new BufferSerializerLong());
        kryo.addDefaultSerializer(ShortBuffer.class, new BufferSerializerShort());
        kryo.addDefaultSerializer(DoubleBuffer.class, new BufferSerializerDouble());
        kryo.addDefaultSerializer(FloatBuffer.class, new BufferSerializerFloat());

    }

    public static class FileSerializer extends Serializer<File> {
        protected boolean needConvSeparator;
        public FileSerializer() {
            super(true, true);
            needConvSeparator = !File.separator.equals("/");
        }
        @Override
        public void write(Kryo kryo, Output output, File object) {
            output.writeString(object == null ? null : encode(object.getPath()));
        }

        @Override
        public File read(Kryo kryo, Input input, Class<? extends File> type) {
            String path = input.readString();
            return path == null ? null : new File(decode(path));
        }

        protected String encode(String path) {
            return path.replaceAll(Pattern.quote(File.separator), "/");
        }
        protected String decode(String path) {
            return path.replaceAll("/", File.separator);
        }
    }

    public static class PatternSerializer extends Serializer<Pattern> {
        public PatternSerializer() {
            super(true, true);
        }

        @Override
        public void write(Kryo kryo, Output output, Pattern object) {
            output.writeString(object == null ? null : object.pattern());
            if (object != null) {
                output.writeInt(object.flags());
            }
        }

        @Override
        public Pattern read(Kryo kryo, Input input, Class<? extends Pattern> type) {
            String s = input.readString();
            return s == null ? null : Pattern.compile(s, input.readInt());
        }
    }

    public static class EnumMapSerializer extends Serializer<EnumMap<?,?>> {
        public EnumMapSerializer() {
            super(true, false);
        }

        protected JavaSerializer fallback = new JavaSerializer();
        @Override
        public void write(Kryo kryo, Output output, EnumMap<?, ?> object) {
            if (object == null) {
                output.write(0);
            } else if (object.isEmpty()) {
                output.write(1);
                fallback.write(kryo, output, object);
            } else {
                output.write(2);
                int size = object.size();
                output.writeInt(size);
                Class<?> eCls = null;
                for (Map.Entry<?,?> e : object.entrySet()) {
                    Enum<?> k = (Enum<?>) e.getKey();
                    if (eCls == null) {
                        eCls = k.getDeclaringClass();
                        kryo.writeClass(output, eCls);
                    }
                    output.writeInt(k.ordinal() + 1, true);
                    kryo.writeClassAndObject(output, e.getValue());
                }
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public EnumMap<?, ?> read(Kryo kryo, Input input, Class<? extends EnumMap<?, ?>> type) {
            int tag = input.read();
            if (tag == 0) {
                return null;
            } else if (tag == 1) {
                return (EnumMap<?, ?>) fallback.read(kryo, input, type);
            } else {
                int size = input.readInt();
                Registration keyReg = kryo.readClass(input);
                EnumMap m = new EnumMap(keyReg.getType());
                try {
                    Enum[] keys = (Enum[]) keyReg.getType().getMethod("values").invoke(null);
                    for (int i = 0; i < size; ++i) {
                        int ord = input.readInt(true) - 1;
                        m.put(keys[ord],
                                kryo.readClassAndObject(input));
                    }
                    return m;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    public static class EnumSetSerializer extends Serializer<EnumSet<?>> {
        protected JavaSerializer fallback = new JavaSerializer();
        public EnumSetSerializer() {
            super(true, true);
        }

        @Override
        public void write(Kryo kryo, Output output, EnumSet<?> object) {
            if (object == null) {
                output.write(0);
            } else if (object.isEmpty()) {
                output.write(1);
                fallback.write(kryo, output, object);
            } else {
                output.write(2);
                int size = object.size();
                output.writeInt(size);
                Class<?> eCls = null;
                for (Enum<?> k : object) {
                    if (eCls == null) {
                        eCls = k.getDeclaringClass();
                        kryo.writeClass(output, eCls);
                    }
                    output.writeInt(k.ordinal() + 1, true);
                }
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public EnumSet<?> read(Kryo kryo, Input input, Class<? extends EnumSet<?>> type) {
            int tag = input.read();
            if (tag == 0) {
                return null;
            } else if (tag == 1) {
                return (EnumSet<?>) fallback.read(kryo, input, type);
            } else {
                int size = input.readInt();
                Registration keyReg = kryo.readClass(input);
                List<Enum> ls = new ArrayList<>(size);
                try {
                    Enum[] keys = (Enum[]) keyReg.getType().getMethod("values").invoke(null);
                    for (int i = 0; i < size; ++i) {
                        int ord = input.readInt(true) - 1;
                        ls.add(keys[ord]);
                    }
                    return EnumSet.copyOf(ls);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    public abstract static class BufferSerializer<BufferType extends Buffer> extends Serializer<BufferType> {
        public BufferSerializer() {
            super(true, false);
        }

        @Override
        public void write(Kryo kryo, Output output, BufferType object) {
            if (object == null) {
                output.writeInt(-1);
            }
            int pos = object.position();
            int lim = object.limit();
            int cap = object.capacity();
            //we cannot access mark info.
            output.writeInt(pos);
            output.writeInt(lim);
            output.writeInt(cap);
            boolean array = object.hasArray();
            output.writeBoolean(array);
            if (array) {
                //no support for arrayOffset
                kryo.writeClassAndObject(output, object.array());
                if (object instanceof ByteBuffer) {
                    output.writeBoolean(((ByteBuffer) object).order().equals(ByteOrder.BIG_ENDIAN));
                }
            } else {
                writeNoArray(kryo, output, object, pos, lim, cap);
                object.position(pos);
            }
        }

        protected abstract void writeNoArray(Kryo kryo, Output output, BufferType object, int pos, int lim, int cap);

        @Override
        @SuppressWarnings("unchecked")
        public BufferType read(Kryo kryo, Input input, Class<? extends BufferType> type) {
            int pos = input.readInt();
            if (pos == -1) {
                return null;
            }
            int lim = input.readInt();
            int cap = input.readInt();
            boolean array = input.readBoolean();
            if (array) {
                Object arrayBody = kryo.readClassAndObject(input);
                if (arrayBody instanceof int[]) {
                    return (BufferType) IntBuffer.wrap((int[]) arrayBody, pos, lim - pos);
                } else if (arrayBody instanceof char[]) {
                    return (BufferType) CharBuffer.wrap((char[]) arrayBody, pos, lim - pos);
                } else if (arrayBody instanceof byte[]) {
                    boolean big = input.readBoolean();
                    return (BufferType) ByteBuffer.wrap((byte[]) arrayBody, pos, lim - pos)
                            .order(big ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
                } else if (arrayBody instanceof short[]) {
                    return (BufferType) ShortBuffer.wrap((short[]) arrayBody, pos, lim - pos);
                } else if (arrayBody instanceof long[]) {
                    return (BufferType) LongBuffer.wrap((long[]) arrayBody, pos, lim - pos);
                } else if (arrayBody instanceof float[]) {
                    return (BufferType) FloatBuffer.wrap((float[]) arrayBody, pos, lim - pos);
                } else if (arrayBody instanceof double[]) {
                    return (BufferType) DoubleBuffer.wrap((double[]) arrayBody, pos, lim - pos);
                } else {
                    throw new RuntimeException("? " + arrayBody);
                }
            } else {
                var buf = readNoArray(kryo, input, type, pos, lim, cap);
                buf.position(pos);
                buf.limit(lim);
                return buf;
            }
        }

        protected abstract BufferType readNoArray(Kryo kryo, Input input, Class<? extends BufferType> type, int pos, int lim, int cap);

        protected void writeByteBufferArray(Kryo kryo, Output output, ByteBuffer buffer) {
            output.writeInt(buffer.remaining());
            output.write(buffer.array(), buffer.position(), buffer.remaining());
            output.writeBoolean(buffer.order().equals(ByteOrder.BIG_ENDIAN));
        }
        protected ByteBuffer readByteBufferArray(Kryo kryo, Input input) {
            byte[] tmp = new byte[input.readInt()];
            input.read(tmp);
            return ByteBuffer.wrap(tmp, 0, tmp.length)
                    .order(input.readBoolean() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        }
    }

    public static class BufferSerializerByte extends BufferSerializer<ByteBuffer> {
        @Override
        protected void writeNoArray(Kryo kryo, Output output, ByteBuffer object, int pos, int lim, int cap) {
            byte[] tmp = new byte[lim - pos];
            object.get(tmp);
            output.write(tmp);
            output.writeBoolean(object.order().equals(ByteOrder.BIG_ENDIAN));
        }

        @Override
        protected ByteBuffer readNoArray(Kryo kryo, Input input, Class<? extends ByteBuffer> type, int pos, int lim, int cap) {
            byte[] tmp = new byte[lim - pos];
            input.read(tmp);
            ByteOrder order = input.readBoolean() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
            ByteBuffer buf = ByteBuffer.allocateDirect(cap).order(order);
            buf.position(pos);
            buf.put(tmp);
            return null;
        }
    }

    public static class BufferSerializerInt extends BufferSerializer<IntBuffer> {
        @Override
        protected void writeNoArray(Kryo kryo, Output output, IntBuffer object, int pos, int lim, int cap) {
            var nBuf = ByteBuffer.allocate((lim - pos) * this.unitSize()).order(object.order());
            var buf = nBuf.asIntBuffer();
            buf.put(object);
            writeByteBufferArray(kryo, output, nBuf);
        }
        protected int unitSize() {
            return 4;
        }

        @Override
        protected IntBuffer readNoArray(Kryo kryo, Input input, Class<? extends IntBuffer> type, int pos, int lim, int cap) {
            var buf = readByteBufferArray(kryo, input).asIntBuffer();
            if (pos != 0 || buf.capacity() != cap) {
                var res = IntBuffer.allocate(cap);
                res.position(pos).limit(lim)
                        .put(buf.position(0).limit(lim - pos));
                return res;
            } else {
                return buf;
            }
        }
    }
    public static class BufferSerializerChar extends BufferSerializer<CharBuffer> {
        @Override
        protected void writeNoArray(Kryo kryo, Output output, CharBuffer object, int pos, int lim, int cap) {
            var nBuf = ByteBuffer.allocate((lim - pos) * this.unitSize()).order(object.order());
            var buf = nBuf.asCharBuffer();
            buf.put(object);
            writeByteBufferArray(kryo, output, nBuf);
        }
        protected int unitSize() {
            return 4;
        }

        @Override
        protected CharBuffer readNoArray(Kryo kryo, Input input, Class<? extends CharBuffer> type, int pos, int lim, int cap) {
            var buf = readByteBufferArray(kryo, input).asCharBuffer();
            if (pos != 0 || buf.capacity() != cap) {
                var res = CharBuffer.allocate(cap);
                res.position(pos).limit(lim)
                        .put(buf.position(0).limit(lim - pos));
                return res;
            } else {
                return buf;
            }
        }
    }
    public static class BufferSerializerLong extends BufferSerializer<LongBuffer> {
        @Override
        protected void writeNoArray(Kryo kryo, Output output, LongBuffer object, int pos, int lim, int cap) {
            var nBuf = ByteBuffer.allocate((lim - pos) * this.unitSize()).order(object.order());
            var buf = nBuf.asLongBuffer();
            buf.put(object);
            writeByteBufferArray(kryo, output, nBuf);
        }
        protected int unitSize() {
            return 4;
        }

        @Override
        protected LongBuffer readNoArray(Kryo kryo, Input input, Class<? extends LongBuffer> type, int pos, int lim, int cap) {
            var buf = readByteBufferArray(kryo, input).asLongBuffer();
            if (pos != 0 || buf.capacity() != cap) {
                var res = LongBuffer.allocate(cap);
                res.position(pos).limit(lim)
                        .put(buf.position(0).limit(lim - pos));
                return res;
            } else {
                return buf;
            }
        }
    }
    public static class BufferSerializerShort extends BufferSerializer<ShortBuffer> {
        @Override
        protected void writeNoArray(Kryo kryo, Output output, ShortBuffer object, int pos, int lim, int cap) {
            var nBuf = ByteBuffer.allocate((lim - pos) * this.unitSize()).order(object.order());
            var buf = nBuf.asShortBuffer();
            buf.put(object);
            writeByteBufferArray(kryo, output, nBuf);
        }
        protected int unitSize() {
            return 4;
        }

        @Override
        protected ShortBuffer readNoArray(Kryo kryo, Input input, Class<? extends ShortBuffer> type, int pos, int lim, int cap) {
            var buf = readByteBufferArray(kryo, input).asShortBuffer();
            if (pos != 0 || buf.capacity() != cap) {
                var res = ShortBuffer.allocate(cap);
                res.position(pos).limit(lim)
                        .put(buf.position(0).limit(lim - pos));
                return res;
            } else {
                return buf;
            }
        }
    }
    public static class BufferSerializerDouble extends BufferSerializer<DoubleBuffer> {
        @Override
        protected void writeNoArray(Kryo kryo, Output output, DoubleBuffer object, int pos, int lim, int cap) {
            var nBuf = ByteBuffer.allocate((lim - pos) * this.unitSize()).order(object.order());
            var buf = nBuf.asDoubleBuffer();
            buf.put(object);
            writeByteBufferArray(kryo, output, nBuf);
        }
        protected int unitSize() {
            return 4;
        }

        @Override
        protected DoubleBuffer readNoArray(Kryo kryo, Input input, Class<? extends DoubleBuffer> type, int pos, int lim, int cap) {
            var buf = readByteBufferArray(kryo, input).asDoubleBuffer();
            if (pos != 0 || buf.capacity() != cap) {
                var res = DoubleBuffer.allocate(cap);
                res.position(pos).limit(lim)
                        .put(buf.position(0).limit(lim - pos));
                return res;
            } else {
                return buf;
            }
        }
    }
    public static class BufferSerializerFloat extends BufferSerializer<FloatBuffer> {
        @Override
        protected void writeNoArray(Kryo kryo, Output output, FloatBuffer object, int pos, int lim, int cap) {
            var nBuf = ByteBuffer.allocate((lim - pos) * this.unitSize()).order(object.order());
            var buf = nBuf.asFloatBuffer();
            buf.put(object);
            writeByteBufferArray(kryo, output, nBuf);
        }
        protected int unitSize() {
            return 4;
        }

        @Override
        protected FloatBuffer readNoArray(Kryo kryo, Input input, Class<? extends FloatBuffer> type, int pos, int lim, int cap) {
            var buf = readByteBufferArray(kryo, input).asFloatBuffer();
            if (pos != 0 || buf.capacity() != cap) {
                var res = FloatBuffer.allocate(cap);
                res.position(pos).limit(lim)
                        .put(buf.position(0).limit(lim - pos));
                return res;
            } else {
                return buf;
            }
        }
    }
}
