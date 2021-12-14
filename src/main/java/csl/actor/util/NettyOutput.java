package csl.actor.util;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

public class NettyOutput extends Output {
    protected final ByteBuf output;

    public NettyOutput(ByteBuf output) {
        this.output = output;
    }

    @Override
    protected boolean require(int required) throws KryoException {
        return true;
    }

    @Override
    public int position() {
        return 0;
    }

    @Override
    public void setPosition(int position) {}


    @Override
    public void reset() {
        total = 0;
    }

    @Override
    public long total() {
        return total;
    }

    @Override
    public void flush() throws KryoException {
        if (output instanceof Flushable) {
            try {
                ((Flushable) output).flush();
            } catch (Exception ex) {
                throw new KryoException(ex);
            }
        }
    }

    @Override
    public void close() throws KryoException {
        if (output instanceof Closeable) {
            try {
                ((Closeable) output).close();
            } catch (Exception ex) {
                throw new KryoException(ex);
            }
        }
    }

    @Override
    public void write(int value) throws KryoException {
        output.writeByte(value);
        total += 1;
    }

    @Override
    public void writeByte(int value) throws KryoException {
        write(value);
    }

    @Override
    public void writeByte(byte value) throws KryoException {
        write(value);
    }

    @Override
    public void writeBytes(byte[] bytes, int offset, int count) throws KryoException {
        output.writeBytes(bytes, offset, count);
        total += count;
    }

    @Override
    public void writeInt(int value) throws KryoException {
        byte b1 = (byte) value;
        byte b2 = (byte) (value >> 8);
        byte b3 = (byte) (value >> 16);
        byte b4 = (byte) (value >> 24);
        output.writeByte(b1);
        output.writeByte(b2);
        output.writeByte(b3);
        output.writeByte(b4);
        total += 4;
    }

    @Override
    public int writeVarInt(int value, boolean optimizePositive) throws KryoException {
        if (!optimizePositive) value = (value << 1) ^ (value >> 31);
        if (value >>> 7 == 0) {
            writeByte((byte) value);
            return 1;
        }
        if (value >>> 14 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7);
            output.writeByte(b1);
            output.writeByte(b2);
            total += 2;
            return 2;
        }
        if (value >>> 21 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7 | 0x80);
            byte b3 = (byte) (value >>> 14);
            output.writeByte(b1);
            output.writeByte(b2);
            output.writeByte(b3);
            total += 3;
            return 3;
        }
        if (value >>> 28 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7 | 0x80);
            byte b3 = (byte) (value >>> 14 | 0x80);
            byte b4 = (byte) (value >>> 21);
            output.writeByte(b1);
            output.writeByte(b2);
            output.writeByte(b3);
            output.writeByte(b4);
            total += 4;
            return 4;
        }
        byte b1 = (byte) ((value & 0x7F) | 0x80);
        byte b2 = (byte) (value >>> 7 | 0x80);
        byte b3 = (byte) (value >>> 14 | 0x80);
        byte b4 = (byte) (value >>> 21 | 0x80);
        byte b5 = (byte) (value >>> 28);
        output.writeByte(b1);
        output.writeByte(b2);
        output.writeByte(b3);
        output.writeByte(b4);
        output.writeByte(b5);
        total += 5;
        return 5;
    }
    
    @Override
    public int writeVarIntFlag(boolean flag, int value, boolean optimizePositive) throws KryoException {
        if (!optimizePositive) value = (value << 1) ^ (value >> 31);
        int first = (value & 0x3F) | (flag ? 0x80 : 0);
        if (value >>> 6 == 0) {
            writeByte((byte) first);
            return 1;
        }
        if (value >>> 13 == 0) {
            byte b1 = (byte) (first | 0x40);
            byte b2 = (byte) (value >>> 6);
            output.writeByte(b1);
            output.writeByte(b2);
            total += 2;
            return 2;
        }
        if (value >>> 20 == 0) {
            byte b1 = (byte) (first | 0x40);
            byte b2 = (byte) ((value >>> 6) | 0x80);
            byte b3 = (byte) (value >>> 13);
            output.writeByte(b1);
            output.writeByte(b2);
            output.writeByte(b3);
            total += 3;
            return 3;
        }
        if (value >>> 27 == 0) {
            byte b1 = (byte) (first | 0x40);
            byte b2 = (byte) ((value >>> 6) | 0x80);
            byte b3 = (byte) ((value >>> 13) | 0x80);
            byte b4 = (byte) (value >>> 20);
            output.writeByte(b1);
            output.writeByte(b2);
            output.writeByte(b3);
            output.writeByte(b4);
            total += 4;
            return 4;
        }
        byte b1 = (byte) (first | 0x40);
        byte b2 = (byte) ((value >>> 6) | 0x80);
        byte b3 = (byte) ((value >>> 13) | 0x80);
        byte b4 = (byte) ((value >>> 20) | 0x80);
        byte b5 = (byte) (value >>> 27);
        output.writeByte(b1);
        output.writeByte(b2);
        output.writeByte(b3);
        output.writeByte(b4);
        output.writeByte(b5);
        total += 5;
        return 5;
    }


    @Override
    public void writeLong(long value) throws KryoException {
        byte b1 = (byte) value;
        byte b2 = (byte) (value >>> 8);
        byte b3 = (byte) (value >>> 16);
        byte b4 = (byte) (value >>> 24);
        byte b5 = (byte) (value >>> 32);
        byte b6 = (byte) (value >>> 40);
        byte b7 = (byte) (value >>> 48);
        byte b8 = (byte) (value >>> 56);
        output.writeByte(b1);
        output.writeByte(b2);
        output.writeByte(b3);
        output.writeByte(b4);
        output.writeByte(b5);
        output.writeByte(b6);
        output.writeByte(b7);
        output.writeByte(b8);
        total += 8;
    }

    @Override
    public int writeVarLong(long value, boolean optimizePositive) throws KryoException {
        
        if (!optimizePositive) value = (value << 1) ^ (value >> 63);
        if (value >>> 7 == 0) {
            writeByte((byte) value);
            return 1;
        }
        if (value >>> 14 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7);

            output.writeByte(b1);
            output.writeByte(b2);
            total += 2;
            return 2;
        }
        if (value >>> 21 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7 | 0x80);
            byte b3 = (byte) (value >>> 14);

            output.writeByte(b1);
            output.writeByte(b2);
            output.writeByte(b3);
            total += 3;
            return 3;
        }
        if (value >>> 28 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7 | 0x80);
            byte b3 = (byte) (value >>> 14 | 0x80);
            byte b4 = (byte) (value >>> 21);

            output.writeByte(b1);
            output.writeByte(b2);
            output.writeByte(b3);
            output.writeByte(b4);
            total += 4;
            return 4;
        }
        if (value >>> 35 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7 | 0x80);
            byte b3 = (byte) (value >>> 14 | 0x80);
            byte b4 = (byte) (value >>> 21 | 0x80);
            byte b5 = (byte) (value >>> 28);

            output.writeByte(b1);
            output.writeByte(b2);
            output.writeByte(b3);
            output.writeByte(b4);
            output.writeByte(b5);
            total += 5;
            return 5;
        }
        if (value >>> 42 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7 | 0x80);
            byte b3 = (byte) (value >>> 14 | 0x80);
            byte b4 = (byte) (value >>> 21 | 0x80);
            byte b5 = (byte) (value >>> 28 | 0x80);
            byte b6 = (byte) (value >>> 35);

            output.writeByte(b1);
            output.writeByte(b2);
            output.writeByte(b3);
            output.writeByte(b4);
            output.writeByte(b5);
            output.writeByte(b6);
            total += 6;
            return 6;
        }
        if (value >>> 49 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7 | 0x80);
            byte b3 = (byte) (value >>> 14 | 0x80);
            byte b4 = (byte) (value >>> 21 | 0x80);
            byte b5 = (byte) (value >>> 28 | 0x80);
            byte b6 = (byte) (value >>> 35 | 0x80);
            byte b7 = (byte) (value >>> 42);

            output.writeByte(b1);
            output.writeByte(b2);
            output.writeByte(b3);
            output.writeByte(b4);
            output.writeByte(b5);
            output.writeByte(b6);
            output.writeByte(b7);
            total += 7;
            return 7;
        }
        if (value >>> 56 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7 | 0x80);
            byte b3 = (byte) (value >>> 14 | 0x80);
            byte b4 = (byte) (value >>> 21 | 0x80);
            byte b5 = (byte) (value >>> 28 | 0x80);
            byte b6 = (byte) (value >>> 35 | 0x80);
            byte b7 = (byte) (value >>> 42 | 0x80);
            byte b8 = (byte) (value >>> 49);

            output.writeByte(b1);
            output.writeByte(b2);
            output.writeByte(b3);
            output.writeByte(b4);
            output.writeByte(b5);
            output.writeByte(b6);
            output.writeByte(b7);
            output.writeByte(b8);
            total += 8;
            return 8;
        }
        byte b1 = (byte) ((value & 0x7F) | 0x80);
        byte b2 = (byte) (value >>> 7 | 0x80);
        byte b3 = (byte) (value >>> 14 | 0x80);
        byte b4 = (byte) (value >>> 21 | 0x80);
        byte b5 = (byte) (value >>> 28 | 0x80);
        byte b6 = (byte) (value >>> 35 | 0x80);
        byte b7 = (byte) (value >>> 42 | 0x80);
        byte b8 = (byte) (value >>> 49 | 0x80);
        byte b9 = (byte) (value >>> 56);

        output.writeByte(b1);
        output.writeByte(b2);
        output.writeByte(b3);
        output.writeByte(b4);
        output.writeByte(b5);
        output.writeByte(b6);
        output.writeByte(b7);
        output.writeByte(b8);
        output.writeByte(b9);
        total += 9;
        return 9;
    }

    @Override
    public void writeFloat(float value) throws KryoException {
        writeInt(Float.floatToIntBits(value));
    }

    @Override
    public void writeDouble(double value) throws KryoException {
        writeLong(Double.doubleToLongBits(value));
    }

    @Override
    public void writeShort(int value) throws KryoException {
        byte b1 = (byte) value;
        byte b2 = (byte) (value >>> 8);

        output.writeByte(b1);
        output.writeByte(b2);
        total += 2;
    }

    @Override
    public void writeChar(char value) throws KryoException {
        byte b1 = (byte) value;
        byte b2 = (byte) (value >>> 8);

        output.writeByte(b1);
        output.writeByte(b2);
        total += 2;
    }

    @Override
    public void writeBoolean(boolean value) throws KryoException {
        byte b1 = (byte) (value ? 1 : 0);
        writeByte(b1);
    }

    @Override
    public void writeString(String value) throws KryoException {
        if (value == null) {
            writeByte(0x80);
            return;
        }
        int charCount = value.length();
        if (charCount == 0) {
            writeByte(1 | 0x80);
            return;
        }
        outer:
        if (charCount > 1 && charCount <= 32) {
            for (int i = 0; i < charCount; i++)
                if (value.charAt(i) > 127) break outer;
            if (!require(charCount)) {
                writeAscii_slow(value, charCount);
            } else {
                for (int i = 0, n = value.length(); i < n - 1; ++i)
                    output.writeByte((byte) value.charAt(i));
                output.writeByte((byte) (((byte) value.charAt(value.length() - 1)) | 0x80));
                total += value.length();
            }
            return;
        }
        writeVarIntFlag(true, charCount + 1, true);
        int charIndex = 0;
        while (true) {
            int c = value.charAt(charIndex);
            if (c > 127) break;
            output.writeByte((byte) c);
            charIndex++;
            total += 1;
            if (charIndex == charCount) {
                return;
            }
        }
        if (charIndex < charCount) writeUtf8_slow(value, charCount, charIndex);
    }

    @Override
    public void writeAscii(String value) throws KryoException {
        if (value == null) {
            writeByte(0x80);
            return;
        }
        int charCount = value.length();
        if (charCount == 0) {
            writeByte(1 | 0x80);
            return;
        }

        if (!require(charCount)) {
            writeAscii_slow(value, charCount);

        } else {
            for (int i = 0, n = value.length(); i < n - 1; ++i)
                output.writeByte((byte) value.charAt(i));
            output.writeByte((byte) (((byte) value.charAt(value.length() - 1)) | 0x80));
            total += value.length();
        }
    }

    private void writeUtf8_slow(String value, int charCount, int charIndex) {
        for (; charIndex < charCount; charIndex++) {
            int c = value.charAt(charIndex);
            if (c <= 0x007F)
                writeByte((byte) c);
            else if (c > 0x07FF) {
                writeByte((byte) (0xE0 | c >> 12 & 0x0F));
                writeByte((byte) (0x80 | c >> 6 & 0x3F));
                writeByte((byte) (0x80 | c & 0x3F));
            } else {
                writeByte((byte) (0xC0 | c >> 6 & 0x1F));
                writeByte((byte) (0x80 | c & 0x3F));
            }
        }
    }
    private void writeAscii_slow(String value, int charCount) throws KryoException {
        if (charCount == 0) return;
        byte[] chars = new byte[charCount];
        value.getBytes(0, charCount, chars, 0);
        chars[charCount - 1] = (byte) (chars[charCount - 1] | 0x80); //end mark
        writeBytes(chars, 0, charCount);
    }

    @Override
    public void writeInts(int[] array, int offset, int count) throws KryoException {
        for (int n = offset + count; offset < n; offset++) {
            int value = array[offset];
            output.writeByte((byte)value);
            output.writeByte((byte)(value >> 8));
            output.writeByte((byte)(value >> 16));
            output.writeByte((byte)(value >> 24));
            total += 4;
        }
    }

    @Override
    public void writeLongs(long[] array, int offset, int count) throws KryoException {
        for (int n = offset + count; offset < n; offset++) {
            long value = array[offset];
            output.writeByte((byte)value);
            output.writeByte((byte)(value >>> 8));
            output.writeByte((byte)(value >>> 16));
            output.writeByte((byte)(value >>> 24));
            output.writeByte((byte)(value >>> 32));
            output.writeByte((byte)(value >>> 40));
            output.writeByte((byte)(value >>> 48));
            output.writeByte((byte)(value >>> 56));
            total += 8;
        }
    }

    @Override
    public void writeFloats(float[] array, int offset, int count) throws KryoException {
        for (int n = offset + count; offset < n; offset++) {
            int value = Float.floatToIntBits(array[offset]);
            output.writeByte((byte)value);
            output.writeByte((byte)(value >> 8));
            output.writeByte((byte)(value >> 16));
            output.writeByte((byte)(value >> 24));
            total += 4;
        }
    }

    @Override
    public void writeDoubles(double[] array, int offset, int count) throws KryoException {
        for (int n = offset + count; offset < n; offset++) {
            long value = Double.doubleToLongBits(array[offset]);
            output.writeByte((byte)value);
            output.writeByte((byte)(value >>> 8));
            output.writeByte((byte)(value >>> 16));
            output.writeByte((byte)(value >>> 24));
            output.writeByte((byte)(value >>> 32));
            output.writeByte((byte)(value >>> 40));
            output.writeByte((byte)(value >>> 48));
            output.writeByte((byte)(value >>> 56));
            total += 8;
        }
    }

    @Override
    public void writeShorts(short[] array, int offset, int count) throws KryoException {
        for (int n = offset + count; offset < n; offset++) {
            int value = array[offset];
            output.writeByte((byte)value);
            output.writeByte((byte)(value >>> 8));
            total += 2;
        }
    }

    @Override
    public void writeChars(char[] array, int offset, int count) throws KryoException {
        for (int n = offset + count; offset < n; offset++) {
            int value = array[offset];
            output.writeByte((byte)value);
            output.writeByte((byte)(value >>> 8));
            total += 2;
        }
    }

    @Override
    public void writeBooleans(boolean[] array, int offset, int count) throws KryoException {
        for (int n = offset + count; offset < n; offset++)
            output.writeByte(array[offset] ? (byte) 1 : 0);
        total += count;
    }
}
