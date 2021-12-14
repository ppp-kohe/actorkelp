package csl.actor.util;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Util;

import java.nio.ByteBuffer;

public class MmapOutput extends Output {
    MmapState state;

    public MmapOutput(MmapState state) {
        this.state = state;
    }

    @Override
    protected boolean require(int required) throws KryoException {
        return (state.current.buffer.remaining() >= required);
    }

    @Override
    public int position() {
        return state.current.buffer.position();
    }

    @Override
    public void setPosition(int position) {
        state.current.buffer.position(position);
    }

    public MmapState getState() {
        return state;
    }

    @Override
    public int getMaxCapacity() {
        return Util.maxArraySize;
    }

    @Override
    public void reset() {
        try {
            state.seek(0L);
        } catch (Exception ex) {
            throw new KryoException(ex);
        }
    }

    @Override
    public long total() {
        return state.current.position();
    }

    @Override
    public void flush() throws KryoException {}

    protected void nextBlock() {
        try {
            state.nextBlock();
        } catch (Exception ex) {
            throw new KryoException(ex);
        }
    }

    @Override
    public void close() throws KryoException {
        try {
            state.close();
        } catch (Exception ex) {
            throw new KryoException(ex);
        }
    }

    @Override
    public void write(int value) throws KryoException {
        writeByte((byte) value);
    }

    @Override
    public void writeByte(byte value) throws KryoException {
        if (!require(1)) {
            nextBlock();
        }
        state.current.buffer.put(value);
    }

    @Override
    public void writeByte(int value) throws KryoException {
        writeByte((byte) value);
    }

    @Override
    public void writeBytes(byte[] bytes, int offset, int count) throws KryoException {
        while (count > 0) {
            ByteBuffer byteBuffer = state.current.buffer;
            int n = Math.min(count, byteBuffer.remaining());
            byteBuffer.put(bytes, offset, n);
            count -= n;
            offset += n;
            if (count > 0 || !byteBuffer.hasRemaining()) {
                nextBlock();
            }
        }
    }

    protected void writeBytesBoundary(byte... bytes) throws KryoException {
        writeBytes(bytes, 0, bytes.length);
    }

    @Override
    public void writeInt(int value) throws KryoException {
        byte b1 = (byte) value;
        byte b2 = (byte) (value >> 8);
        byte b3 = (byte) (value >> 16);
        byte b4 = (byte) (value >> 24);
        if (require(4)) {
            ByteBuffer byteBuffer = state.current.buffer;
            byteBuffer.put(b1);
            byteBuffer.put(b2);
            byteBuffer.put(b3);
            byteBuffer.put(b4);
        } else {
            writeBytesBoundary(b1, b2, b3, b4);
        }
    }

    @Override
    public int writeVarInt(int value, boolean optimizePositive) throws KryoException {
        ByteBuffer byteBuffer = state.current.buffer;
        if (!optimizePositive) value = (value << 1) ^ (value >> 31);
        if (value >>> 7 == 0) {
            writeByte((byte) value);
            return 1;
        }
        if (value >>> 14 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7);
            if (require(2)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
            } else {
                writeBytesBoundary(b1, b2);
            }
            return 2;
        }
        if (value >>> 21 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7 | 0x80);
            byte b3 = (byte) (value >>> 14);
            if (require(3)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
                byteBuffer.put(b3);
            } else {
                writeBytesBoundary(b1, b2, b3);
            }
            return 3;
        }
        if (value >>> 28 == 0) {
            byte b1 = (byte) ((value & 0x7F) | 0x80);
            byte b2 = (byte) (value >>> 7 | 0x80);
            byte b3 = (byte) (value >>> 14 | 0x80);
            byte b4 = (byte) (value >>> 21);
            if (require(4)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
                byteBuffer.put(b3);
                byteBuffer.put(b4);
            } else {
                writeBytesBoundary(b1, b2, b3, b4);
            }
            return 4;
        }
        byte b1 = (byte) ((value & 0x7F) | 0x80);
        byte b2 = (byte) (value >>> 7 | 0x80);
        byte b3 = (byte) (value >>> 14 | 0x80);
        byte b4 = (byte) (value >>> 21 | 0x80);
        byte b5 = (byte) (value >>> 28);
        if (require(5)) {
            byteBuffer.put(b1);
            byteBuffer.put(b2);
            byteBuffer.put(b3);
            byteBuffer.put(b4);
            byteBuffer.put(b5);
        } else {
            writeBytesBoundary(b1, b2, b3, b4, b5);
        }
        return 5;
    }

    @Override
    public int writeVarIntFlag(boolean flag, int value, boolean optimizePositive) throws KryoException {
        ByteBuffer byteBuffer = state.current.buffer;
        if (!optimizePositive) value = (value << 1) ^ (value >> 31);
        int first = (value & 0x3F) | (flag ? 0x80 : 0);
        if (value >>> 6 == 0) {
            writeByte((byte) first);
            return 1;
        }
        if (value >>> 13 == 0) {
            byte b1 = (byte) (first | 0x40);
            byte b2 = (byte) (value >>> 6);
            if (require(2)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
            } else {
                writeBytesBoundary(b1, b2);
            }
            return 2;
        }
        if (value >>> 20 == 0) {
            byte b1 = (byte) (first | 0x40);
            byte b2 = (byte) ((value >>> 6) | 0x80);
            byte b3 = (byte) (value >>> 13);
            if (require(3)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
                byteBuffer.put(b3);
            } else {
                writeBytesBoundary(b1, b2, b3);
            }
            return 3;
        }
        if (value >>> 27 == 0) {
            byte b1 = (byte) (first | 0x40);
            byte b2 = (byte) ((value >>> 6) | 0x80);
            byte b3 = (byte) ((value >>> 13) | 0x80);
            byte b4 = (byte) (value >>> 20);
            if (require(4)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
                byteBuffer.put(b3);
                byteBuffer.put(b4);
            } else {
                writeBytesBoundary(b1, b2, b3, b4);
            }
            return 4;
        }
        byte b1 = (byte) (first | 0x40);
        byte b2 = (byte) ((value >>> 6) | 0x80);
        byte b3 = (byte) ((value >>> 13) | 0x80);
        byte b4 = (byte) ((value >>> 20) | 0x80);
        byte b5 = (byte) (value >>> 27);
        if (require(5)) {
            byteBuffer.put(b1);
            byteBuffer.put(b2);
            byteBuffer.put(b3);
            byteBuffer.put(b4);
            byteBuffer.put(b5);
        } else {
            writeBytesBoundary(b1, b2, b3, b4, b5);
        }
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
        if (require(8)) {
            ByteBuffer byteBuffer = state.current.buffer;
            byteBuffer.put(b1);
            byteBuffer.put(b2);
            byteBuffer.put(b3);
            byteBuffer.put(b4);
            byteBuffer.put(b5);
            byteBuffer.put(b6);
            byteBuffer.put(b7);
            byteBuffer.put(b8);
        } else {
            writeBytesBoundary(b1, b2, b3, b4, b5, b6, b7, b8);
        }
    }

    @Override
    public int writeVarLong(long value, boolean optimizePositive) throws KryoException {
        ByteBuffer byteBuffer = state.current.buffer;
        if (!optimizePositive) value = (value << 1) ^ (value >> 63);
        if (value >>> 7 == 0) {
            writeByte((byte) value);
            return 1;
        }
        if (value >>> 14 == 0) {
            byte b1 = (byte)((value & 0x7F) | 0x80);
            byte b2 = (byte)(value >>> 7);
            if (require(2)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
            } else {
                writeBytesBoundary(b1, b2);
            }
            return 2;
        }
        if (value >>> 21 == 0) {
            byte b1 = (byte)((value & 0x7F) | 0x80);
            byte b2 = (byte)(value >>> 7 | 0x80);
            byte b3 = (byte)(value >>> 14);
            if (require(3)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
                byteBuffer.put(b3);
            } else {
                writeBytesBoundary(b1, b2, b3);
            }
            return 3;
        }
        if (value >>> 28 == 0) {
            byte b1 = (byte)((value & 0x7F) | 0x80);
            byte b2 = (byte)(value >>> 7 | 0x80);
            byte b3 = (byte)(value >>> 14 | 0x80);
            byte b4 = (byte)(value >>> 21);
            if (require(4)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
                byteBuffer.put(b3);
                byteBuffer.put(b4);
            } else {
                writeBytesBoundary(b1, b2, b3, b4);
            }
            return 4;
        }
        if (value >>> 35 == 0) {
            byte b1 = (byte)((value & 0x7F) | 0x80);
            byte b2 = (byte)(value >>> 7 | 0x80);
            byte b3 = (byte)(value >>> 14 | 0x80);
            byte b4 = (byte)(value >>> 21 | 0x80);
            byte b5 = (byte)(value >>> 28);
            if (require(5)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
                byteBuffer.put(b3);
                byteBuffer.put(b4);
                byteBuffer.put(b5);
            } else {
                writeBytesBoundary(b1, b2, b3, b4, b5);
            }
            return 5;
        }
        if (value >>> 42 == 0) {
            byte b1 = (byte)((value & 0x7F) | 0x80);
            byte b2 = (byte)(value >>> 7 | 0x80);
            byte b3 = (byte)(value >>> 14 | 0x80);
            byte b4 = (byte)(value >>> 21 | 0x80);
            byte b5 = (byte)(value >>> 28 | 0x80);
            byte b6 = (byte)(value >>> 35);
            if (require(6)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
                byteBuffer.put(b3);
                byteBuffer.put(b4);
                byteBuffer.put(b5);
                byteBuffer.put(b6);
            } else {
                writeBytesBoundary(b1, b2, b3, b4, b5, b6);
            }
            return 6;
        }
        if (value >>> 49 == 0) {
            byte b1 = (byte)((value & 0x7F) | 0x80);
            byte b2 = (byte)(value >>> 7 | 0x80);
            byte b3 = (byte)(value >>> 14 | 0x80);
            byte b4 = (byte)(value >>> 21 | 0x80);
            byte b5 = (byte)(value >>> 28 | 0x80);
            byte b6 = (byte)(value >>> 35 | 0x80);
            byte b7 = (byte)(value >>> 42);
            if (require(7)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
                byteBuffer.put(b3);
                byteBuffer.put(b4);
                byteBuffer.put(b5);
                byteBuffer.put(b6);
                byteBuffer.put(b7);
            } else {
                writeBytesBoundary(b1, b2, b3, b4, b5, b6, b7);
            }
            return 7;
        }
        if (value >>> 56 == 0) {
            byte b1 = (byte)((value & 0x7F) | 0x80);
            byte b2 = (byte)(value >>> 7 | 0x80);
            byte b3 = (byte)(value >>> 14 | 0x80);
            byte b4 = (byte)(value >>> 21 | 0x80);
            byte b5 = (byte)(value >>> 28 | 0x80);
            byte b6 = (byte)(value >>> 35 | 0x80);
            byte b7 = (byte)(value >>> 42 | 0x80);
            byte b8 = (byte)(value >>> 49);
            if (require(8)) {
                byteBuffer.put(b1);
                byteBuffer.put(b2);
                byteBuffer.put(b3);
                byteBuffer.put(b4);
                byteBuffer.put(b5);
                byteBuffer.put(b6);
                byteBuffer.put(b7);
                byteBuffer.put(b8);
            } else {
                writeBytesBoundary(b1, b2, b3, b4, b5, b6, b7, b8);
            }
            return 8;
        }
        byte b1 = (byte)((value & 0x7F) | 0x80);
        byte b2 = (byte)(value >>> 7 | 0x80);
        byte b3 = (byte)(value >>> 14 | 0x80);
        byte b4 = (byte)(value >>> 21 | 0x80);
        byte b5 = (byte)(value >>> 28 | 0x80);
        byte b6 = (byte)(value >>> 35 | 0x80);
        byte b7 = (byte)(value >>> 42 | 0x80);
        byte b8 = (byte)(value >>> 49 | 0x80);
        byte b9 = (byte)(value >>> 56);
        if (require(9)) {
            byteBuffer.put(b1);
            byteBuffer.put(b2);
            byteBuffer.put(b3);
            byteBuffer.put(b4);
            byteBuffer.put(b5);
            byteBuffer.put(b6);
            byteBuffer.put(b7);
            byteBuffer.put(b8);
            byteBuffer.put(b9);
        } else {
            writeBytesBoundary(b1, b2, b3, b4, b5, b6, b7, b8, b9);
        }
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
        if (require(2)) {
            ByteBuffer byteBuffer = state.current.buffer;
            byteBuffer.put(b1);
            byteBuffer.put(b2);
        } else {
            writeBytesBoundary(b1, b2);
        }
    }

    @Override
    public void writeChar(char value) throws KryoException {
        byte b1 = (byte) value;
        byte b2 = (byte) (value >>> 8);
        if (require(2)) {
            ByteBuffer byteBuffer = state.current.buffer;
            byteBuffer.put(b1);
            byteBuffer.put(b2);
        } else {
            writeBytesBoundary(b1, b2);
        }
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

        ByteBuffer byteBuffer = state.current.buffer;
        outer:
        if (charCount > 1 && charCount <= 32) {
            for (int i = 0; i < charCount; i++)
                if (value.charAt(i) > 127) break outer;
            if (!require(charCount)) {
                writeAscii_slow(value, charCount);
                byteBuffer = state.current.buffer;
            } else {
                for (int i = 0, n = value.length(); i < n; ++i)
                    byteBuffer.put((byte) value.charAt(i));
            }
            int end = byteBuffer.position() - 1;
            byteBuffer.put(end, (byte) (byteBuffer.get(end) | 0x80));
            return;
        }
        writeVarIntFlag(true, charCount + 1, true);
        int charIndex = 0;
        byteBuffer = state.current.buffer; //the current may be changed
        if (require(charCount)) {
            while (true) {
                int c = value.charAt(charIndex);
                if (c > 127) break;
                byteBuffer.put((byte)c);
                charIndex++;
                if (charIndex == charCount) {
                    return;
                }
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

        ByteBuffer byteBuffer = state.current.buffer;
        if (!require(charCount)) {
            writeAscii_slow(value, charCount);
            byteBuffer = state.current.buffer;
        } else {
            for (int i = 0, n = value.length(); i < n; ++i)
                byteBuffer.put((byte) value.charAt(i));
        }
        int end = byteBuffer.position() - 1;
        byteBuffer.put(end, (byte) (byteBuffer.get(end) | 0x80));
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
        writeBytes(chars, 0, charCount);
    }

    @Override
    public void writeInts(int[] array, int offset, int count) throws KryoException {
        if (require(count << 2)) {
            ByteBuffer byteBuffer = state.current.buffer;
            for (int n = offset + count; offset < n; offset++) {
                int value = array[offset];
                byteBuffer.put((byte)value);
                byteBuffer.put((byte)(value >> 8));
                byteBuffer.put((byte)(value >> 16));
                byteBuffer.put((byte)(value >> 24));
            }
        } else {
            for (int n = offset + count; offset < n; offset++)
                writeInt(array[offset]);
        }
    }

    @Override
    public void writeLongs(long[] array, int offset, int count) throws KryoException {
        if (require(count << 3)) {
            ByteBuffer byteBuffer = state.current.buffer;
            for (int n = offset + count; offset < n; offset++) {
                long value = array[offset];
                byteBuffer.put((byte)value);
                byteBuffer.put((byte)(value >>> 8));
                byteBuffer.put((byte)(value >>> 16));
                byteBuffer.put((byte)(value >>> 24));
                byteBuffer.put((byte)(value >>> 32));
                byteBuffer.put((byte)(value >>> 40));
                byteBuffer.put((byte)(value >>> 48));
                byteBuffer.put((byte)(value >>> 56));
            }
        } else {
            for (int n = offset + count; offset < n; offset++)
                writeLong(array[offset]);
        }
    }

    @Override
    public void writeFloats(float[] array, int offset, int count) throws KryoException {
        if (require(count << 2)) {
            ByteBuffer byteBuffer = state.current.buffer;
            for (int n = offset + count; offset < n; offset++) {
                int value = Float.floatToIntBits(array[offset]);
                byteBuffer.put((byte)value);
                byteBuffer.put((byte)(value >> 8));
                byteBuffer.put((byte)(value >> 16));
                byteBuffer.put((byte)(value >> 24));
            }
        } else {
            for (int n = offset + count; offset < n; offset++)
                writeFloat(array[offset]);
        }
    }

    @Override
    public void writeDoubles(double[] array, int offset, int count) throws KryoException {
        if (require(count << 3)) {
            ByteBuffer byteBuffer = state.current.buffer;
            for (int n = offset + count; offset < n; offset++) {
                long value = Double.doubleToLongBits(array[offset]);
                byteBuffer.put((byte)value);
                byteBuffer.put((byte)(value >>> 8));
                byteBuffer.put((byte)(value >>> 16));
                byteBuffer.put((byte)(value >>> 24));
                byteBuffer.put((byte)(value >>> 32));
                byteBuffer.put((byte)(value >>> 40));
                byteBuffer.put((byte)(value >>> 48));
                byteBuffer.put((byte)(value >>> 56));
            }
        } else {
            for (int n = offset + count; offset < n; offset++)
                writeDouble(array[offset]);
        }
    }

    @Override
    public void writeShorts(short[] array, int offset, int count) throws KryoException {
        if (require(count << 1)) {
            ByteBuffer byteBuffer = state.current.buffer;
            for (int n = offset + count; offset < n; offset++) {
                int value = array[offset];
                byteBuffer.put((byte)value);
                byteBuffer.put((byte)(value >>> 8));
            }
        } else {
            for (int n = offset + count; offset < n; offset++)
                writeShort(array[offset]);
        }
    }

    @Override
    public void writeChars(char[] array, int offset, int count) throws KryoException {
        if (require(count << 1)) {
            ByteBuffer byteBuffer = state.current.buffer;
            for (int n = offset + count; offset < n; offset++) {
                int value = array[offset];
                byteBuffer.put((byte)value);
                byteBuffer.put((byte)(value >>> 8));
            }
        } else {
            for (int n = offset + count; offset < n; offset++)
                writeChar(array[offset]);
        }
    }

    @Override
    public void writeBooleans(boolean[] array, int offset, int count) throws KryoException {
        if (require(count)) {
            ByteBuffer byteBuffer = state.current.buffer;
            for (int n = offset + count; offset < n; offset++)
                byteBuffer.put(array[offset] ? (byte)1 : 0);
        } else {
            for (int n = offset + count; offset < n; offset++)
                writeBoolean(array[offset]);
        }
    }
}
