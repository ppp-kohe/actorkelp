package csl.example.util;

import com.esotericsoftware.kryo.io.Output;
import csl.actor.util.MmapOutput;
import csl.actor.util.MmapState;
import csl.example.TestTool;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class ExampleMmapOutput {
    public static void main(String[] args) throws Exception {
        new ExampleMmapOutput().run();
        new ExampleMmapOutput().runBoundary();
        new ExampleMmapOutput().runVar();
        new ExampleMmapOutput().runPerformance();
        new ExampleMmapOutput().runPerformance();
    }

    public void run() throws Exception {
        System.err.println("---------- run Mmap");
        Path path = Paths.get("target/debug-mmap-1");
        Files.deleteIfExists(path);
        try (MmapOutput out = new MmapOutput(new MmapState.MmapStateWrite(path,
                new MmapState.BlockBufferIndexer(100)))) {
            testOut(out);
        }
        System.err.println("---------- run Output");
        Path pathEx = Paths.get("target/debug-mmap-2");
        try (Output out = new Output(new FileOutputStream(pathEx.toFile()), 100)) {
            testOut(out);
        }
        checkFile(pathEx, path);
    }

    private void testOut(Output out) {
        //in capacity
        out.write((byte) 0xA5); //1
        out.writeShort((short) 0xA55A); //3
        out.writeChar((char) 0xA55A); //5
        out.writeInt(0xA55A_A55A); //9
        out.writeLong(0xA55A_A55A_A55A_A55AL); //17
        out.writeBoolean(true); //18
        out.writeFloat(Float.intBitsToFloat(0xA55A_A55A)); //22
        out.writeDouble(Double.longBitsToDouble(0xA55A_A55A_A55A_A55AL)); //30
        System.err.println("after double " + out.total());

        out.writeBytes(data(270)); //300
        out.writeChars(dataChars(10), 0, 10); //320
        out.writeShorts(dataShorts(10), 0, 10); //340
        System.err.println("after shorts " + out.total());
        out.writeInts(dataInts(10), 0, 10); //380
        out.writeBytes(data(20)); //400
        out.writeLongs(dataLongs(10), 0, 10); //480
        System.err.println("after longs " + out.total());
        out.writeBytes(data(20)); //500
        out.writeBooleans(dataBooleans(10), 0, 10); //510
        out.writeFloats(dataFloats(10), 0, 10); //550
        out.writeBytes(data(50)); //600
        out.writeDoubles(dataDoubles(10), 0, 10); //680
        System.err.println("after doubles " + out.total());
        out.writeBytes(data(20)); //700

        out.writeString(dataAscii(20)); //720
        System.err.println("after ascii str " + out.total());
        out.writeString(dataAscii(40)); //761 : 1varFlag
        System.err.println("after ascii str2 " + out.total());
        String utf = dataAscii(10) + ((char) 0x251) + dataStr(20);
        System.err.println("str len " + utf.getBytes(StandardCharsets.UTF_8).length); //72
        out.writeBytes(data(39)); //800
        out.writeString(utf);  //873 : 1varFlag
        System.err.println("after ascii utf8 " + out.total());
        out.writeBytes(data(27)); //900
        out.writeAscii(dataAscii(40)); //940
        out.writeString(null); //941
        out.writeString(""); //942
        out.writeAscii(null); //943
        out.writeAscii(""); //944

        TestTool.assertEquals("total in capacity", 944L, out.total());
    }

    public void runBoundary() throws Exception {
        System.err.println("---------- runBoundary Mmap");
        Path path = Paths.get("target/debug-mmap-1");
        Files.deleteIfExists(path);
        try (MmapOutput out = new MmapOutput(new MmapState.MmapStateWrite(path,
                new MmapState.BlockBufferIndexer(100)))) {
            testOutBoundary(out);
        }
        System.err.println("---------- runBoundary Output");
        Path pathEx = Paths.get("target/debug-mmap-2");
        try (Output out = new Output(new FileOutputStream(pathEx.toFile()), 100)) {
            testOutBoundary(out);
        }
        checkFile(pathEx, path);
    }
    private void testOutBoundary(Output out) {
        out.writeBytes(data(99));
        out.write((byte) 0xA5); //100
        out.write((byte) 0xA5); //101
        out.writeBytes(data(98)); //199
        out.writeShort((short) 0xA55A); //201
        out.writeBytes(data(98)); //299
        out.writeChar((char) 0xA55A); //301
        out.writeBytes(data(98)); //399
        out.writeInt(0xA55A_A55A); //403
        out.writeBytes(data(96)); //499
        out.writeLong(0xA55A_A55A_A55A_A55AL); //507
        out.writeBytes(data(92)); //599
        out.writeBoolean(true); //600
        out.writeBoolean(true); //601
        out.writeBytes(data(98)); //699
        out.writeFloat(Float.intBitsToFloat(0xA55A_A55A)); //703
        out.writeBytes(data(96)); //799
        out.writeDouble(Double.longBitsToDouble(0xA55A_A55A_A55A_A55AL)); //807
        System.err.println("after double " + out.total());

        out.writeBytes(data(92)); //899
        out.writeChars(dataChars(10), 0, 10); //919
        out.writeBytes(data(80)); //999
        out.writeShorts(dataShorts(10), 0, 10); //1019
        System.err.println("after shorts " + out.total());
        out.writeBytes(data(80)); //1099
        out.writeInts(dataInts(10), 0, 10); //1139
        out.writeBytes(data(60)); //1199
        out.writeLongs(dataLongs(10), 0, 10); //1279
        System.err.println("after longs " + out.total());
        out.writeBytes(data(20)); //1299
        out.writeBooleans(dataBooleans(10), 0, 10); //1309
        out.writeBytes(data(90)); //1399
        out.writeFloats(dataFloats(10), 0, 10); //1439
        out.writeBytes(data(60)); //1499
        out.writeDoubles(dataDoubles(10), 0, 10); //1579
        System.err.println("after doubles " + out.total());
        out.writeBytes(data(20)); //1599

        out.writeString(dataAscii(20)); //1619
        System.err.println("after ascii str " + out.total());
        out.writeBytes(data(70)); //1689 //for !require after 1varFlag
        out.writeString(dataAscii(40)); //1730 : 1varFlag
        System.err.println("after ascii str2 " + out.total());
        String utf = dataAscii(10) + ((char) 0x251) + dataStr(20);
        System.err.println("str len " + utf.getBytes(StandardCharsets.UTF_8).length); //72
        out.writeBytes(data(20)); //1750
        out.writeString(utf);  //1823 : 1varFlag
        System.err.println("after ascii utf8 " + out.total());
        out.writeBytes(data(57)); //1880
        out.writeAscii(dataAscii(40)); //1920

        TestTool.assertEquals("total in capacity", 1920L, out.total());
    }

    public void runVar() throws Exception {
        System.err.println("---------- runVar Mmap");
        Path path = Paths.get("target/debug-mmap-1");
        Files.deleteIfExists(path);
        try (MmapOutput out = new MmapOutput(new MmapState.MmapStateWrite(path,
                new MmapState.BlockBufferIndexer(100)))) {
            testOutVar(out);
        }
        System.err.println("---------- runVar Output");
        Path pathEx = Paths.get("target/debug-mmap-2");
        try (Output out = new Output(new FileOutputStream(pathEx.toFile()), 100)) {
            testOutVar(out);
        }
        checkFile(pathEx, path);
    }

    public void testOutVar(Output out) {
        out.writeVarInt(0x7A, true); //1
        System.err.println("+1 " + out.total());
        out.writeVarInt(0x2A5A, true); //3
        System.err.println("+2 " + out.total());
        out.writeVarInt(0xA_A55A, true); //6
        System.err.println("+3 " + out.total());
        out.writeVarInt(0x2A_A55A, true); //10
        System.err.println("+4 " + out.total());
        out.writeVarInt(0xFA5A_A55A, true); //15
        System.err.println("+5 " + out.total());

        out.writeVarLong(0x7AL, true); //16
        System.err.println("+1 " + out.total());
        out.writeVarLong(0x2A5AL, true); //18
        System.err.println("+2 " + out.total());
        out.writeVarLong(0xA_A55AL, true); //21
        System.err.println("+3 " + out.total());
        out.writeVarLong(0x2A_A55AL, true); //25
        System.err.println("+4 " + out.total());
        out.writeVarLong(0xA55A_A55AL, true); //30
        System.err.println("+5 " + out.total());

        out.writeVarLong(0xA_A55A_A55AL, true); //36
        System.err.println("+6 " + out.total());
        out.writeVarLong(0xA55A_A55A_A55AL, true); //43
        System.err.println("+7 " + out.total());
        out.writeVarLong(0xA5_A55A_A55A_A55AL, true); //51
        System.err.println("+8 " + out.total());
        out.writeVarLong(0xFAA5_A55A_A55A_A55AL, true); //60
        System.err.println("+9 " + out.total());

        out.writeVarIntFlag(true, 0x25, true); //61
        System.err.println("+1 " + out.total());
        out.writeVarIntFlag(true, 0xA55, true); //63
        System.err.println("+2 " + out.total());
        out.writeVarIntFlag(true, 0xA_A55A, true); //66
        System.err.println("+3 " + out.total());
        out.writeVarIntFlag(true, 0x2A_A55A, true); //70
        System.err.println("+4 " + out.total());
        out.writeVarIntFlag(true, 0xFA5A_A55A, true); //75
        System.err.println("+5 " + out.total());

        //////

        out.writeBytes(data(24)); //99
        out.writeVarInt(0x2A5A, true); //101
        System.err.println("+2 " + out.total());
        out.writeBytes(data(98)); //199
        out.writeVarInt(0xA_A55A, true); //202
        System.err.println("+3 " + out.total());
        out.writeBytes(data(97)); //299
        out.writeVarInt(0x2A_A55A, true); //303
        System.err.println("+4 " + out.total());
        out.writeBytes(data(96)); //399
        out.writeVarInt(0xFA5A_A55A, true); //404
        System.err.println("+5 " + out.total());

        out.writeBytes(data(95)); //499
        out.writeVarLong(0x2A5AL, true); //501
        System.err.println("+2 " + out.total());
        out.writeBytes(data(98)); //599
        out.writeVarLong(0xA_A55AL, true); //602
        System.err.println("+3 " + out.total());
        out.writeBytes(data(97)); //699
        out.writeVarLong(0x2A_A55AL, true); //703
        System.err.println("+4 " + out.total());
        out.writeBytes(data(96)); //799
        out.writeVarLong(0xA55A_A55AL, true); //804
        System.err.println("+5 " + out.total());

        out.writeBytes(data(95)); //899
        out.writeVarLong(0xA_A55A_A55AL, true); //905
        System.err.println("+6 " + out.total());
        out.writeBytes(data(94)); //999
        out.writeVarLong(0xA55A_A55A_A55AL, true); //1006
        System.err.println("+7 " + out.total());
        out.writeBytes(data(93)); //1099
        out.writeVarLong(0xA5_A55A_A55A_A55AL, true); //1107
        System.err.println("+8 " + out.total());
        out.writeBytes(data(92)); //1199
        out.writeVarLong(0xFAA5_A55A_A55A_A55AL, true); //1208
        System.err.println("+9 " + out.total());

        out.writeBytes(data(91)); //1299
        out.writeVarIntFlag(true, 0xA55, true); //1301
        System.err.println("+2 " + out.total());
        out.writeBytes(data(98)); //1399
        out.writeVarIntFlag(true, 0xA_A55A, true); //1402
        System.err.println("+3 " + out.total());
        out.writeBytes(data(97)); //1499
        out.writeVarIntFlag(true, 0x2A_A55A, true); //1503
        System.err.println("+4 " + out.total());
        out.writeBytes(data(96)); //1599
        out.writeVarIntFlag(true, 0xFA5A_A55A, true); //1604
        System.err.println("+5 " + out.total());

        TestTool.assertEquals("total", 1604L, out.total());
    }

    private void checkFile(Path p1, Path p2) {
        try {
            System.err.println("check files: " + p1 + " vs " + p2);
            long s1 = Files.size(p1);
            long s2 = Files.size(p2);
            TestTool.assertEquals("size", s1, s2);

            long sames = 0;
            long diffs = 0;
            try (InputStream in = new BufferedInputStream(new FileInputStream(p2.toFile()));
                 InputStream inEx = new BufferedInputStream(new FileInputStream(p1.toFile()))) {
                for (long i = 0; i < s1; ++i) {
                    int a = in.read();
                    int e = inEx.read();
                    if (a != e) {
                        TestTool.assertEquals(String.format("data %,d", i), e, a);
                        diffs++;
                        if (diffs > 100) {
                            System.err.println("stop too many errors");
                            break;
                        }
                    } else {
                        sames++;
                    }
                }
            }
            TestTool.assertTrue("same file " + sames, diffs == 0);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private byte[] data(int n) {
        byte[] bs = new byte[n];
        for (int i = 0; i < n; ++i) {
            bs[i] = (byte) (0xFF & i);
        }
        return bs;
    }
    private char[] dataChars(int n) {
        char[] bs = new char[n];
        for (int i = 0; i < n; ++i) {
            bs[i] = (char) (0xFF_FF & i);
        }
        return bs;
    }

    private short[] dataShorts(int n) {
        short[] bs = new short[n];
        for (int i = 0; i < n; ++i) {
            bs[i] = (short) (0xFF_FF & i);
        }
        return bs;
    }
    private int[] dataInts(int n) {
        int[] bs = new int[n];
        for (int i = 0; i < n; ++i) {
            bs[i] = i;
        }
        return bs;
    }
    private long[] dataLongs(int n) {
        long[] bs = new long[n];
        for (int i = 0; i < n; ++i) {
            bs[i] = (((long)i) << 32L) | i;
        }
        return bs;
    }
    private float[] dataFloats(int n) {
        float[] bs = new float[n];
        for (int i = 0; i < n; ++i) {
            bs[i] = i;
        }
        return bs;
    }
    private double[] dataDoubles(int n) {
        double[] bs = new double[n];
        for (int i = 0; i < n; ++i) {
            bs[i] = Double.longBitsToDouble((((long)i) << 32L) | i);
        }
        return bs;
    }

    private boolean[] dataBooleans(int n) {
        boolean[] bs = new boolean[n];
        for (int i = 0; i < n; ++i) {
            bs[i] = (i % 2 == 0);
        }
        return bs;
    }

    private String dataAscii(int n) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < n; ++i) {
            buf.append((char) (0x7F & i));
        }
        return buf.toString();
    }

    private String dataStr(int n) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < n; ++i) {
            int r = i % (0x3090 - 0x3041);
            char c = (char) (0x3041 + r);
            buf.append(c);
        }
        return buf.toString();
    }

    public void runPerformance() throws Exception {
        System.err.println("---------- runPerformance Mmap");
        Path path = Paths.get("target/debug-mmap-p1");
        Files.deleteIfExists(path);
        long n = 500_000_000;
        Instant start = Instant.now();
        Random rand = new Random(10101);
        try (MmapOutput out = new MmapOutput(new MmapState.MmapStateWrite(path))) {
            for (long i = 0; i < n; ++i) {
                out.writeInt(rand.nextInt());
            }
        }
        System.err.println(Duration.between(start, Instant.now()));
        System.err.println("---------- runPerformance Output");
        Path pathEx = Paths.get("target/debug-mmap-p2");
        Files.deleteIfExists(pathEx);
        start = Instant.now();
        rand = new Random(10101);
        try (Output out = new Output(new FileOutputStream(pathEx.toFile()), 8192)) {
            for (long i = 0; i < n; ++i) {
                out.writeInt(rand.nextInt());
            }
        }
        System.err.println(Duration.between(start, Instant.now()));
        checkFile(pathEx, path);
        Files.deleteIfExists(path);
        Files.deleteIfExists(pathEx);
    }
}
