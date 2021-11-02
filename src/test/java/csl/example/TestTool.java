package csl.example;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

public class TestTool {
    public static AtomicInteger fail = new AtomicInteger();
    protected boolean printOk;

    public TestTool() {
        this(true);
    }

    public TestTool(boolean printOk) {
        this.printOk = printOk;
    }

    public static <E> void assertEquals(String msg, E expected, E actual) {
        assertEquals(msg, expected, actual, Objects::deepEquals);
    }

    public static <E> void assertEquals(String msg, E expected, E actual, BiPredicate<E,E> p) {
        new TestTool().check(msg, expected, actual, p);
    }

    public static void assertTrue(String msg, boolean b) {
        new TestTool().check(msg, true, b, Boolean::equals);
    }

    public <E> void check(String msg, E expected, E actual, BiPredicate<E,E> p) {
        if (p.test(expected, actual)) {
            if (printOk) {
                String data = Objects.toString(actual);
                if (data.length() > 300) {
                    data = data.substring(0, 300) + "...";
                }
                printOk(msg + " : " + data);
            }
        } else {
            fail.incrementAndGet();
            printError(msg + " : " + expected + ", " + actual);
        }
    }

    public void printError(String s) {
        System.err.println(formatColor(196, "[Error] ") + s);
    }

    public void printOk(String s) {
        System.err.println(formatColor(76, "[OK] ") + s);
    }

    private String formatColor(int c, String s) {
        return String.format("\033[38;5;%dm%s\033[0m",c, s);
    }
}
