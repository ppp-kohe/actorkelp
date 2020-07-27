package csl.actor.example;

import java.util.Objects;
import java.util.function.BiPredicate;

public class TestTool {

    public static <E> void assertEquals(String msg, E expected, E actual) {
        assertEquals(msg, expected, actual, Objects::deepEquals);
    }

    public static <E> void assertEquals(String msg, E expected, E actual, BiPredicate<E,E> p) {
        if (p.test(expected, actual)) {
            new TestTool().printOk(msg + " : " + actual);
        } else {
            new TestTool().printError(msg + " : " + expected + ", " + actual);
        }
    }

    public static void assertTrue(String msg, boolean b) {
        if (b) {
            new TestTool().printOk(msg);
        } else {
            new TestTool().printError(msg);
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
