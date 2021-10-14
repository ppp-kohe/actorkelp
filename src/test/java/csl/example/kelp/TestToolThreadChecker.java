package csl.example.kelp;

import csl.example.TestTool;

public final class TestToolThreadChecker {
    private volatile Thread thread;
    private Object target;
    private StackTraceElement[] lastError;

    public TestToolThreadChecker(Object target) {
        this.target = target;
    }

    public boolean before() {
        Thread current = Thread.currentThread();
        Thread t = thread;
        if (t == current) {
            return false; //recursion
        } else if (t == null) {
            thread = current;
            return true;
        } else {
            thread = current;
            lastError = t.getStackTrace();
            return false;
        }
    }

    public boolean after(boolean i) {
        Thread current = Thread.currentThread();
        Thread t = thread;
        if (i) {
            if (t != current) {
                new TestTool().printError("!!! DebugThreadCheckTool " + objStr() + ": after error: " + current);
                print(current);
                print(t);
                i = false;
            }
            thread = null;
        } else {
            //error
            if (t == current) {
                System.err.println("!!! DebugThreadCheckTool " + objStr() + ": recursion: " + current);
                print(t);
            } else {
                new TestTool().printError("!!! DebugThreadCheckTool " + objStr() + ": before error: " + current + " vs " + t);
                print(current);
                print(t);
            }
        }
        return i;
    }
    private String objStr() {
        return target.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(target));
    }

    private void print(Thread t) {
        StringBuilder buf = new StringBuilder();
        if (t != null) {
            buf.append(t).append(":\n");
            for (StackTraceElement e : t.getStackTrace()) {
                buf.append("  ").append(e).append("\n");
            }
        } else {
            buf.append(" null\n");
        }
        buf.append("==========");
        System.err.println(buf);
    }

    public void error(boolean i, Exception e) {
        Thread current = Thread.currentThread();
        Thread t = thread;
        new TestTool().printError("!!! DebugThreadCheckTool " + objStr() + ": exception, flag=" + i + " : " + e);
        print(current);
        print(t);
    }

}
