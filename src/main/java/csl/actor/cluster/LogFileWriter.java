package csl.actor.cluster;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

public class LogFileWriter extends PrintStream {
    protected Pattern colorEscape;
    protected PrintWriter out;
    protected int depth;

    public LogFileWriter(OutputStream originalErr, Path path, boolean preserveColor) throws IOException {
        super(originalErr);
        out = new PrintWriter(Files.newBufferedWriter(path), true);
        if (preserveColor) {
            colorEscape = null;
        } else {
            colorEscape = Pattern.compile("\033\\[(38;5;\\d+?|0)m");
        }
    }

    //overrides methods calling "write"
    @Override
    public void print(String s) {
        try {
            depth++;
            super.print(s);
        } finally {
            depth--;
        }
        if (colorEscape != null) {
            out.print(colorEscape.matcher(s).replaceAll(""));
        } else {
            out.print(s);
        }
    }

    @Override
    public synchronized void print(char c) {
        try {
            depth++;
            super.print(c);
        } finally {
            depth--;
        }
        out.print(c);
    }

    @Override
    public synchronized void print(boolean b) {
        try {
            depth++;
            super.print(b);
        } finally {
            depth--;
        }
        out.print(b);
    }

    @Override
    public synchronized void print(int i) {
        try {
            depth++;
            super.print(i);
        } finally {
            depth--;
        }
        out.print(i);
    }

    @Override
    public synchronized void print(long l) {
        try {
            depth++;
            super.print(l);
        } finally {
            depth--;
        }
        out.print(l);
    }

    @Override
    public synchronized void print(float f) {
        try {
            depth++;
            super.print(f);
        } finally {
            depth--;
        }
        out.print(f);
    }

    @Override
    public synchronized void print(double d) {
        try {
            depth++;
            super.print(d);
        } finally {
            depth--;
        }
        out.print(d);
    }

    @Override
    public synchronized void print(char[] s) {
        try {
            depth++;
            super.print(s);
        } finally {
            depth--;
        }
        out.print(s);
    }

    @Override
    public synchronized void print(Object obj) {
        try {
            depth++;
            super.print(obj);
        } finally {
            depth--;
        }
        out.print(obj);
    }

    /// overrides methods calling print(v) + newLine()

    @Override
    public synchronized void println() {
        try {
            depth++;
            super.println();
        } finally {
            depth--;
        }
        out.println();
    }

    @Override
    public synchronized void println(boolean x) {
        try {
            depth++;
            super.println(x);
        } finally {
            depth--;
        }
        out.println();
    }

    @Override
    public synchronized void println(char x) {
        try {
            depth++;
            super.println(x);
        } finally {
            depth--;
        }
        out.println();
    }

    @Override
    public synchronized void println(int x) {
        try {
            depth++;
            super.println(x);
        } finally {
            depth--;
        }
        out.println();
    }

    @Override
    public void println(long x) {
        try {
            depth++;
            super.println(x);
        } finally {
            depth--;
        }
        out.println();
    }

    @Override
    public synchronized void println(float x) {
        try {
            depth++;
            super.println(x);
        } finally {
            depth--;
        }
        out.println();
    }

    @Override
    public synchronized void println(double x) {
        try {
            depth++;
            super.println(x);
        } finally {
            depth--;
        }
        out.println();
    }

    @Override
    public synchronized void println(char[] x) {
        try {
            depth++;
            super.println(x);
        } finally {
            depth--;
        }
        out.println();
    }

    @Override
    public synchronized void println(String x) {
        try {
            depth++;
            super.println(x);
        } finally {
            depth--;
        }
        out.println();
    }

    @Override
    public synchronized void println(Object x) {
        try {
            depth++;
            super.println(x);
        } finally {
            depth--;
        }
        out.println();
    }

    ////

    @Override
    public void flush() {
        super.flush();
        out.flush();
    }

    @Override
    public void close() {
        super.close();
        out.close();
    }

    @Override
    public void write(int b) {
        super.write(b);
        if (depth == 0) {
            out.write(b);
        }
    }

    @Override
    public synchronized void write(byte[] buf, int off, int len) {
        super.write(buf, off, len);
        if (depth == 0) {
            for (int i = 0; i < len; ++i) {
                byte b = buf[off];
                out.write(b);
            }
        }
    }
}
