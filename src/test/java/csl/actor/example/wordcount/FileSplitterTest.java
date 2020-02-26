package csl.actor.example.wordcount;

import csl.actor.cluster.FileSplitter;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileSplitterTest {
    public static void main(String[] args) throws Exception {
        String src = args[0];
        long block = 10_000_000L;

        List<String> lines = Files.readAllLines(Paths.get(src));
        List<Text> texts = new ArrayList<>();
        for (int i = 0, l = lines.size(); i < l; ++i) {
            texts.add(new Text(i, lines.get(i)));
        }
        lines = null;
        log(String.format("lines=%,d", texts.size()));

        List<Text> data = new ArrayList<>();
        FileSplitter splitter = FileSplitter.getWithSplitLength(block);
        for (FileSplitter.FileSplit s : splitter.split(src)) {
            long n = 0;
            for (Iterator<String> iter = splitter.openLineIterator(s);
                 iter.hasNext(); ) {
                String line = iter.next();
                data.add(new Text(s.getSplitIndex(), n, line));
                ++n;
            }
            //log("" + n);
        }
        log(String.format("read lines=%,d", data.size()));
        for (int i = 0, l = texts.size(); i < l; ++i) {
            Text e = texts.get(i);
            Text a = data.get(i);
            if (!e.data.equals(a.data)) {
                log("invalid : " + e + "\n       vs " + a);
                throw new RuntimeException();
            }
        }
        if (data.size() != texts.size()) {
            throw new RuntimeException("error size");
        }
    }

    static void log(String str) {
        System.err.println(str);
    }

    public static class Text implements Comparable<Text> {
        long num;
        long num2;
        String data;

        public Text(long num, String data) {
            this.num = num;
            this.data = data;
        }

        public Text(long num, long num2, String data) {
            this.num = num;
            this.num2 = num2;
            this.data = data;
        }

        @Override
        public int compareTo(Text o) {
            int r;
            return (r = Long.compare(num, o.num)) == 0 ?
                    Long.compare(num2, o.num2) : r;
        }

        @Override
        public String toString() {
            return "Text{" +
                    "num=" + num +
                    ", num2=" + num2 +
                    ", data='" + data + '\'' +
                    '}';
        }
    }
}
