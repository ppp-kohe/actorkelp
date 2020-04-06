package csl.actor.example.exp.wordcount;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WordCountStream {
    public static void main(String[] args) throws Exception {
        String src = args[0];
        String dst = args[1];

        File dstDir = new File(dst);
        dstDir.mkdirs();
        Instant start = Instant.now();
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(new File(dstDir,  "out-stream.txt").toPath()))){
            WordCountStream.out = out;
            try (Stream<String> ls = Files.lines(Paths.get(src))) {
                //.stream()
                    ls.flatMap(line -> Arrays.stream(line.split("\\W+")))
                        .map(w -> new Count(w, 1))
                        .collect(Collectors.toConcurrentMap(w -> w.word, w -> w, Count::add)).values()
                        .stream()
                        .sorted(Comparator.comparing(c -> c.word))
                        .forEach(WordCountStream::write);
            }
        }
        System.err.println("finish: " + Duration.between(start, Instant.now()));
    }
    static PrintWriter out;

    static void write(Count c) {
        out.println(c);
    }

    public static class Count implements Serializable {
        public static final long serialVersionUID = 1L;
        public String word;
        public long count;

        public Count(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public Count add(Count b) {
            count += b.count;
            return this;
        }

        @Override
        public String toString() {
            return "(" + word + "," + count + ")";
        }
    }
}
