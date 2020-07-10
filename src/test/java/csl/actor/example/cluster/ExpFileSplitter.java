package csl.actor.example.cluster;

import csl.actor.util.FileSplitter;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public class ExpFileSplitter {
    public static void main(String[] args) throws Exception {
        new ExpFileSplitter().run(args);
    }

    public void run(String[] args) throws IOException  {
        Instant start = Instant.now();
        FileSplitter sp = FileSplitter.getWithSplitCount(10);
        sp.split(args[0]).parallelStream()
                .forEach(s -> read(sp, s));

        System.out.println("Finish " + Duration.between(start, Instant.now()) + " " +
                String.format("%,d words, %,d lines, %,d splits", words.get(), lines.get(), splits.get()));
    }

    AtomicLong splits = new AtomicLong();
    AtomicLong lines = new AtomicLong();
    AtomicLong words = new AtomicLong();

    public void read(FileSplitter sp, FileSplitter.FileSplit s) {
        try {
            splits.incrementAndGet();
            sp.openLineIterator(s)
                    .forEachRemaining(l -> {
                        lines.incrementAndGet();
                        words.addAndGet(l.split("\\W+").length);
                    });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
