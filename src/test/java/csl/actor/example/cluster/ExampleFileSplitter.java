package csl.actor.example.cluster;

import csl.actor.cluster.FileSplitter;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExampleFileSplitter {
    public static void main(String[] args) throws Exception {
        String path = args[0];
        FileSplitter s;
        if (args.length >= 3 && args[1].equals("-l")) {
            long l = Long.parseLong(args[2].replaceAll("[_,]", ""));
            s = FileSplitter.getWithSplitLength(l);
        } else {
            long n = (args.length >= 2 ? Long.parseLong(args[1].replaceAll("[_,]", "")) : Runtime.getRuntime().availableProcessors());
            s = FileSplitter.getWithSplitCount(n);
        }

        Instant startTime = Instant.now();
        List<FileSplitter.FileSplit> sps = s.split(path);
        Instant splitTime = Instant.now();
        System.out.println(String.format("split: %s : splits=%,d, splitLen=%,d", Duration.between(startTime, splitTime), sps.size(), sps.get(0).getSplitLength()));

        startTime = Instant.now();
        ExecutorService service = Executors.newScheduledThreadPool(sps.size());
        CountDownLatch latch = new CountDownLatch(sps.size());
        ConcurrentLinkedQueue<Long> lns= new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> wds= new ConcurrentLinkedQueue<>();
        for (FileSplitter.FileSplit split : sps) {
            service.execute(() -> {
                try {
                    long ln = 0;
                    long wd = 0;
                    for (Iterator<String> iter = s.openLineIterator(split);
                         iter.hasNext(); ) {
                        String line = iter.next();
                        wd += Arrays.stream(line.split("￿￿￿￿\\W+")).count();
                        ++ln;
                    }
                    lns.offer(ln);
                    wds.offer(wd);
                    latch.countDown();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
        }

        latch.await();
        Instant openTime = Instant.now();
        System.out.println(String.format("open: %s : lns=%,d wds=%,d", Duration.between(startTime, openTime),
                lns.stream().mapToLong(Long::valueOf).sum(),
                wds.stream().mapToLong(Long::valueOf).sum()));
        service.shutdownNow();
    }

}
