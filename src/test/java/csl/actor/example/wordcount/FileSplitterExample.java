package csl.actor.example.wordcount;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileSplitterExample {
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
        System.out.println(String.format("split: %s : splits=%,d, splitLen=%,d", Duration.between(startTime, splitTime), sps.size(), sps.get(0).splitLength));

        startTime = Instant.now();
        ExecutorService service = Executors.newScheduledThreadPool(sps.size());
        CountDownLatch latch = new CountDownLatch(sps.size());
        List<Long> positions = new ArrayList<>(sps.size());
        for (FileSplitter.FileSplit split : sps) {
            service.execute(() -> {
                try (RandomAccessFile f = s.open(split)) {
                    positions.add(f.getFilePointer());
                    latch.countDown();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
        }

        latch.await();
        positions.sort(Comparator.naturalOrder());
        Instant openTime = Instant.now();
        System.out.println(String.format("open: %s : positions=%s", Duration.between(startTime, openTime), positions));
        service.shutdownNow();
    }
}
