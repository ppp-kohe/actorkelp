package csl.actor.kelp.persist;

import com.esotericsoftware.kryo.Serializer;
import csl.actor.ActorSystem;
import csl.actor.kelp.ActorKelpFunctions;
import csl.actor.kelp.behavior.HistogramTree;
import csl.actor.persist.PersistentFileManager;
import csl.actor.util.SampleTiming;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TreeMerger {
    protected PersistentFileManager manager;
    protected HistogramTree tree;

    protected TreeMap<Object, Queue<KeyHistogramsPersistable.KeyValueLoader>> currentKeyToLoaders;
    protected KeyHistogramsPersistable.FullTreeLoader remainingLoader;

    protected int bufferMax = 500_000;
    protected int loaderMax = 16;
    protected ActorKelpFunctions.KeyValuesReducer<Object, Object> reducer = new ActorKelpFunctions.KeyValuesReducerNone<>();

    protected PersistentFileManager.PersistentFileReaderSource lastSource;
    protected long lastLength;

    protected ActorSystem.SystemLogger logger;

    protected int mergedLoaders;
    protected Set<String> processedFiles = new HashSet<>();

    public static boolean logMerge = System.getProperty("csl.actor.persist.merge", "true").equals("true");

    protected AtomicInteger loadLoopCount = new AtomicInteger();
    protected AtomicLong totalKeys = new AtomicLong();
    protected AtomicLong totalValues = new AtomicLong();
    protected AtomicLong totalMergedValues = new AtomicLong();
    protected AtomicLong totalWriteBytes = new AtomicLong();
    protected AtomicInteger totalMergedLoaders = new AtomicInteger();

    protected SampleTiming logTimingLoop = new SampleTiming(4, 1024);
    protected SampleTiming logTiming = new SampleTiming(1 << 20, 1 << 30);

    public TreeMerger(PersistentFileManager manager) {
        this.manager = manager;
        logger = manager.getLogger();
    }

    public TreeMerger(HistogramTree tree) {
        this.tree = tree;
        this.manager = tree.getPersistent();
        if (tree instanceof HistogramTreePersistable) {
            logger = ((HistogramTreePersistable) tree).logger(manager);
        } else {
            logger = manager.getLogger();
        }
    }

    public TreeMerger withBufferMax(int bufferMax) {
        this.bufferMax = bufferMax;
        return this;
    }

    public TreeMerger withReducer(ActorKelpFunctions.KeyValuesReducer<Object, Object> reducer) {
        this.reducer = reducer;
        return this;
    }

    public TreeMerger withLoaderMax(int loaderMax) {
        this.loaderMax = loaderMax;
        return this;
    }

    protected String logHeader() {
        if (tree != null) {
            return String.format("Histogram(%h)", System.identityHashCode(tree));
        } else {
            return "TreeMerger";
        }
    }

    public long getLastLength() { //the returned source size
        return lastLength;
    }

    public PersistentFileManager.PersistentFileReaderSource mergeAllFromTree(HistogramTreePersistable.ReducerBeforePersist reducer) throws IOException {
        PersistentFileManager.PersistentFileReaderSource source = null;
        if (tree instanceof HistogramTreePersistable) {
            if (tree.getRoot() != null) { //saving memory
                source = ((HistogramTreePersistable) tree).persistTree(null);
            } else {
                source = ((HistogramTreePersistable) tree).getPreviousFullTreeSource();
            }
            if (source != null) {
                return mergeAll(source);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public PersistentFileManager.PersistentFileReaderSource mergeAll(PersistentFileManager.PersistentFileReaderSource source) throws IOException {
        return mergeAll(KeyHistogramsPersistable.treeLoaderLastFullTree(source));
    }

    public PersistentFileManager.PersistentFileReaderSource mergeAll(KeyHistogramsPersistable.FullTreeLoader loader) throws IOException {
        Instant start = Instant.now();
        logger.log(logMerge, KeyHistogramsPersistable.logPersistColor, "%s mergeAll from %s",
                logHeader(), loader.getSource());
        remainingLoader = loader;
        while (remainingLoader != null) {
            merge(remainingLoader);
        }
        logger.log(logMerge, KeyHistogramsPersistable.logPersistColor, "%s mergeAll finish %s %,d loaders",
                logHeader(), Duration.between(start, Instant.now()),
                mergedLoaders);
        return lastSource;
    }

    protected void init(KeyHistogramsPersistable.FullTreeLoader loader, boolean log) throws IOException {
        initTreeFromLoader(loader);
        currentKeyToLoaders = new TreeMap<>(comparator());
        if (lastSource != null) {
            putNextKeyToLoaderWithProcessedFile(new MergeLoader(lastSource));
        }
        while (loader != null && currentKeyToLoaders.size() < loaderMax) {
            putNextKeyToLoaderWithProcessedFile(loader);
            loader = loader.copyForPreviousOrNull();
        }
        remainingLoader = loader;
        loadLoopCount.incrementAndGet();
        totalMergedLoaders.set(currentKeyToLoaders.size());
        if (log) {
            logger.log(logMerge, KeyHistogramsPersistable.logPersistColor, "%s timing=%,d init %,d loaders%s",
                    logHeader(), logTimingLoop.getLast(), currentKeyToLoaders.size(),
                    (remainingLoader != null ? String.format(", remaining %s", remainingLoader.getSource()) : ", no remaining"));
        }
    }

    protected PersistentFileManager.PersistentWriter nextWriterForMerging() throws IOException {
        if (lastSource == null) {
            PersistentFileManager.PersistentFileWriter w = manager.createWriterForHead("mergetree");
            lastSource = w.createReaderSourceFromCurrentPosition();
            return w;
        } else { //reuse the file
            lastSource = lastSource.newSource(lastLength);
            return TreeWritings.createWriter(manager, lastSource.pathExpanded, lastSource.offset);
        }
    }

    public void merge(KeyHistogramsPersistable.FullTreeLoader topLoader) throws IOException {
        Instant start = Instant.now();
        boolean log = logTimingLoop.next();
        init(topLoader, log);
        TreeWritings.lockWriter();
        try (MergeWriter mergeWriter = new MergeWriter(nextWriterForMerging())) {
            mergeWriter.writeHeader(tree.getKeyType(), tree.getValueTypesForPositions());

            long prevCheckPos = mergeWriter.position();
            while (!currentKeyToLoaders.isEmpty()) {
                Map.Entry<Object, Queue<KeyHistogramsPersistable.KeyValueLoader>> next = currentKeyToLoaders.firstEntry();
                KeyHistogramsPersistable.KeyValueLoader loader;

                Object key = next.getKey();
                List<List<Object>> buffers = new ArrayList<>();

                totalKeys.incrementAndGet();
                while ((loader = next.getValue().poll()) != null) {
                    int listPos = loader.currentListPosition();
                    List<Object> buffer = bufferGet(buffers, listPos);
                    Object val = loader.nextValue();
                    while (val != null) {
                        totalValues.incrementAndGet();
                        if (logTiming.next()) {
                            totalMergedValues.addAndGet(mergeWriter.getWrittenValues());
                            totalWriteBytes.addAndGet(mergeWriter.position() - prevCheckPos);
                            prevCheckPos = mergeWriter.position();
                            logger.log(logMerge, KeyHistogramsPersistable.logPersistColor,
                                    "%s timing=%,d merging %s read=(key:%,d values:%,d) merged=(values:%,d size=%s)",
                                    logHeader(), logTiming.getLast(),
                                    Duration.between(start, Instant.now()), totalKeys.get(), totalValues.get(), totalMergedValues.get(),
                                    PersistentConditionActor.bytesString(totalWriteBytes.get()));
                        }
                        buffer.add(val);
                        bufferWrite(key, listPos, buffer, false, mergeWriter);
                        val = loader.nextValue();
                    }
                    putNextKeyToLoader(loader);
                }
                int listPos = 0;
                for (List<Object> buffer : buffers) {
                    bufferWrite(key, listPos, buffer, true, mergeWriter);
                    ++listPos;
                }

                currentKeyToLoaders.remove(key); //delete the entry
            }
            lastLength = mergeWriter.writeEnd();
            totalMergedValues.addAndGet(mergeWriter.getWrittenValues());
            totalWriteBytes.addAndGet(mergeWriter.position() - prevCheckPos);
        } finally {
            TreeWritings.unlockWriter();
            if (log) {
                logger.log(logMerge, KeyHistogramsPersistable.logPersistColor,
                        "%s timing=%,d merged %s read=(key:%,d values:%,d) merged=(values:%,d size=%s %s)",
                        logHeader(), logTimingLoop.getLast(),
                        Duration.between(start, Instant.now()), totalKeys.get(), totalValues.get(), totalMergedValues.get(),
                        PersistentConditionActor.bytesString(lastLength),
                        lastSource);
            }
        }
    }

    protected void bufferWrite(Object key, int listPos, List<Object> buffer, boolean force, MergeWriter mergeWriter) throws IOException {
        int prevSize = buffer.size();
        if (force || prevSize >= bufferMax) {
            if (prevSize >= reducer.requiredSize()) {
                Iterable<Object> vs = reducer.apply(key, buffer);
                if (vs != buffer) {
                    buffer.clear();
                    if (vs instanceof Collection<?>) {
                        buffer.addAll((Collection<?>) vs);
                    } else {
                        for (Object v : vs) {
                            buffer.add(v);
                        }
                    }
                }
            }
            if (force || buffer.size() >= prevSize || buffer.size() > bufferMax) { //force or cannot reduce or still large buffer
                mergeWriter.write(key, listPos, buffer);
                buffer.clear();
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected Comparator<Object> comparator() {
        return (Comparator<Object>) (tree == null ?
                new ActorKelpFunctions.KeyComparatorDefault<>() :
                tree.getComparator());
    }


    protected void initTreeFromLoader(KeyHistogramsPersistable.FullTreeLoader loader) throws IOException {
        if (loader != null && tree == null) {
            tree = loader.tree();
            if (tree.getPersistent() == null) {
                tree.init(manager);
            }
        }
    }

    protected void putNextKeyToLoader(KeyHistogramsPersistable.KeyValueLoader loader) throws IOException {
        Object k = loader.nextKey();
        if (k != null) {
            currentKeyToLoaders.computeIfAbsent(k, _k -> new LinkedBlockingQueue<>())
                    .offer(loader);
        }
    }

    protected void putNextKeyToLoaderWithProcessedFile(KeyHistogramsPersistable.KeyValueLoader loader) throws IOException {
        putNextKeyToLoader(loader);
        ++mergedLoaders;
        PersistentFileManager.PersistentFileReaderSource source = loader.getSource();
        if (source != null) {
            processedFiles.add(source.pathExpanded);
        }
    }

    public int getLoadLoopCount() {
        return loadLoopCount.get();
    }
    public long getTotalKeys() {
        return totalKeys.get();
    }
    public long getTotalValues() {
        return totalValues.get();
    }
    public long getTotalMergedValues() {
        return totalMergedValues.get();
    }
    public long getTotalWriteBytes() {
        return totalWriteBytes.get();
    }

    public int getTotalMergedLoaders() {
        return totalMergedLoaders.get();
    }

    /**
     * <pre>
     *     Class keyType,
     *     Map valueTypesForPositions,
     *     int -1,
     *     key1,
     *     listPos1, valueCount, values...., //same listPosN may occur multiple times in same key
     *     listPos1, valueCount, values....,
     *     ...
     *     int -1, //no listPos, next key
     *     key2,
     *     listPos1, valueCount, values....,
     *     ...
     *     int -2
     *     PersistentFileEnd
     * </pre>
     */
    public static class MergeWriter implements Closeable {
        PersistentFileManager.PersistentWriter writer;
        Object lastKey;
        int lastListPos;

        long writtenValues;

        Class<?> keyType;
        Serializer<?> keyTypeSerializer;
        Map<Object, Class<?>> valueTypesForPositions;

        public MergeWriter(PersistentFileManager.PersistentWriter writer) {
            this.writer = writer;
        }

        public void writeHeader(Class<?> keyType, Map<Object,Class<?>> valueTypesForPositions) throws IOException {
            writer.write(keyType);
            writer.write(valueTypesForPositions);

            this.keyType = HistogramTree.finalTypeOrNull(keyType);
            this.keyTypeSerializer = writer.serializer(keyType);
            this.valueTypesForPositions = valueTypesForPositions;
        }

        public void write(Object key, int listPos, List<Object> buffer) throws IOException {
            if (!Objects.equals(key, lastKey)) {
                writer.writeInt(-1);
                if (keyTypeSerializer == null) {
                    writer.write(key);
                } else {
                    writer.write(key, keyTypeSerializer);
                }
                this.lastKey = key;
            }
            this.lastListPos = listPos;
            writer.writeInt(listPos);
            writer.writeInt(buffer.size());

            Class<?> valType = HistogramTree.finalValueTypeOrNull(valueTypesForPositions, listPos);
            Serializer<?> valSer = writer.serializer(valType);
            if (valSer == null) {
                for (Object b : buffer) {
                    writer.write(b);
                    ++writtenValues;
                }
            } else {
                for (Object b : buffer) {
                    writer.write(b, valSer);
                    ++writtenValues;
                }
            }
        }

        public long getWrittenValues() {
            return writtenValues;
        }

        public long writeEnd() throws IOException {
            writer.writeInt(-2);
            writer.write(PersistentFileManager.FILE_END);
            return writer.position();
        }

        public long position() {
            return writer.position();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    private List<Object> bufferGet(List<List<Object>> values, int listPos) {
        while (listPos >= values.size()) {
            values.add(new ArrayList<>());
        }
        return values.get(listPos);
    }

    /**
     * loader saved by {@link MergeWriter}
     */
    public static class MergeLoader implements KeyHistogramsPersistable.KeyValueLoader {
        protected PersistentFileManager.PersistentFileReaderSource source;
        protected PersistentFileManager.PersistentFileReader reader;
        protected Object key;
        protected int listPosition;
        protected int remaining;

        protected Class<?> keyType;
        protected Map<Object, Class<?>> valueTypesForPositions;

        protected Serializer<?> keySerializer;
        protected Class<?> currentValueType;
        protected Serializer<?> currentValueSerializer;

        @SuppressWarnings("unchecked")
        public MergeLoader(PersistentFileManager.PersistentFileReaderSource source) {
            this.reader = source.createReader();

            keyType = HistogramTree.finalTypeOrNull((Class<?>) reader.next());
            valueTypesForPositions = (Map<Object, Class<?>>) reader.next();
            keySerializer = reader.serializer(keyType);

            listPosition = reader.nextInt(); //top -1
            if (listPosition == -2) { //finish
                reader.close();
                reader = null;
            }
        }

        @Override
        public PersistentFileManager.PersistentFileReaderSource getSource() {
            return source;
        }

        @Override
        public Object nextKey() {
            if (reader != null && remaining <= 0) {
                if (keySerializer == null) {
                    key = reader.next();
                } else {
                    key = reader.next(keyType, keySerializer);
                }
                loadList();
                if (listPosition == -1) { //never
                    return nextKey();
                }
            }
            return key;
        }

        @Override
        public int currentListPosition() {
            return listPosition;
        }

        private void loadList() {
            listPosition = reader.nextInt();
            if (listPosition == -1) { //nextKey
                remaining = 0;
            } else if (listPosition == -2) { //end
                remaining = 0;
                reader.close();
                reader = null;
                key = null;
            } else {
                remaining = reader.nextInt();
                currentValueType = HistogramTree.finalValueTypeOrNull(valueTypesForPositions, listPosition);
                currentValueSerializer = reader.serializer(currentValueType);
            }
        }

        @Override
        public Object nextValue() {
            if (remaining > 0 && reader != null) {
                --remaining;
                Object v;
                if (currentValueSerializer == null) {
                    v = reader.next();
                } else {
                    v = reader.next(currentValueType, currentValueSerializer);
                }
                if (remaining == 0) {
                    loadList();
                }
                return v;
            } else {
                return null;
            }
        }
    }
}
