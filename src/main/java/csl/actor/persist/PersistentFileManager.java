package csl.actor.persist;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;
import csl.actor.util.PathModifier;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * <ul>
 *     <li><i>path</i>: <code>pathModifier.baseDir / </code><i>pathExpanded</i>
 *     </li>
 *
 *     <li><i>pathExpanded</i>: <code>expand(subPath / </code><i>filePath</i><code>)</code>
 *     </li>
 *
 *     <li><i>filePath</i>: a sub-path, or head+number <i>head</i><code>-00001</code></li>
 * </ul>
 */
public class PersistentFileManager {
    protected String subPath;
    protected long fileCount;
    protected PathModifier pathModifier;
    protected ActorSystem.SystemLogger logger;

    protected KryoBuilder.SerializerFunction serializer;

    protected static final Map<ActorSystem, Map<String, PersistentFileManager>> systemPersistent = new WeakHashMap<>();

    public static boolean logPersist = System.getProperty("csl.actor.persist.log", "true").equals("true");
    public static boolean logDebugPersist = System.getProperty("csl.actor.persist.debug", "false").equals("true");
    public static int logColorPersist = ActorSystem.systemPropertyColor("csl.actor.persist.color", 94);

    public static PersistentFileManager getPersistentFile(ActorSystem system, String subPath) {
        synchronized (systemPersistent) {
            return systemPersistent.computeIfAbsent(system, s -> new HashMap<>())
                .computeIfAbsent(subPath, p -> createPersistentFile(p, system));
        }
    }

    public static PersistentFileManager createPersistentFile(String subPath, ActorSystem system) {
        if (system instanceof PersistentFileManagerFactory) {
            return ((PersistentFileManagerFactory) system).createFileManager(subPath);
        } else {
            KryoBuilder.SerializerFunction serializer;
            if (system instanceof ActorSystemRemote) {
                serializer = ((ActorSystemRemote) system).getSerializer();
            } else {
                serializer = new KryoBuilder.SerializerPoolDefault(system);
            }
            return new PersistentFileManager(subPath, serializer, PathModifier.getPathModifier(system),
                    system == null ? new ActorSystemDefault.SystemLoggerErr() : system.getLogger());
        }
    }

    public interface PersistentFileManagerFactory {
        PersistentFileManager createFileManager(String path);
    }

    public PersistentFileManager(String subPath, KryoBuilder.SerializerFunction serializer,
                                 PathModifier pathModifier, ActorSystem.SystemLogger logger) {
        this.subPath = subPath;
        this.serializer = serializer;
        this.pathModifier = pathModifier;
        this.logger = logger;
    }

    public PathModifier getPathModifier() {
        return pathModifier;
    }

    public PersistentFileWriter createWriterForHead(String head) {
        return createWriterForPathExpanded(createExpandedPathForHead(head));
    }

    public PersistentFileWriter createWriterForFilePath(String filePath) {
        return createWriterForPathExpanded(getPathExpandedForFilePath(filePath));
    }

    public PersistentFileWriter createWriterForPathExpanded(String pathExpanded) {
        try {
            logger.log(logPersist, logColorPersist, "createWriter: %s", pathExpanded);
            return new PersistentFileWriter(pathExpanded, this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param head the prefix of <i>filePath</i>
     * @return <i>pathExpanded</i>: <code>expand(subPath / </code><i>head</i>-00001<code>)</code>
     */
    public synchronized String createExpandedPathForHead(String head) {
        long c = fileCount;
        ++fileCount;
        String p = Paths.get(subPath, String.format("%s-%05d", head, c)).toString();
        while (Files.exists(pathModifier.getExpanded(p))) {
            p = Paths.get(subPath, String.format("%s-%05d", head, c)).toString();
            ++fileCount;
            c = fileCount;
        }
        return pathModifier.expandPath(p);
    }

    public Path getPathForExpandedPath(String pathExpanded) {
        return getPathForExpandedPath(pathExpanded, false);
    }

    public Path getPathForExpandedPath(String pathExpanded, boolean createParent) {
        Path p = pathModifier.get(pathExpanded);
        logger.log(logDebugPersist, logColorPersist, "getPath: %s -> %s", pathExpanded, p);
        Path dir = p.getParent();
        if (createParent && dir != null && !Files.exists(dir)) {
            try {
                Files.createDirectories(dir);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return p;
    }

    public KryoBuilder.SerializerFunction getSerializer() {
        return serializer;
    }

    public ActorSystem.SystemLogger getLogger() {
        return logger;
    }

    public void openForWrite(Path path) { }

    public void close(Path path) { }

    public void delete(String pathExpanded) {
        try {
            Path p = pathModifier.get(pathExpanded);
            if (Files.exists(p)) {
                Files.delete(p);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public String getPathExpandedForFilePath(String filePath) {
        return pathModifier.expandPath(Paths.get(subPath, filePath).toString());
    }

    public Path getPathForFilePath(String filePath) {
        return getPathForExpandedPath(getPathExpandedForFilePath(filePath));
    }

    public Path getPathForFilePath(String filePath, boolean createParent) {
        return getPathForExpandedPath(getPathExpandedForFilePath(filePath), createParent);
    }

    public PersistentFileReader createReaderForFilePath(String filePath) {
        return createReaderForPathExpanded(getPathExpandedForFilePath(filePath));
    }

    public PersistentFileReader createReaderForFilePath(String filePath, long offset) {
        return createReaderForPathExpanded(getPathExpandedForFilePath(filePath), offset);
    }

    public PersistentFileReader createReaderForPathExpanded(String pathExpanded) {
        return createReaderForPathExpanded(pathExpanded, 0);
    }

    public PersistentFileReader createReaderForPathExpanded(String pathExpanded, long offset) {
        try {
            return new PersistentFileReader(pathExpanded, offset, this);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public PersistentFileReaderSource createReaderSourceForFilePath(String filePath) {
        return createReaderSourceForPathExpanded(getPathExpandedForFilePath(filePath));
    }

    public PersistentFileReaderSource createReaderSourceForFilePath(String filePath, long offset) {
        return createReaderSourceForPathExpanded(getPathExpandedForFilePath(filePath), offset);
    }

    public PersistentFileReaderSource createReaderSourceForPathExpanded(String pathExpanded) {
        return createReaderSourceForPathExpanded(pathExpanded, 0);
    }

    public PersistentFileReaderSource createReaderSourceForPathExpanded(String pathExpanded, long offset) {
        return new PersistentFileReaderSource(pathExpanded, offset, this);
    }

    public static class PersistentFileEnd implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    public static class PersistentFileWriter implements AutoCloseable {
        protected String pathExpanded;
        protected Path filePath;
        protected PersistentFileManager manager;
        protected Output output;
        protected KryoBuilder.SerializerFunction serializer;

        public PersistentFileWriter(String pathExpanded, PersistentFileManager manager) throws IOException  {
            this.pathExpanded = pathExpanded;
            this.manager = manager;
            Path p = manager.getPathForExpandedPath(pathExpanded, true);
            manager.openForWrite(p);
            this.filePath = p;
            output = new Output(Files.newOutputStream(p));
            serializer = manager.getSerializer();
        }

        public long position() {
            return output.total();
        }

        public void write(Object obj) {
            try {
                serializer.write(output, obj);
            } catch (Exception ex) {
                throw new RuntimeException(String.format("write: path=%s obj=%s",
                        pathExpanded, manager.getLogger().toStringLimit(obj)), ex);
            }
        }

        public PersistentFileReaderSource createReaderSourceFromCurrentPosition() {
            return new PersistentFileReaderSource(pathExpanded, position(), manager);
        }

        public void flush() {
            output.flush();
        }

        @Override
        public void close() {
            write(new PersistentFileEnd());
            output.close();
            manager.close(filePath);
        }

        @Override
        public String toString() {
            return String.format("writer(path=%s, pos=%,d)", pathExpanded, position());
        }
    }

    public static class PersistentFileReaderSource implements Serializable {
        public static final long serialVersionUID = 1L;
        protected String pathExpanded;
        protected long offset;
        protected PersistentFileManager manager;

        public PersistentFileReaderSource() {}

        public PersistentFileReaderSource(String pathExpanded, long offset, PersistentFileManager manager) {
            this.pathExpanded = pathExpanded;
            this.offset = offset;
            this.manager = manager;
        }

        public void setManager(PersistentFileManager manager) {
            this.manager = manager;
        }

        public PersistentFileManager getManager() {
            return manager;
        }

        public PersistentFileReader createReader() {
            try {
                manager.getLogger().log(logDebugPersist, logColorPersist, "open: %s", this);
                return new PersistentFileReader(pathExpanded, offset, manager);
            } catch (IOException ex) {
                throw new RuntimeException("createReader: " + toString(), ex);
            }
        }

        public PersistentFileReaderSource newSource(long offset) {
            return new PersistentFileReaderSource(pathExpanded, offset, manager);
        }

        @Override
        public String toString() {
            return String.format("source(path=%s,offset=%,d)", pathExpanded, offset);
        }

        public void delete() {
            manager.delete(pathExpanded);
        }
    }

    public static class PersistentFileReader implements AutoCloseable {
        protected String pathExpanded;
        protected PersistentFileManager manager;
        protected KryoBuilder.SerializerFunction serializer;
        protected Input input;
        protected long offset;
        protected long position;
        protected InputStream inputStream;
        protected int bufferSize = 4096;

        public PersistentFileReader(String pathExpanded, long offset, PersistentFileManager manager) throws IOException {
            this.pathExpanded = pathExpanded;
            this.manager = manager;
            this.serializer = manager.getSerializer();
            inputStream = new FileInputStream(manager.getPathForExpandedPath(pathExpanded, false).toFile()); //Files.newInputStream(path).skip(n) is slow
            this.offset = offset;
            inputStream.skip(offset);
            this.position = offset;
            input = new Input(inputStream, bufferSize);
        }

        public Object next()  {
            long prev = input.total();
            Object v = serializer.read(input);
            position += input.total() - prev;
            return v;
        }

        public long nextLong() {
            long prev = input.total();
            long v = input.readLong();
            position += input.total() - prev;
            return v;
        }

        public long position() {
            return position;
        }

        public void position(long newPosition) {
            input.skip(newPosition - position());
            position = newPosition;
        }

        @Override
        public void close() {
            input.close();
        }

        @Override
        public String toString() {
            return String.format("reader(path=%s,offset=%,d,pos=%,d)", pathExpanded, offset, position());
        }

        public PersistentFileManager getManager() {
            return manager;
        }

        public void delete() {
            manager.delete(pathExpanded);
        }
    }

    public String getPathInternal() {
        return subPath;
    }

    public static class PersistentFileManagerSerializer extends Serializer<PersistentFileManager> {
        protected ActorSystem system;

        public PersistentFileManagerSerializer(ActorSystem system) {
            this.system = system;
        }

        @Override
        public void write(Kryo kryo, Output output, PersistentFileManager persistentFileManager) {
            String path = persistentFileManager.getPathInternal();
            output.writeString(path);
        }

        @Override
        public PersistentFileManager read(Kryo kryo, Input input, Class<? extends PersistentFileManager> aClass) {
            String path = input.readString();
            return PersistentFileManager.getPersistentFile(system, path);
        }
    }
}
