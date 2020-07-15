package csl.actor.cluster;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Supplier;

public class PersistentFileManager {
    protected String path;
    protected long fileCount;
    protected ConfigDeployment.PathModifier pathModifier;
    protected ActorSystem.SystemLogger logger;

    protected KryoBuilder.SerializerFunction serializer;

    protected static final Map<ActorSystem, PersistentFileManager> systemPersistent = new WeakHashMap<>();

    public static boolean logPersist = System.getProperty("csl.actor.persist.log", "true").equals("true");
    public static boolean logDebugPersist = System.getProperty("csl.actor.persist.debug", "false").equals("true");
    public static int logColorPersist = ActorSystem.systemPropertyColor("csl.actor.persist.color", 94);

    public static PersistentFileManager getPersistentFile(ActorSystem system, Supplier<String> path) {
        synchronized (systemPersistent) {
            return systemPersistent.computeIfAbsent(system, s -> createPersistentFile(path.get(), s));
        }
    }

    public static PersistentFileManager createPersistentFile(String path, ActorSystem system) {
        if (system instanceof PersistentFileManagerFactory) {
            return ((PersistentFileManagerFactory) system).createFileManager(path);
        } else {
            KryoBuilder.SerializerFunction serializer;
            if (system instanceof ActorSystemRemote) {
                serializer = ((ActorSystemRemote) system).getSerializer();
            } else {
                serializer = new KryoBuilder.SerializerPoolDefault(system);
            }
            return new PersistentFileManager(path, serializer, ConfigDeployment.getPathModifier(system),
                    system == null ? new ActorSystemDefault.SystemLoggerErr() : system.getLogger());
        }
    }

    public interface PersistentFileManagerFactory {
        PersistentFileManager createFileManager(String path);
    }

    public PersistentFileManager(String path, KryoBuilder.SerializerFunction serializer,
                                 ConfigDeployment.PathModifier pathModifier, ActorSystem.SystemLogger logger) {
        this.path = path;
        this.serializer = serializer;
        this.pathModifier = pathModifier;
        this.logger = logger;
    }

    public ConfigDeployment.PathModifier getPathModifier() {
        return pathModifier;
    }

    public synchronized PersistentFileWriter createWriter(String head) {
        try {
            String path = createPath(head);
            logger.log(logPersist, logColorPersist, "createWriter: %s", path);
            return new PersistentFileWriter(path, this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized String createPath(String head) {
        long c = fileCount;
        ++fileCount;
        String p = Paths.get(path, String.format("%s-%05d", head, c)).toString();
        while (Files.exists(pathModifier.get(p))) {
            p = Paths.get(path, String.format("%s-%05d", head, c)).toString();
            ++fileCount;
            c = fileCount;
        }
        return pathModifier.expandPath(p);
    }

    public Path getPath(String pathExpanded) {
        Path p = pathModifier.get(pathExpanded);
        logger.log(logDebugPersist, logColorPersist, "getPath: %s -> %s", pathExpanded, p);
        Path dir = p.getParent();
        if (dir != null && !Files.exists(dir)) {
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
            Path p = manager.getPath(pathExpanded);
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
        protected transient PersistentFileManager manager;

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
            inputStream = new FileInputStream(manager.getPath(pathExpanded).toFile()); //Files.newInputStream(path).skip(n) is slow
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
}
