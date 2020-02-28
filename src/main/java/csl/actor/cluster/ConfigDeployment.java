package csl.actor.cluster;

import csl.actor.ActorSystem;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.regex.Pattern;

public class ConfigDeployment extends ConfigBase {
    public String ssh = "ssh %s"; //host
    public String java = "java %s %s %s"; //option mainClass args
    public String host = "localhost";
    public int port = 38888;
    public String configType;
    public String baseDir = "target/debug";
    public boolean master = false;
    public boolean sharedDeploy = true;
    public long joinTimeoutMs = 10_000;
    public String pathSeparator = File.pathSeparator;

    public ConfigDeployment() {
    }

    public ConfigDeployment(Class<? extends ConfigBase> configType) {
        this.configType = configType.getName();
    }

    @Override
    public void readProperty(Field f, Object v) throws Exception {
        if (f.getName().equals("host") && v instanceof String && v.toString().contains(":")) {
            //host and port
            String[] hp = v.toString().split(":");
            host = hp[0];
            port = Integer.parseInt(hp[1]);
        } else {
            super.readProperty(f, v);
        }
    }

    public String getAddress() {
        return host + ":" + port;
    }

    public ConfigBase createAppConfig() {
        try {
            return (ConfigBase) Class.forName(configType)
                    .getConstructor().newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected static Map<ActorSystem, PathModifier> pathModifiers = new WeakHashMap<>();

    public static PathModifier getPathModifier(ActorSystem system) {
        synchronized (pathModifiers) {
            return pathModifiers.computeIfAbsent(system, s -> p -> Paths.get(p));
        }
    }

    public static void setPathModifier(ActorSystem system, PathModifier m) {
        synchronized (pathModifiers) {
            pathModifiers.put(system, m);
        }
    }

    public void setPathModifierWithBaseDir(ActorSystem system) {
        setPathModifierWithBaseDir(system, this.baseDir, host, port);
    }

    public static void setPathModifierWithBaseDir(ActorSystem system, String baseDir, String host, int port) {
        String hostId = ActorPlacement.toOutputFileComponent(false, 18, host) + "-" +
                ActorPlacement.toOutputFileComponent(false, 8, Integer.toString(port));
        setPathModifier(system, new PathModifierHost(baseDir, hostId));
    }

    public interface PathModifier {
        Path get(String path);
    }

    @Override
    public String logMessage(String msg) {
        return super.logMessage(getLogHeader() + msg);
    }

    public String getLogHeader() {
        return "[" + host + ":" + port + "] ";
    }

    public static class PathModifierHost implements PathModifier {
        protected String baseDir;
        protected String id;

        public PathModifierHost(String baseDir, String id) {
            this.baseDir = baseDir;
            this.id = id;
        }

        @Override
        public Path get(String path) {
            return Paths.get(baseDir,
                    path.replaceAll(Pattern.quote("%i"), id));
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "baseDir='" + baseDir + '\'' +
                    ", id='" + id + '\'' +
                    '}';
        }
    }
}
