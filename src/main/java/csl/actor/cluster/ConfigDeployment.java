package csl.actor.cluster;

import csl.actor.ActorSystem;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Function;

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
        String baseDir = this.baseDir;
        setPathModifier(system, p -> Paths.get(baseDir, p));
    }

    public interface PathModifier {
        Path get(String path);
    }
}
