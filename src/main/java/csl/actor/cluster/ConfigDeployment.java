package csl.actor.cluster;

import csl.actor.ActorSystem;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
    public String appNameHeader = "app";

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
            return pathModifiers.computeIfAbsent(system, ConfigDeployment::createDefaultPathModifier);
        }
    }

    public static void setPathModifier(ActorSystem system, PathModifier m) {
        synchronized (pathModifiers) {
            pathModifiers.put(system, m);
        }
    }

    public PathModifierHost setPathModifierWithBaseDir(ActorSystem system) {
        return setPathModifierWithBaseDir(system, this.baseDir).setHost(host, port);
    }

    public static PathModifierHost setPathModifierWithBaseDir(ActorSystem system, String baseDir) {
        PathModifierHost h = new PathModifierHost(baseDir);
        setPathModifier(system, h);
        return h;
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

    public static PathModifierHost createDefaultPathModifier(ActorSystem system) {
        PathModifierHost pm = new PathModifierHost(".");
        ActorAddress.ActorAddressRemote addr;
        if (system instanceof ActorSystemRemote &&
                (addr = ((ActorSystemRemote) system).getServerAddress()) != null) {
            pm.setHost(addr.getHost(), addr.getPort());
        } else {
            pm.setHost("local");
        }
        pm.setApp(getAppName("app"));
        return pm;
    }

    public static String getAppName(String head) {
        Instant now = Instant.now();
        OffsetDateTime time = OffsetDateTime.ofInstant(now, ZoneOffset.UTC);
        int milli = (time.getHour() * 60 * 60 + time.getMinute() * 60 + time.getSecond()) * 1000
                + (time.getNano() / 1000_000);
        //nano max: 999,999,999 /1m ->         999
        //hour max: 23 * 60^2 *1k ->    82,800,000
        //min max : 59 * 60   *1k ->     3,540,000
        //sec max : 59        *1k ->        59,000
        //total max:                    86,399,999 -hex-> 5265bff -len-> 7
        String milliStr = String.format("%h", milli);
        while (milliStr.length() < 7) {
            milliStr = "0" + milliStr;
        }
        return String.format("%s-%s-%s", head,
                time.format(DateTimeFormatter.ofPattern("uu-MM-dd")), milliStr);
    }

    public static class PathModifierHost implements PathModifier {
        protected String baseDir;

        protected String host;
        protected String app;

        public PathModifierHost(String baseDir) {
            this.baseDir = baseDir;
        }

        public PathModifierHost setHost(String host) {
            this.host = host;
            return this;
        }

        public PathModifierHost setHost(String host, int port) {
            String hostId = ActorPlacement.toOutputFileComponent(false, 18, host) + "-" +
                    ActorPlacement.toOutputFileComponent(false, 8, Integer.toString(port));
            return setHost(hostId);
        }

        public PathModifierHost setApp(String app) {
            this.app = app;
            return this;
        }

        public String getBaseDir() {
            return baseDir;
        }

        public String getHost() {
            return host;
        }

        public String getApp() {
            return app;
        }

        @Override
        public Path get(String path) {
            return Paths.get(baseDir,
                    path.replaceAll(Pattern.quote("%h"), host)
                        .replaceAll(Pattern.quote("%a"), app));
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() +
                    "(baseDir=" + baseDir + ", host=" + host + ", app=" + app + ")";
        }
    }
}
