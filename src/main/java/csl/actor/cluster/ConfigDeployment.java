package csl.actor.cluster;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.ActorSystem;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;
import csl.actor.util.ConfigBase;
import csl.actor.util.PathModifier;

import java.io.File;
import java.lang.reflect.Field;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.Function;

public class ConfigDeployment extends ConfigBase {
    public static final long serialVersionUID = 1L;
    public String ssh = "ssh %s"; //host
    public String java = "java %s %s %s"; //option mainClass args
    public String javaVmOption = ""; //additional option for java
    public String host = "localhost";
    public int port = 38888;
    public String configType = ""; //empty means the default: "csl.actor.kelp.ConfigKelp";
    public String baseDir = "target/kelp";
    public boolean primary = false;
    public boolean sharedDeploy = true;
    public long joinTimeoutMs = 10_000;
    public String pathSeparator = File.pathSeparator;
    public String appNameHeader = "app";

    public boolean logFile = true;
    public String logFilePath = "%a/stderr-%h.txt";
    public boolean logFilePreserveColor = true;

    public String httpHost = ""; //use host
    public int httpPort = 48888;

    public boolean configPathAsPrimaryFirstArgument = true;
    public String kryoBuilderType = ""; //empty means the default: KryoBuilder.class.getName();

    public boolean throttle = false;


    public int systemThroughput = 256;
    public float systemThreadFactor = 5;
    public int systemServerLeaderThreads = 4;
    public float systemServerWorkerThreadsFactor = 20;
    public float systemClientThreadsFactor = 10;

    public ConfigDeployment() {
    }

    public ConfigDeployment(Class<? extends ConfigBase> configType) {
        this.configType = configType.getName();
    }

    @Override
    public void readProperty(Field f, Object v) {
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

    @SuppressWarnings("unchecked")
    public ConfigBase createAppConfig(Class<? extends ConfigBase> defaultConfType) {
        try {
            Class<? extends ConfigBase> type;
            if (configType.isEmpty()) {
                type = defaultConfType;
            } else {
                type = (Class<? extends ConfigBase>) Class.forName(configType);
            }
            return createConfig(type);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public PathModifier.PathModifierHost setPathModifierWithBaseDir(ActorSystem system) {
        return PathModifier.setPathModifierWithBaseDir(system, this.baseDir).setHost(host, port);
    }

    @Override
    protected FormatAndArgs logMessageHeader() {
        return super.logMessageHeader().append(logMessageHeaderHostPort()).append(new FormatAndArgs(" "));
    }

    public FormatAndArgs logMessageHeaderHostPort() {
        return new FormatAndArgs("[%s:%d]", host, port);
    }

    public Function<ActorSystem, Kryo> kryoBuilder(Class<? extends KryoBuilder> defaultKryoBuilderType) {
        return KryoBuilder.builder(ClusterDeployment.getBuilderType(this.kryoBuilderType, defaultKryoBuilderType));
    }
}
