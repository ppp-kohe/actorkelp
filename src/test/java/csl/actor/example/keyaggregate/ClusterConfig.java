package csl.actor.example.keyaggregate;

import csl.actor.keyaggregate.Config;
import csl.actor.keyaggregate.ConfigBase;

import java.io.File;
import java.lang.reflect.Field;

public class ClusterConfig extends ConfigBase {
    public String ssh = "ssh %s"; //host
    public String java = "java %s %s %s"; //option mainClass args
    public String host = "localhost";
    public int port = 38888;
    public String configType = Config.class.getName();
    public String baseDir = "target/debug";
    public boolean master = false;
    public boolean sharedDeploy = true;
    public long joinTimeoutMs = 10_000;
    public String pathSeparator = File.pathSeparator;

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
}
