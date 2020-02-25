package csl.actor.example.keyaggregate;

import csl.actor.keyaggregate.ActorPlacement;
import csl.actor.keyaggregate.ActorPlacementKeyAggregation;
import csl.actor.keyaggregate.Config;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClusterDeployment {
    public static class NodeMain {
        public static void main(String[] args) {
            int n = Integer.parseInt(System.getProperty("csl.actor.logColor", "0"));
            String selfAddr = args[0];
            String joinAddr = args[1];
            ActorSystemRemote system = new ActorSystemRemote();
            ActorPlacementKeyAggregation p = new ActorPlacementKeyAggregation(system,
                    new ActorPlacement.PlacementStrategyUndertaker());

            system.startWithoutWait(ActorAddress.get(selfAddr));

            Config.CONFIG_DEFAULT.log(n, String.format("%s: joining to %s", system, joinAddr));
            p.join(ActorAddress.ActorAddressRemote.get(joinAddr));
        }
    }

    protected ActorSystemRemote system;
    protected ActorPlacementKeyAggregation masterPlace;
    protected String appName;
    protected ClusterUnit master;
    protected List<ClusterUnit> nodes = new ArrayList<>();
    protected Map<ClusterUnit, Process> processes = new ConcurrentHashMap<>();

    public ActorSystemRemote getSystem() {
        return system;
    }

    public ActorPlacementKeyAggregation getMasterPlace() {
        return masterPlace;
    }

    public ClusterUnit getMaster() {
        return master;
    }

    public Config getMasterAppConfig() {
        return master.getAppConfig();
    }

    public ClusterConfig getMasterClusterConfig() {
        return master.getClusterConfig();
    }

    public ActorPlacementKeyAggregation deploy(String confFile) {
        try {
            return deploy(new ClusterCommands().loadConfigFile(confFile));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ActorPlacementKeyAggregation deploy(ClusterUnit... units) {
        try {
            return deploy(Arrays.asList(units));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ActorPlacementKeyAggregation deploy(List<ClusterUnit> units) throws Exception {
        master = units.stream()
                .filter(u -> u.getClusterConfig().master)
                .findFirst()
                .orElseThrow(RuntimeException::new);
        if (master == null) {
            master = master();
        }

        system = new ActorSystemRemote();
        masterPlace = new ActorPlacementKeyAggregation(system,
                new ActorPlacement.PlacementStrategyRoundRobin(0));

        Map<ActorAddress, Config> configMap = masterPlace.getRemoteConfig();
        units.forEach(u ->
                configMap.put(ActorAddress.get(u.getClusterConfig().getAddress()), u.getAppConfig()));

        system.startWithoutWait(master.getClusterConfig().port);

        this.appName = getAppName();
        deployFiles(master);

        units.stream()
                .filter(u -> !u.getClusterConfig().master)
                .forEach(u -> {
                    nodes.add(u);
                    system.execute(() -> deployNode(u));
                });

        Set<ActorAddress> nodeAddrs = new HashSet<>(configMap.keySet());
        nodeAddrs.remove(ActorAddress.get(master.getClusterConfig().getAddress()));
        int limit = Math.max(3, nodeAddrs.size() / masterPlace.getTotalThreads());
        while (!nodeAddrs.isEmpty() && limit > 0) {
            try {
                ActorAddress joinedAddr = masterPlace.getCompleted()
                        .poll(master.getClusterConfig().joinTimeoutMs, TimeUnit.MILLISECONDS);
                if (joinedAddr != null) {
                    ActorAddress joinedHost = joinedAddr.getHostAddress();
                    nodeAddrs.remove(joinedHost);
                    master.log(String.format("%s: joined: %s  remaining=%,d", joinedHost, appName, nodeAddrs.size()));
                } else {
                    master.log(String.format("%s: waiting join: limit=%,d remaining=%,d", appName, limit, nodeAddrs.size()));
                    if (nodeAddrs.size() <= 5) {
                        master.log("  remaining nodes: " + nodeAddrs);
                    }
                    --limit;
                }
            } catch (Exception ex) {
                //
                master.log(String.format("%s %s", appName, ex));
                break;
            }
        }
        master.log(String.format("%s launched %,d nodes", appName, masterPlace.getClusterSize()));
        return masterPlace;
    }


    public void deployNode(ClusterUnit unit) {
        try {
            unit.log(String.format("%s: deploy: %s", appName, unit.getClusterConfig().getAddress()));
            deployFiles(unit);

            String cps = "";
            if (unit.getClassPathList() != null) {
                cps = String.format("-cp '%s'", String.join(unit.getClusterConfig().pathSeparator, unit.getClassPathList()));
            }

            String javaCmd = String.format(unit.getClusterConfig().java,
                    "-Dcsl.actor.logColor=" + unit.appConfig.get("logColor") + " " + cps,
                    "'" + NodeMain.class.getName() + "'", //escape
                    unit.getClusterConfig().getAddress() + " " +
                    master.getClusterConfig().getAddress());

            String dir = unit.getClusterConfig().baseDir;
            if (!dir.isEmpty()) {
                javaCmd = String.format("cd '%s'; %s", dir, javaCmd);
            }

            processes.put(unit,
                    launch(unit,
                            sshCommand(unit, javaCmd)));

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    public List<String> sshCommand(ClusterUnit unit, String appCmd) {
        String ssh = String.format(unit.getClusterConfig().ssh,
                unit.getClusterConfig().host);
        ClusterCommands.CommandLineParser parser = new ClusterCommands.CommandLineParser(ssh.trim());
        List<String> sshCmd = new ArrayList<>();
        while (parser.hasNext()) {
            if (parser.peek() == '"') {
                sshCmd.add(parser.eatStringLiteral());
            } else {
                StringBuilder buf = new StringBuilder();
                while (parser.hasNext() && !Character.isWhitespace(parser.peek())) {
                    buf.append(parser.eat());
                }
                sshCmd.add(buf.toString());
            }
            parser.eatWhitespaces();
        }
        sshCmd.add(appCmd);
        return sshCmd;
    }

    public Process launch(ClusterUnit unit, List<String> cmd) {
        try {
            unit.log(String.format("%s: command: %s : %s", appName, unit.getClusterConfig().getAddress(), cmd));
            ProcessBuilder builder = new ProcessBuilder().command(cmd);
            builder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .redirectError(ProcessBuilder.Redirect.INHERIT);
            return builder.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void deployFiles(ClusterUnit unit) throws Exception {
        String header = unit.getClusterConfig().getAddress();
        ClusterConfig conf = unit.getClusterConfig();

        if (conf.master && conf.sharedDeploy) { //suppose the code is running under the master
            //the baseDir is shared between all nodes
            Path appDir = Paths.get(conf.baseDir, appName);
            if (Files.exists(appDir)) {
                throw new RuntimeException("failed creation: " + appDir);
            }

            Path jarDir = appDir.resolve("jars");
            Files.createDirectories(jarDir);

            String path = System.getProperty("java.class.path", "");
            String[] items = path.split(Pattern.quote(File.pathSeparator));
            int count = 0;
            List<String> pathList = new ArrayList<>();
            for (String pathItem : items) {
                Path filePath = Paths.get(pathItem);
                if (Files.isDirectory(filePath)) { //creates jar and copies it
                    String fileName = String.format("%s-%03d.jar", filePath.getFileName(), count);
                    Path jarPath = jarDir.resolve(fileName);
                    pathList.add(jarPath.toString());

                    unit.log(String.format("%s deployFiles: %s\n      %s\n  ->  %s", appName, header, filePath, jarPath));
                    try (Stream<Path> ps = Files.walk(filePath);
                        JarOutputStream out = new JarOutputStream(
                                new BufferedOutputStream(new FileOutputStream(jarPath.toFile())))) {
                        ps.forEachOrdered(p -> writeToJar(unit, filePath, p, out));
                    }
                } else { //copy jar
                    String fileName = filePath.getFileName().toString();
                    Path jarPath = jarDir.resolve(fileName);
                    unit.log(String.format("%s deployFiles: %s\n      %s\n  ->  %s", appName, header, filePath, jarPath));
                    if (pathList.contains(jarPath.toString())) {
                        int i = fileName.lastIndexOf(".");
                        if (i > 0) {
                            String pre = fileName.substring(0, i);
                            String pos = fileName.substring(i);
                            jarPath = jarDir.resolve(String.format("%s-%03d%s", pre, count, pos));
                        } else {
                            jarPath = jarDir.resolve(String.format("%s-%03d", fileName, count));
                        }
                    }
                    Files.copy(filePath, jarPath);
                    pathList.add(jarPath.toString());
                }
                ++count;
            }

            unit.setClassPathList(pathList);

        } else if (!conf.master) {
            if (conf.sharedDeploy) {
                String masterBase = "^" + Pattern.quote(Paths.get(master.getClusterConfig().baseDir).toString());
                String unitBase = conf.baseDir;
                unit.setClassPathList(master.getClassPathList().stream()
                        .map(p -> p.replaceFirst(masterBase, unitBase))
                        .collect(Collectors.toList()));
            } else {
                String sshCommand = String.format(conf.ssh, conf.host, conf.port);
                //TODO ssh deploy
            }
        }
    }

    public String getAppName() {
        if (appName == null) {
            appName = getNextAppName();
        }
        return appName;
    }

    public String getNextAppName() {
        Instant now = Instant.now();
        OffsetDateTime time = OffsetDateTime.ofInstant(now, ZoneOffset.UTC);
        int milli = (time.getHour() * 60 * 60 + time.getMinute() * 60 + time.getSecond()) * 1000
                + (time.getNano() / 1000_000);
        return String.format("app-%s-%h",
                time.format(DateTimeFormatter.ofPattern("uu-MM-dd")), milli);
    }

    public void writeToJar(ClusterUnit unit, Path startDir, Path path, JarOutputStream out) {
        try {
            String p = startDir.relativize(path).toString()
                    .replaceAll(Pattern.quote(File.separator), "/");
            boolean dir = Files.isDirectory(path);
            if (dir && !p.endsWith("/")) {
                p += "/";
            }
            JarEntry e = new JarEntry(p);
            try {
                FileTime time = Files.getLastModifiedTime(path);
                e.setLastModifiedTime(time);
            } catch (Exception ex) {
                //
            }
            out.putNextEntry(e);
            try {
                if (!dir) {
                    Files.copy(path, out);
                }
            } finally {
                out.closeEntry();
            }
        } catch (Exception ex) {
            unit.log("error writeToJar: " + path + " : " + ex);
        }
    }

    public static ClusterUnit master() {
        ClusterUnit unit = new ClusterUnit();
        ClusterConfig conf = new ClusterConfig();
        conf.master = true;
        unit.setClusterConfig(conf);
        unit.setAppConfig(Config.CONFIG_DEFAULT);
        unit.setName("localhost");
        return unit;
    }

    public static ClusterUnit node(String host, int port) {
        ClusterUnit unit = new ClusterUnit();
        ClusterConfig conf = new ClusterConfig();
        conf.host = host;
        conf.port = port;
        unit.setClusterConfig(conf);
        unit.setAppConfig(Config.CONFIG_DEFAULT);
        unit.setName(host + ":" + port);
        return unit;
    }

    public static class ClusterUnit {
        protected String name;
        protected ClusterConfig clusterConfig;
        protected Config appConfig;
        protected ClusterCommands.CommandBlock block;

        protected List<String> classPathList;

        public void setBlock(ClusterCommands.CommandBlock block) {
            this.block = block;
        }

        public ClusterCommands.CommandBlock getBlock() {
            return block;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public ClusterConfig getClusterConfig() {
            return clusterConfig;
        }

        public Config getAppConfig() {
            return appConfig;
        }

        public void setClusterConfig(ClusterConfig clusterConfig) {
            this.clusterConfig = clusterConfig;
        }

        public void setAppConfig(Config appConfig) {
            this.appConfig = appConfig;
        }

        public void log(String str) {
            if (appConfig != null) {
                appConfig.log(str);
            } else {
                clusterConfig.log(str);
            }
        }

        public void setClassPathList(List<String> classPathList) {
            this.classPathList = classPathList;
        }

        public List<String> getClassPathList() {
            return classPathList;
        }

        public ClusterUnit edit(Consumer<ClusterUnit> f) {
            f.accept(this);
            return this;
        }
    }
}
