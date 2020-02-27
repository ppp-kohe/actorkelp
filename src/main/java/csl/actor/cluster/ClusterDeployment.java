package csl.actor.cluster;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;
import csl.actor.cluster.ClusterCommands.ClusterUnit;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClusterDeployment<AppConfType extends ConfigBase,
        PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>> {
    protected Class<AppConfType> defaultConfType;
    protected Class<PlaceType> placeType;

    protected String appName;
    protected ClusterUnit<AppConfType> master;
    protected List<ClusterUnit<AppConfType>> nodes = new ArrayList<>();
    protected Map<ClusterUnit<AppConfType>, Process> processes = new ConcurrentHashMap<>();
    protected ActorSystemRemote system;

    protected PlaceType masterPlace;

    public ClusterDeployment(Class<AppConfType> defaultConfType, Class<PlaceType> placeType) {
        this.defaultConfType = defaultConfType;
        this.placeType = placeType;
    }

    public Class<AppConfType> getDefaultConfType() {
        return defaultConfType;
    }

    public Class<?> getNodeMainType() {
        return NodeMain.class;
    }

    public Class<PlaceType> getPlaceType() {
        return placeType;
    }

    public ClusterUnit<AppConfType> getMaster() {
        return master;
    }

    public List<ClusterUnit<AppConfType>> getNodes() {
        return nodes;
    }

    public Map<ClusterUnit<AppConfType>, Process> getProcesses() {
        return processes;
    }

    public PlaceType getMasterPlace() {
        return masterPlace;
    }

    public ActorSystemRemote getSystem() {
        return system;
    }

    public PlaceType deploy(String confFile) {
        try {
            return deploy(new ClusterCommands<>(defaultConfType).loadConfigFile(confFile));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @SafeVarargs
    public final PlaceType deploy(ClusterCommands.ClusterUnit<AppConfType>... units) {
        try {
            return deploy(Arrays.asList(units));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public PlaceType deploy(List<ClusterCommands.ClusterUnit<AppConfType>> units) throws Exception {
        deployMaster(units);
        deployNodes(units);
        awaitNodes();
        return getMasterPlace();
    }


    public void deployMaster(List<ClusterUnit<AppConfType>> units) throws Exception {
        master = units.stream()
                .filter(u -> u.getDeploymentConfig().master)
                .findFirst()
                .orElseThrow(RuntimeException::new);
        if (master == null) {
            master = master();
        }
        this.appName = getAppName();
        system = new ActorSystemRemote();

        deployFiles(master);
        system.startWithoutWait(master.getDeploymentConfig().port);

        deployMasterAfterSystemInit(units);
    }

    protected void deployMasterAfterSystemInit(List<ClusterUnit<AppConfType>> units) throws Exception {
        System.setProperty("csl.actor.logColor", Integer.toString(master.getDeploymentConfig().getLogColorDefault()));
        masterPlace = createPlace(placeType, system,
                new ActorPlacement.PlacementStrategyRoundRobin(0));
        Map<ActorAddress, AppConfType> configMap = masterPlace.getRemoteConfig();
        units.forEach(u ->
                configMap.put(ActorAddress.get(u.getDeploymentConfig().getAddress()), u.getAppConfig()));
    }

    public void deployNodes(List<ClusterUnit<AppConfType>> units) {
        units.stream()
                .filter(u -> !u.getDeploymentConfig().master)
                .forEach(u ->
                    system.execute(() -> deployNode(u)));
    }

    public void deployNode(ClusterUnit<AppConfType> unit) {
        try {
            nodes.add(unit);
            unit.log(String.format("%s: deploy: %s", appName, unit.getDeploymentConfig().getAddress()));
            deployFiles(unit);

            deployNodeStartProcess(unit);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void deployNodeStartProcess(ClusterUnit<AppConfType> unit) {
        try {
            String javaCmd = getCommandWithDir(unit, getJavaCommand(unit));

            processes.put(unit,
                    launch(unit,
                            sshCommand(unit, javaCmd)));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void awaitNodes() throws Exception {
        ClusterUnit<AppConfType> master = getMaster();
        String appName = getAppName();
        masterPlace.awaitsJoining(appName, master);
    }

    protected String getJavaCommand(ClusterUnit<AppConfType> unit) throws Exception {
        return String.format(unit.getDeploymentConfig().java,
                "-Dcsl.actor.logColor=" + unit.getAppConfig().get("logColor") + " "
                        + getClassPathOption(unit),
                escape(getNodeMainType().getName()),
                unit.getDeploymentConfig().getAddress() + " " +
                        master.getDeploymentConfig().getAddress() + " " +
                        escape(getPlaceType().getName()));
    }

    protected String escape(String s) {
        return "'" + s + "'";
    }

    protected String getClassPathOption(ClusterUnit<AppConfType> unit) {
        String cps = "";
        if (unit.getClassPathList() != null) {
            cps = String.format("-cp '%s'", String.join(unit.getDeploymentConfig().pathSeparator, unit.getClassPathList()));
        }
        return cps;
    }

    protected String getCommandWithDir(ClusterUnit<AppConfType> unit, String cmd) {
        String dir = unit.getDeploymentConfig().baseDir;
        if (!dir.isEmpty()) {
            cmd = String.format("cd '%s'; %s", dir, cmd);
        }
        return cmd;
    }

    @SuppressWarnings("unchecked")
    public ClusterUnit<AppConfType> master() {
        ClusterUnit<AppConfType> unit = new ClusterUnit<>();
        ConfigDeployment conf = new ConfigDeployment(defaultConfType);
        conf.master = true;
        unit.setDeploymentConfig(conf);
        unit.setAppConfig((AppConfType) conf.createAppConfig());
        unit.setName("localhost");
        return unit;
    }


    @SuppressWarnings("unchecked")
    public ClusterUnit<AppConfType> node(String host, int port) {
        ClusterUnit<AppConfType> unit = new ClusterUnit<>();
        ConfigDeployment conf = new ConfigDeployment();
        conf.host = host;
        conf.port = port;
        unit.setDeploymentConfig(conf);
        unit.setAppConfig((AppConfType) conf.createAppConfig());
        unit.setName(host + ":" + port);
        return unit;
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

    public List<String> sshCommand(ClusterUnit<?> unit, String appCmd) {
        String ssh = String.format(unit.getDeploymentConfig().ssh,
                unit.getDeploymentConfig().host);
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

    public Process launch(ClusterUnit<?> unit, List<String> cmd) {
        try {
            unit.log(String.format("%s: command: %s : %s", getAppName(), unit.getDeploymentConfig().getAddress(), cmd));
            ProcessBuilder builder = new ProcessBuilder().command(cmd);
            builder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .redirectError(ProcessBuilder.Redirect.INHERIT);
            return builder.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void deployFiles(ClusterUnit<?> unit) throws Exception {
        ConfigDeployment conf = unit.getDeploymentConfig();

        String appName = getAppName();

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
            List<String> pathList = new ArrayList<>();
            for (String pathItem : items) {
                Path filePath = Paths.get(pathItem);
                if (Files.isDirectory(filePath)) { //creates jar and copies it
                    deployFilesDirToJar(unit, filePath, jarDir, pathList);
                } else { //copy jar
                    deployFilesCopyFile(unit, filePath, jarDir, pathList);
                }
            }

            unit.setClassPathList(pathList);

        } else if (!conf.master) {
            if (conf.sharedDeploy) {
                String masterBase = "^" + Pattern.quote(Paths.get(master.getDeploymentConfig().baseDir).toString());
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

    protected void deployFilesDirToJar(ClusterUnit<?> unit, Path filePath, Path jarDir, List<String> pathList) throws Exception {
        String header = unit.getDeploymentConfig().getAddress();
        int count = pathList.size();
        String fileName = String.format("%s-%03d.jar", filePath.getFileName(), count);
        Path jarPath = jarDir.resolve(fileName);
        pathList.add(jarPath.toString());

        unit.log(String.format("%s deployFiles: %s\n      %s\n  ->  %s", appName, header, filePath, jarPath));
        try (Stream<Path> ps = Files.walk(filePath);
             JarOutputStream out = new JarOutputStream(
                     new BufferedOutputStream(new FileOutputStream(jarPath.toFile())))) {
            ps.forEachOrdered(p -> writeToJar(unit, filePath, p, out));
        }
    }

    public void writeToJar(ClusterUnit<?> unit, Path startDir, Path path, JarOutputStream out) {
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

    protected void deployFilesCopyFile(ClusterUnit<?> unit, Path filePath, Path jarDir, List<String> pathList) throws Exception {
        String header = unit.getDeploymentConfig().getAddress();
        int count = pathList.size();
        String fileName = filePath.getFileName().toString();
        Path jarPath = jarDir.resolve(fileName);
        unit.log(String.format("%s deployFiles: %s\n      %s\n  ->  %s", getAppName(), header, filePath, jarPath));
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

    public static class NodeMain {
        public static void main(String[] args) throws Exception {
            new NodeMain().run(args);
        }

        public void run(String[] args) throws Exception {
            int n = Integer.parseInt(System.getProperty("csl.actor.logColor", "0"));
            ConfigBase logger = new ConfigBase();

            String selfAddr = args[0];
            String joinAddr = args[1];
            String placeType = (args.length > 2 ? args[2] : "");

            ActorSystemRemote system = new ActorSystemRemote();

            system.startWithoutWait(ActorAddress.get(selfAddr));
            ActorPlacement.ActorPlacementDefault p = createPlace(placeType, system, new ActorPlacement.PlacementStrategyUndertaker());

            logger.log(n, String.format("%s: joining to %s", p, joinAddr));
            p.join(ActorAddress.ActorAddressRemote.get(joinAddr));
        }

        @SuppressWarnings("unchecked")
        public static ActorPlacementForCluster<?> createPlace(String placeType, ActorSystem system,
                                                                       ActorPlacement.PlacementStrategy strategy) throws Exception {
            return ClusterDeployment.createPlace((Class<? extends ActorPlacementForCluster<ConfigBase>>) Class.forName(placeType), system, strategy);
        }
    }


    public static <AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
        PlaceType createPlace(Class<PlaceType> placeType, ActorSystem system,
                              ActorPlacement.PlacementStrategy strategy) throws Exception {
        return placeType.getConstructor(ActorSystem.class, ActorPlacement.PlacementStrategy.class)
                .newInstance(system, strategy);
    }

    public static abstract class ActorPlacementForCluster<AppConfType extends ConfigBase> extends ActorPlacement.ActorPlacementDefault {
        protected Map<ActorAddress, AppConfType> remoteConfig = new HashMap<>();
        protected BlockingQueue<ActorAddress> completed = new LinkedBlockingDeque<>();
        protected Set<ActorAddress> configSetSent = new HashSet<>();

        public ActorPlacementForCluster(ActorSystem system, String name) {
            super(system, name);
        }

        public ActorPlacementForCluster(ActorSystem system, String name, PlacementStrategy strategy) {
            super(system, name, strategy);
        }

        public ActorPlacementForCluster(ActorSystem system) {
            super(system);
        }

        /**
         * the constructor for nodes
         * @param system the system
         * @param strategy the strategy
         */
        public ActorPlacementForCluster(ActorSystem system, PlacementStrategy strategy) {
            super(system, strategy);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(ConfigSet.class, this::receiveConfigSet)
                    .with(super.initBehavior())
                    .build();
        }

        @Override
        protected PlacementStrategy initStrategy() {
            return new PlacementStrategyRoundRobinThreads();
        }

        public Map<ActorAddress, AppConfType> getRemoteConfig() {
            return remoteConfig;
        }

        public BlockingQueue<ActorAddress> getCompleted() {
            return completed;
        }

        @Override
        protected void updateTotalThreads() {
            super.updateTotalThreads();
            synchronized (this) {
                getCluster().stream()
                        .map(AddressListEntry::getPlacementActor)
                        .forEach(this::joined);
            }
        }


        protected void joined(ActorAddress addr) {
            if (!configSetSent.contains(addr)) {
                ActorRefRemote.get(getSystem(), addr)
                        .tell(new ConfigSet(remoteConfig), this);
                configSetSent.add(addr);
                completed.offer(addr);
            }
        }

        @SuppressWarnings("unchecked")
        public void receiveConfigSet(ConfigSet set) {
            remoteConfig.putAll((Map<ActorAddress, AppConfType>) set.getRemoteConfig());
        }

        public void awaitsJoining(String appName, ClusterUnit<AppConfType> master) {
            BlockingQueue<ActorAddress> completed = getCompleted();
            Set<ActorAddress> nodeAddrs = new HashSet<>(getRemoteConfig().keySet());
            nodeAddrs.remove(ActorAddress.get(master.getDeploymentConfig().getAddress()));
            int limit = Math.max(3, nodeAddrs.size() / getTotalThreads());
            while (!nodeAddrs.isEmpty() && limit > 0) {
                try {
                    ActorAddress joinedAddr = completed
                            .poll(master.getDeploymentConfig().joinTimeoutMs, TimeUnit.MILLISECONDS);
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
            master.log(String.format("%s launched %,d nodes", appName, getClusterSize()));
        }

        public ActorRef move(Actor actor, ActorAddress address) {
            try {
                return place(actor, getEntry(address).getPlacementActor())
                        .get(10, TimeUnit.SECONDS);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static class ConfigSet implements Serializable, KryoSerializable {
        public Map<ActorAddress, ? extends ConfigBase> remoteConfig;

        public ConfigSet() {}

        public ConfigSet(Map<ActorAddress, ? extends ConfigBase> remoteConfig) {
            this.remoteConfig = remoteConfig;
        }

        public Map<ActorAddress, ? extends ConfigBase> getRemoteConfig() {
            return remoteConfig;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            kryo.writeClassAndObject(output, remoteConfig);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void read(Kryo kryo, Input input) {
            remoteConfig = (Map<ActorAddress, ? extends ConfigBase>) kryo.readClassAndObject(input);
        }
    }

    ///////////////

    public CompletableFuture<ActorRef> move(ActorRef actor, ActorAddress targetHost) {
        return ResponsiveCalls.<ActorPlacementForCluster<AppConfType>, ActorRef>sendTask(getSystem(),
                getMasterPlace().getPlace(actor),
                (a, s) -> a.move(actor.asLocal(), targetHost));
    }

    public CompletableFuture<?> loadAndSendToActor(ActorRef target, String serializedMailboxPath) {
        return ResponsiveCalls.sendTaskConsumer(getSystem(), target, (a,s) -> {
            //temporary manager
            MailboxPersistable.PersistentFileManager m = MailboxPersistable.createPersistentFile(serializedMailboxPath, a.getSystem());
            MailboxPersistable.MessageOnStorage msg = new MailboxPersistable.MessageOnStorageFile(
                    new MailboxPersistable.PersistentFileReaderSource(serializedMailboxPath, 0, m));
            synchronized (msg) {
                Message<?> next = msg.readNext();
                while (next != null) {
                    a.tell(next.getData());
                    next = msg.readNext();
                }
            }
        });
    }

    public CompletableFuture<?> shutdown(ActorAddress targetHost) {
        return ResponsiveCalls.<ActorPlacement.ActorPlacementDefault>sendTaskConsumer(getSystem(),
                getMasterPlace().getEntry(targetHost).getPlacementActor().ref(getSystem()),
                (a,s) -> {
                    a.close();
                    new Thread() { public void run() { //the shutting down thread
                        try {
                            Thread.sleep(3_000);
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                        a.getSystem().close();
                    }}.start();
                    try {
                        a.getSystem().awaitClose(6_000, TimeUnit.MILLISECONDS);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                });
    }

    public CompletableFuture<?> shutdownAll() {
        List<CompletableFuture<?>> tasks = new ArrayList<>(masterPlace.getClusterSize());
        for (ActorPlacement.AddressListEntry e : masterPlace.getCluster()) {
            tasks.add(shutdown(e.getPlacementActor()));
        }
        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0]));
    }
}
