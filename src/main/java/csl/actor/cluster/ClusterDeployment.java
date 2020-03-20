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

import java.io.File;
import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ClusterDeployment<AppConfType extends ConfigBase,
        PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>> {
    public static String NON_DRIVER_SYMBOL_CONF = "-";
    public static String NON_DRIVER_PROPERTY_CONF = "csl.actor.cluster.conf";
    public static String NON_DRIVER_FILE_CONF = "cluster-config.txt"; //under appDir
    public static String NON_DRIVER_PROPERTY_APP_NAME = "csl.actor.cluster.appName";

    protected String masterMainType; //for remoteDriver
    protected List<String> masterMainArgs = new ArrayList<>(); //for remoteDriver

    protected boolean driverMode = true;
    protected Class<AppConfType> defaultConfType;
    protected Class<PlaceType> placeType;

    protected String appName;
    protected ClusterUnit<AppConfType> master;
    protected List<ClusterUnit<AppConfType>> nodes = new ArrayList<>();
    protected Map<ClusterUnit<AppConfType>, Process> processes = new ConcurrentHashMap<>();
    protected ActorSystemRemote system;

    protected PlaceType masterPlace;

    protected ActorRef attachedPlace;

    protected ClusterHttp http;

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface PropertyInterface {
        String value() default "";
    }

    /**
     * starts cluster as remote-driver mode
     * @param args {configFilePath, masterMainType, masterMainArgs}
     */
    public static void main(String[] args) throws Exception {
        new ClusterDeployment<>(ConfigBase.class, ActorPlacementForCluster.class)
                .runAsRemoteDriver(args);
    }

    public void runAsRemoteDriver(String... args) throws Exception {
        String masterMainType = args[0];
        String configFilePath = args[1];
        List<String> masterMainArgs = Arrays.asList(args).subList(2, args.length);
        runAsRemoteDriver(configFilePath, masterMainType, masterMainArgs);
    }

    public void runAsRemoteDriver(String configFilePath, String masterMainType, List<String> masterMainArgs) throws Exception {
        int n = deployAsRemoteDriver(configFilePath, masterMainType, masterMainArgs)
                .waitFor();
        System.exit(n);
    }

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

    public ClusterHttp getHttp() {
        return http;
    }

    public ActorSystemRemote getSystem() {
        return system;
    }

    /**
     * @return true if the running process is a driver
     */
    public boolean isDriverMode() {
        return driverMode;
    }

    public void setDriverMode(boolean driverMode) {
        this.driverMode = driverMode;
    }

    public String getMasterMainType() {
        return masterMainType;
    }

    public void setMasterMainType(String masterMainType) {
        this.masterMainType = masterMainType;
    }

    public List<String> getMasterMainArgs() {
        return masterMainArgs;
    }

    public Process deployAsRemoteDriver(String confFile, String masterMainType, List<String> masterArgs) {
        setMasterMainType(masterMainType);
        getMasterMainArgs().addAll(masterArgs);
        return deployAsRemoteDriver(confFile);
    }

    public Process deployAsRemoteDriver(String confFile) {
        try {
            return deployAsRemoteDriver(loadConfigFile(confFile));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public Process deployAsRemoteDriver(List<ClusterCommands.ClusterUnit<AppConfType>> units) throws Exception {
        deployMasterInitMaster(units);
        deployMasterInitAppName();
        deployFiles(master);
        deployNodesAsRemoteDriver(units);
        deployNodeStartProcess(master);
        attachAsRemoteDriver(ActorAddress.get(master.getDeploymentConfig().getAddress()));
        return processes.get(master);
    }

    public void deployNodesAsRemoteDriver(List<ClusterUnit<AppConfType>> units) {
        units.stream()
                .filter(u -> !u.getDeploymentConfig().master)
                .forEach(this::deployNodeFiles);
    }

    public void attachAsRemoteDriver(ActorAddress.ActorAddressRemote addr) {
        if (system == null) {
            attachInitSystem();
        }
        try {
            Exception last = null;
            for (int i = 0; i < 12; ++i) {
                try {
                    Thread.sleep(5_000);
                    attachInitPlaceRef(addr);

                    String s = ResponsiveCalls.sendTask(getSystem(), getPlace(),
                            Object::toString).get(30, TimeUnit.SECONDS);
                    master.log("launched: %s", s);
                    return;
                } catch (Exception ex) {
                    last = ex;
                    if (!ex.toString().contains("connection refused")) {
                        throw ex;
                    }
                }
            }
            throw last;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     *
     * @param confFile a cluster commands file parsed by {@link ClusterCommands#loadConfigFile(String)}, or
     *                   special symbol "-" ({@link #NON_DRIVER_SYMBOL_CONF}) for non-driver mode
     *                   and then it loads from the system property {@link #NON_DRIVER_PROPERTY_CONF}.
     *
     * @return the created instance of {@link ActorPlacementForCluster}
     */
    public PlaceType deploy(String confFile) {
        try {
            return deploy(loadConfigFile(confFile));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public PlaceType deployAsNonDriver() {
        return deploy("-");
    }

    public List<ClusterUnit<AppConfType>> loadConfigFile(String confFile) {
        if (confFile.equals(NON_DRIVER_SYMBOL_CONF)) {
            setDriverMode(false);
            confFile = System.getProperty(NON_DRIVER_PROPERTY_CONF, NON_DRIVER_FILE_CONF);
        }
        confFile = confFile.replaceAll("/", File.separator);
        return new ClusterCommands<>(defaultConfType).loadConfigFile(confFile);
    }

    public String getUnitMainType(ClusterUnit<AppConfType> unit) {
        if (unit.getDeploymentConfig().master) {
            return masterMainType;
        } else {
            return getNodeMainType().getName();
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
        deployMasterInitMaster(units);
        deployMasterInitAppName();
        deployMasterInitSystem();
        deployMasterInitUncaughtHandler();
        deployFiles(master);
        deployMasterStartSystem();
        deployMasterAfterSystemInit(units);
    }

    protected void deployMasterInitMaster(List<ClusterUnit<AppConfType>> units) {
        master = units.stream()
                .filter(u -> u.getDeploymentConfig().master)
                .findFirst()
                .orElseThrow(RuntimeException::new);
        if (master == null) {
            master = master();
        }
    }

    protected void deployMasterInitAppName() {
        this.appName = getAppName();
    }

    protected void deployMasterInitSystem() {
        system = new ActorSystemCluster();
        system.getLocalSystem().setLogger(new ConfigBase.SystemLoggerHeader(system.getLogger(), master.getAppConfig()));
    }

    protected void deployMasterInitUncaughtHandler() {
        setDefaultUncaughtHandler(master.getDeploymentConfig());
    }

    protected void deployMasterStartSystem() {
        system.startWithoutWait(ActorAddress.get(master.getDeploymentConfig().getAddress()));
    }

    protected void deployMasterAfterSystemInit(List<ClusterUnit<AppConfType>> units) throws Exception {
        deployMasterAfterSystemInitLogColor();
        deployMasterAfterSystemInitPath();
        master.log("master %s: path %s", master.getDeploymentConfig().getAddress(), ConfigDeployment.getPathModifier(system));

        deployMasterAfterSystemInitPlace();
        master.log("master %s: started %s", master.getDeploymentConfig().getAddress(), masterPlace);

        deployMasterAfterSystemInitSendConfigToUnits(units);

        deployMasterAfterSystemInitHttp();
    }

    protected void deployMasterAfterSystemInitLogColor() {
        System.setProperty("csl.actor.logColor", Integer.toString(master.getDeploymentConfig().getLogColorDefault()));
    }

    protected void deployMasterAfterSystemInitPath() throws Exception {
        ConfigDeployment.PathModifierHost ph = master.getDeploymentConfig().setPathModifierWithBaseDir(system);
        ph.setApp(getAppName());
        System.setProperty("csl.actor.path.app", ph.getApp());
        setLogFile(master.getDeploymentConfig(), ph);
    }

    public static void setLogFile(ConfigDeployment conf, ConfigDeployment.PathModifier pm) throws Exception {
        if (conf.logFile) {
            Path p = pm.getExpanded(conf.logFilePath);
            Path parent = p.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }
            System.setErr(new LogFileWriter(System.err, p, conf.logFilePreserveColor));
        }
    }

    protected void deployMasterAfterSystemInitPlace() throws Exception {
        masterPlace = createPlace(placeType, system,
                new ActorPlacement.PlacementStrategyRoundRobin(0));
        masterPlace.setDeployment(this);
        masterPlace.setLogger(master.getDeploymentConfig());
    }

    protected void deployMasterAfterSystemInitSendConfigToUnits(List<ClusterUnit<AppConfType>> units) {
        Map<ActorAddress, AppConfType> configMap = masterPlace.getRemoteConfig();
        units.forEach(u ->
                configMap.put(ActorAddress.get(u.getDeploymentConfig().getAddress()), u.getAppConfig()));
    }

    protected void deployMasterAfterSystemInitHttp() {
        int port = master.getDeploymentConfig().httpPort;
        if (port > 0) {
            http = new ClusterHttp(this);
            String host = master.getDeploymentConfig().httpHost;
            if (host.isEmpty()) {
                host = master.getDeploymentConfig().host;
            }
            http.start(host, port);
        }
    }

    public void deployNodes(List<ClusterUnit<AppConfType>> units) {
        units.stream()
                .filter(u -> !u.getDeploymentConfig().master)
                .forEach(u ->
                    system.execute(() -> deployNode(u)));
    }


    public void deployNode(ClusterUnit<AppConfType> unit) {
        try {
            unit.log("%s: deploy: %s", appName, unit.getDeploymentConfig().getAddress());
            deployNodeFiles(unit);
            deployNodeStartProcess(unit);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void deployNodeFiles(ClusterUnit<AppConfType> unit) {
        try {
            nodes.add(unit);
            unit.log("%s: deploy: %s files", appName, unit.getDeploymentConfig().getAddress());
            deployFiles(unit);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void deployNodeStartProcess(ClusterUnit<AppConfType> unit) {
        try {
            String javaCmd = getCommandWithDir(unit, getJavaCommand(unit));

            processes.put(unit,
                    launch(unit, getAppName(),
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
                getJavaCommandOptions(unit),
                escape(getUnitMainType(unit)),
                getJavaCommandArgs(unit));
    }

    protected String getJavaCommandOptions(ClusterUnit<AppConfType> unit) throws Exception {
        ConfigDeployment.PathModifier pm = ConfigDeployment.getPathModifier(system);
        String pathProps = "";
        if (pm instanceof ConfigDeployment.PathModifierHost) {
            ConfigDeployment.PathModifierHost pmh = (ConfigDeployment.PathModifierHost) pm;
            String app = pmh.getApp();
            if (app != null) {
                pathProps += escape("-Dcsl.actor.path.app=" + app) + " ";
            }
        }

        if (unit.getDeploymentConfig().master) {
            pathProps += escape("-D" + NON_DRIVER_PROPERTY_CONF + "=" +
                    getAppName() + "/" + NON_DRIVER_FILE_CONF) + " "; //suppose the working dir is baseDir
        }


        return escape("-Dcsl.actor.logColor=" + unit.getAppConfig().get("logColor")) + " " +
                escape("-Dcsl.actor.logFile=" + unit.getDeploymentConfig().logFile) + " " +
                escape("-Dcsl.actor.logFilePath=" + unit.getDeploymentConfig().logFilePath) + " " +
                escape("-Dcsl.actor.logFilePreserveColor=" + unit.getDeploymentConfig().logFilePreserveColor) + " " +
                escape("-D" + NON_DRIVER_PROPERTY_APP_NAME + "=" + getAppName()) + " " +
                pathProps +
                getJavaCommandOptionsClassPath(unit);
    }

    protected String getJavaCommandArgs(ClusterUnit<AppConfType> unit) throws Exception {
        if (unit.getDeploymentConfig().master) {
            String args = "";
            if (unit.getDeploymentConfig().configPathAsMasterFirstArgument) {
                args += "- ";
            }
            args += masterMainArgs.stream()
                    .map(this::escape)
                    .collect(Collectors.joining(" "));
            return args;
        } else {
            return unit.getDeploymentConfig().getAddress() + " " +
                    master.getDeploymentConfig().getAddress() + " " +
                    escape(getPlaceType().getName());
        }
    }

    protected String escape(String s) {
        return "'" + s + "'";
    }

    protected String getJavaCommandOptionsClassPath(ClusterUnit<AppConfType> unit) {
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
        unit.setAppConfigLogHeader();
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
        unit.setAppConfigLogHeader();
        return unit;
    }

    public String getAppName() {
        if (appName == null) {
            String name = System.getProperty(NON_DRIVER_PROPERTY_APP_NAME, "");
            if (name.isEmpty()) {
                appName = getNextAppName();
            } else {
                appName = name;
            }
        }
        return appName;
    }

    public String getNextAppName() {
        String head;
        if (master != null)  {
            head = master.getDeploymentConfig().appNameHeader;
        } else {
            head = "app";
        }
        return ConfigDeployment.getAppName(head);
    }

    public static List<String> sshCommand(ClusterUnit<?> unit, String appCmd) {
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

    public static Process launch(ClusterUnit<?> unit, String appName, List<String> cmd) {
        return launch(unit, null, appName, cmd);
    }

    public static Process launch(ClusterUnit<?> unit, File dir, String appName, List<String> cmd) {
        try {
            unit.log("%s: command: %s : %s", appName, unit.getDeploymentConfig().getAddress(), cmd);
            ProcessBuilder builder = new ProcessBuilder().command(cmd);
            builder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .redirectError(ProcessBuilder.Redirect.INHERIT);
            if (dir != null) {
                builder.directory(dir);
            }
            return builder.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void deployFiles(ClusterUnit<?> unit) throws Exception {
        ConfigDeployment conf = unit.getDeploymentConfig();

        if (conf.master) { //suppose the code is running under the master
            if (driverMode) {
                if (conf.sharedDeploy) {
                    //the baseDir is shared between all nodes
                    new ClusterFiles(getAppName(), unit).deployFiles();
                } else {
                    new ClusterFiles.ClusterFilesSsh(getAppName(), unit).deployFiles();
                }
            } else {
                String appName = getAppName();
                unit.setClassPathList(Arrays.stream(System.getProperty("java.class.path", "").split(Pattern.quote(File.pathSeparator)))
                    .filter(item -> item.startsWith(appName)) //appName/jars/
                    .collect(Collectors.toList()));
            }
        } else {
            if (driverMode) {
                if (conf.sharedDeploy) {
                    String masterBase = "^" + Pattern.quote(Paths.get(master.getDeploymentConfig().baseDir).toString());
                    String unitBase = conf.baseDir;
                    unit.setClassPathList(master.getClassPathList().stream()
                            .map(p -> p.replaceFirst(masterBase, unitBase))
                            .collect(Collectors.toList()));
                } else {
                    new ClusterFiles.ClusterFilesSsh(getAppName(), unit).deployFiles();
                }
            } else {
                unit.setClassPathList(master.getClassPathList());
            }
        }
    }


    public static void setDefaultUncaughtHandler(ConfigBase base) {
        Thread.setDefaultUncaughtExceptionHandler((t,ex) -> {
            base.log(ex, "uncaught: %s", t);
        });
    }

    public static class NodeMain {
        public static void main(String[] args) throws Exception {
            new NodeMain().run(args);
        }

        protected int logColor = ActorSystem.systemPropertyColor("csl.actor.logColor", 0);
        protected ConfigDeployment logger;
        protected ActorAddress.ActorAddressRemote selfAddress;
        protected ActorSystemRemote system;
        protected ActorPlacement.ActorPlacementDefault place;

        public void run(String[] args) throws Exception {
            initLogger();

            String selfAddr = args[0];
            String joinAddr = args[1];
            String placeType = (args.length > 2 ? args[2] : "");

            selfAddress = initSelfAddress(selfAddr);
            system = initSystem();

            initPath();
            logger.log(logColor, "%s: path %s", selfAddress, ConfigDeployment.getPathModifier(system));

            startSystem();
            place = initPlace(placeType);

            logger.log(logColor, "%s: joining to %s", place, joinAddr);
            join(joinAddr);
        }

        protected void initLogger() {
            logger = new ConfigDeployment();
            logger.read("csl.actor", System.getProperties());
            setDefaultUncaughtHandler(logger);
        }

        protected ActorAddress.ActorAddressRemote initSelfAddress(String selfAddr) {
            ActorAddress.ActorAddressRemote selfAddrObj = ActorAddress.get(selfAddr);
            logger.host = selfAddrObj.getHost();
            logger.port = selfAddrObj.getPort();
            return selfAddrObj;
        }

        protected ActorSystemRemote initSystem() {
            ActorSystemRemote system = new ActorSystemCluster();
            system.getLocalSystem().setLogger(new ConfigBase.SystemLoggerHeader(system.getLogger(), logger));
            return system;
        }

        protected void initPath() throws Exception {
            //the working directory is the baseDir
            ConfigDeployment.PathModifierHost ph = ConfigDeployment.setPathModifierWithBaseDir(system,
                    Paths.get(".").toAbsolutePath().normalize().toString());
            ph.setHost(selfAddress.getHost(), selfAddress.getPort());
            String appName = System.getProperty("csl.actor.path.app", "");
            ph.setApp(appName);
            setLogFile(logger, ph);
        }

        protected void startSystem() {
            system.startWithoutWait(selfAddress);
        }

        protected ActorPlacement.ActorPlacementDefault initPlace(String placeType) throws Exception {
            ActorPlacement.ActorPlacementDefault p = createPlace(placeType, system, new ActorPlacement.PlacementStrategyUndertaker());
            p.setLogger(logger);
            return p;
        }

        protected void join(String joinAddr) {
            place.join(ActorAddress.ActorAddressRemote.get(joinAddr));
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
        protected ClusterDeployment<AppConfType, ?> deployment;

        public ActorPlacementForCluster(ActorSystem system, String name) {
            super(system, name);
        }

        public ActorPlacementForCluster(ActorSystem system, String name, PlacementStrategy strategy) {
            super(system, name, strategy);
        }

        public ActorPlacementForCluster(ActorSystem system) {
            super(system);
        }

        public void setDeployment(ClusterDeployment<AppConfType, ?> deployment) {
            this.deployment = deployment;
        }

        public ClusterDeployment<AppConfType, ?> getDeployment() {
            return deployment;
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
                        master.log("%s: joined: %s  remaining=%,d", joinedHost, appName, nodeAddrs.size());
                    } else {
                        master.log("%s: waiting join: limit=%,d remaining=%,d", appName, limit, nodeAddrs.size());
                        if (nodeAddrs.size() <= 5) {
                            master.log("  remaining nodes: " + nodeAddrs);
                        }
                        --limit;
                    }
                } catch (Exception ex) {
                    //
                    master.log("%s %s", appName, ex);
                    break;
                }
            }
            master.log("%s launched %,d nodes", appName, getClusterSize());
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

    @PropertyInterface("cluster")
    public ClusterStats getClusterStats() {
        return new ClusterStats().set(this);
    }

    public static class ClusterStats implements Serializable, ClusterHttp.ToJson {
        public String appName;
        public Class<? extends ConfigBase> defaultConfType;
        public Class<?> nodeMainType;
        public Class<? extends ActorPlacementForCluster<?>> placeType;
        public ClusterUnit<?> master;
        public List<ClusterUnit<?>> nodes;

        public ClusterStats set(ClusterDeployment<?,?> d) {
            appName = d.getAppName();
            defaultConfType = d.getDefaultConfType();
            nodeMainType = d.getNodeMainType();
            placeType = d.getPlaceType();
            master = d.getMaster();
            nodes = new ArrayList<>(d.getNodes());
            return this;
        }

        @Override
        public Map<String, Object> toJson(Function<Object, Object> valueConverter) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("appName", toJson(valueConverter, appName, ""));
            json.put("defaultConfType", toJson(valueConverter, defaultConfType, ""));
            json.put("nodeMainType", toJson(valueConverter, nodeMainType, ""));
            json.put("placeType", toJson(valueConverter, placeType, ""));
            json.put("master", toJson(valueConverter, master, null));
            json.put("nodes", toJson(valueConverter, nodes, new ArrayList<>()));
            return json;
        }
    }

    public ActorRef getPlace(ActorRef actor) {
        if (actor instanceof ActorRefRemote) {
            ActorAddress address = ((ActorRefRemote) actor).getAddress();
            ActorPlacement.AddressListEntry entry = placeGet(p -> p.getEntry(address));
            return entry.getPlacementActor().ref(getSystem());
        } else if (actor instanceof Actor) {
            try {
                //temporary place
                return createPlace(placeType, system, new ActorPlacement.PlacementStrategyRoundRobin(0));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } else {
            return getPlace();
        }
    }

    @PropertyInterface("placement-move")
    public CompletableFuture<ActorRef> move(ActorRef actor, ActorAddress targetHost) {
        return ResponsiveCalls.<ActorPlacementForCluster<AppConfType>, ActorRef>sendTask(getSystem(),
                getPlace(actor),
                (a) -> a.move(actor.asLocal(), targetHost));
    }

    @PropertyInterface("actor-load-and-send")
    public CompletableFuture<?> loadAndSendToActor(ActorRef target, String serializedMailboxPath) {
        return ResponsiveCalls.sendTaskConsumer(getSystem(), target, (a) -> {
            //temporary manager
            //TODO the serializedMailbxPath is expanded ?
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

    @PropertyInterface("placement-shutdown")
    public CompletableFuture<?> shutdown(ActorAddress targetHost) {
        return ResponsiveCalls.<ActorPlacement.ActorPlacementDefault>sendTaskConsumer(getSystem(),
                placeGet(p -> p.getEntry(targetHost)).getPlacementActor().ref(getSystem()),
                new ShutdownTask());
    }

    public static class ShutdownTask implements CallableMessage.CallableMessageConsumer<ActorPlacement.ActorPlacementDefault> {
        @Override
        public void accept(ActorPlacement.ActorPlacementDefault self) {
            self.close();
            new Thread() { public void run() { //the shutting down thread
                try {
                    Thread.sleep(3_000);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                self.getSystem().close();
            }}.start();
            try {
                self.getSystem().awaitClose(6_000, TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public CompletableFuture<?> shutdownCluster() {
        List<CompletableFuture<?>> tasks = new ArrayList<>(masterPlace.getClusterSize());
        for (ActorPlacement.AddressListEntry e : masterPlace.getCluster()) {
            tasks.add(shutdown(e.getPlacementActor()));
        }
        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0]));
    }

    public void destroyClusterProcesses() {
        getProcesses().forEach((unit, proc) -> {
            if (proc.isAlive()) {
                unit.log("%s : destroy process: %s", unit, proc);
                proc.destroy();
            }
        });
    }

    @PropertyInterface("cluster-shutdown-all")
    public void shutdownAll() {
        getMaster().log("shutdownAll");
        try {
            shutdownCluster().get(3, TimeUnit.MINUTES);
        } catch (Exception ex) {
            //
        }
        destroyClusterProcesses();
        getSystem().close();
        if (http != null) {
            http.close();
        }
    }

    /////////

    @PropertyInterface("placement")
    public ActorRef getPlace() {
        if (masterPlace != null) {
            return getMasterPlace();
        } else {
            return attachedPlace;
        }
    }

    @PropertyInterface("placement-config-master")
    public AppConfType getMasterConfig() {
        if (master != null) {
            return master.getAppConfig();
        } else {
            return placeGet(p -> p.getRemoteConfig().get(p.getSelfAddress().getHostAddress()));
        }
    }

    public ActorRef getAttachedPlace() {
        return attachedPlace;
    }

    public void attach(String hostAndPort) {
        attach(ActorAddress.get(hostAndPort));
    }

    public void attach(String host, int port) {
        attach(ActorAddress.get(host, port));
    }

    public void attach(ActorAddress.ActorAddressRemote addr) {
        if (system == null) {
            attachInitSystem();
        }
        try {
            attachInitPlaceRef(addr);

            CompletableFuture.allOf(
                    attachInitRun(a -> a.getDeployment().getAppName(), r -> this.appName = r),
                    attachInitRun(a -> a.getDeployment().getMaster(), r -> master = r),
                    attachInitRun(a -> a.getDeployment().getNodes(), r -> nodes = r)
                ).get(30, TimeUnit.SECONDS);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void attachInitSystem() {
        system = new ActorSystemCluster();
        int port;
        try (ServerSocket sock = new ServerSocket(0)) { //obtain a dynamic port
            port = sock.getLocalPort();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        system.startWithoutWait(port);
    }

    protected void attachInitPlaceRef(ActorAddress.ActorAddressRemote addr) {
        ActorAddress.ActorAddressRemoteActor place = addr.getActor(ActorPlacement.PLACEMENT_NAME);
        attachedPlace = place.ref(system);
    }

    protected <T> CompletableFuture<?> attachInitRun(CallableMessage<PlaceType, T> getter,
                                                     Consumer<T> setter) {
        return ResponsiveCalls.sendTask(getSystem(), getPlace(),
                getter).thenAccept(setter);
    }

    public <T> T placeGet(CallableMessage<PlaceType, T> getter) {
        try {
            return ResponsiveCalls.sendTask(getSystem(), getPlace(), getter)
                    .get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public <T> T placeGet(ActorAddress.ActorAddressRemote host, CallableMessage<PlaceType, T> getter) {
        ActorAddress.ActorAddressRemoteActor addr;
        try {
            addr = this.<ActorAddress.ActorAddressRemoteActor>placeGet(mp -> mp.getEntry(host).getPlacementActor());
        } catch (Exception ex) {
            throw new RuntimeException("placeGet.1: getPlacementActor", ex);
        }
        try {
            return ResponsiveCalls.sendTask(getSystem(),
                    addr,
                    getter)
                    .get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new RuntimeException("placeGet.2: sendTask " + addr, ex);
        }
    }

    //////

    @PropertyInterface("system")
    public SystemStats getAttachedSystemStats() {
        return placeGet(p -> new SystemStats().set(p));
    }

    @PropertyInterface("system-actors")
    public Map<String, ActorRef> getAttachedSystemNamedActorMap() {
        return placeGet(p -> toRefMap(((ActorSystemRemote) p.getSystem()).getLocalSystem().getNamedActorMap()));
    }

    @PropertyInterface("system-process-count")
    public int getAttachedSystemProcessingCount() {
        return placeGet(p -> ((ActorSystemRemote) p.getSystem()).getLocalSystem().getProcessingCount().get());
    }

    @PropertyInterface("system-connections")
    public Map<ActorAddress, NetworkStats> getAttachedSystemRemoteConnectionMap() {
        return placeGet(p -> ((ActorSystemRemote) p.getSystem()).getConnectionMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new NetworkStats().send(e.getValue()))));
    }

    @PropertyInterface("system-server-receive")
    public NetworkStats getAttachedSystemRemoteServerReceive() {
        return placeGet(p -> new NetworkStats().receive((ActorSystemRemote) p.getSystem()));
    }

    @PropertyInterface("placement-total-threads")
    public int getAttachedPlacementTotalThreads() {
        return placeGet(ActorPlacement.ActorPlacementDefault::getTotalThreads);
    }

    @PropertyInterface("placement-clusters")
    public List<ActorPlacement.AddressListEntry> getAttachedPlacementCluster() {
        return placeGet(ActorPlacement.ActorPlacementDefault::getCluster);
    }

    @PropertyInterface("placement-clusters-with-self")
    public List<ActorPlacement.AddressListEntry> getAttachedPlacementClusterWithSelf() {
        return placeGet(ActorPlacement.ActorPlacementDefault::getClusterWithSelf);
    }

    @PropertyInterface("placement-created-actors")
    public long getAttachedPlacementCreatedActors() {
        return placeGet(ActorPlacement.ActorPlacementDefault::getCreatedActors);
    }

    @PropertyInterface("placement-remote-config")
    public Map<ActorAddress, ? extends ConfigBase> getAttachedPlacementForClusterRemoteConfig() {
        return placeGet(ActorPlacementForCluster::getRemoteConfig);
    }

    public static Map<String, ActorRef> toRefMap(Map<String, Actor> map) {
        return map.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static class SystemStats implements Serializable, ClusterHttp.ToJson {
        public int throughput;
        public int threads;
        public String systemToString;
        public String placementStrategy;

        public SystemStats set(ActorPlacementForCluster<?> a) {
            throughput = ((ActorSystemRemote) a.getSystem()).getLocalSystem().getThroughput();
            systemToString = a.getSystem().toString();
            placementStrategy = a.getStrategy().getDescription();
            threads = a.getSystem().getThreads();
            return this;
        }

        @Override
        public Map<String, Object> toJson(Function<Object, Object> valueConverter) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("throughput", toJson(valueConverter, (long) throughput));
            json.put("threads", toJson(valueConverter, (long) threads));
            json.put("systemToString", toJson(valueConverter, systemToString, ""));
            json.put("placementStrategy", toJson(valueConverter,
                    placementStrategy, ""));
            return json;
        }
    }

    public static class NetworkStats implements Serializable, ClusterHttp.ToJson {
        public ActorAddress address;
        public long count;
        public long messages;
        public Duration time;
        public long bytes;
        public long errors;

        public NetworkStats() { }

        public NetworkStats send(ActorSystemRemote.ConnectionActor a) {
            address = a.getAddress();
            count = a.getConnection().getRecordSendCount();
            messages = a.getRecordSendMessages();
            time = a.getRecordSendMessageTime();
            errors = a.getConnection().getRecordSendErrors();
            bytes = a.getConnection().getRecordSendBytes();
            return this;
        }

        public NetworkStats receive(ActorSystemRemote system) {
            address = system.getServerAddress();
            count = system.getServer().getRecordReceiveCount();
            messages = system.getDeliverer().getReceiveMessages();
            time = system.getServer().getRecordReceiveTime();
            errors = system.getServer().getRecordReceiveErrors();
            bytes = system.getServer().getRecordReceiveBytes();
            return this;
        }

        @Override
        public String toString() {
            return "{" +
                    "address=" + address +
                    ", count=" + count +
                    ", messages=" + messages +
                    ", time=" + time +
                    ", bytes=" + bytes +
                    ", errors=" + errors +
                    '}';
        }

        @Override
        public Map<String, Object> toJson(Function<Object, Object> valueConverter) {
            Map<String,Object> json = new LinkedHashMap<>();
            json.put("address", toJson(valueConverter, address, ""));
            json.put("count", toJson(valueConverter, count));
            json.put("messages", toJson(valueConverter, messages));
            json.put("time", toJson(valueConverter, time, Duration.ZERO));
            json.put("bytes", toJson(valueConverter, bytes));
            json.put("errors", toJson(valueConverter, errors));
            return json;
        }
    }

    ///////

    @PropertyInterface("system")
    public SystemStats getAttachedSystemStats(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, p -> new SystemStats().set(p));
    }

    @PropertyInterface("system-actors")
    public Map<String, ActorRef> getAttachedSystemNamedActorMap(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, p -> toRefMap(((ActorSystemRemote) p.getSystem()).getLocalSystem().getNamedActorMap()));
    }

    @PropertyInterface("system-process-count")
    public int getAttachedSystemProcessingCount(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, p -> ((ActorSystemRemote) p.getSystem()).getLocalSystem().getProcessingCount().get());
    }

    @PropertyInterface("system-connections")
    public Map<ActorAddress, NetworkStats> getAttachedSystemRemoteConnectionMap(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, p -> ((ActorSystemRemote) p.getSystem()).getConnectionMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new NetworkStats().send(e.getValue()))));
    }

    @PropertyInterface("system-server-receive")
    public NetworkStats getAttachedSystemRemoteServerReceive(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, p -> new NetworkStats().receive((ActorSystemRemote) p.getSystem()));
    }

    @PropertyInterface("placement-total-threads")
    public int getAttachedPlacementTotalThreads(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, ActorPlacement.ActorPlacementDefault::getTotalThreads);
    }

    @PropertyInterface("placement-clusters")
    public List<ActorPlacement.AddressListEntry> getAttachedPlacementCluster(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, ActorPlacement.ActorPlacementDefault::getCluster);
    }

    @PropertyInterface("placement-clusters-with-self")
    public List<ActorPlacement.AddressListEntry> getAttachedPlacementClusterWithSelf(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, ActorPlacement.ActorPlacementDefault::getClusterWithSelf);
    }

    @PropertyInterface("placement-created-actors")
    public long getAttachedPlacementCreatedActors(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, ActorPlacement.ActorPlacementDefault::getCreatedActors);
    }

    @PropertyInterface("placement-remote-config")
    public Map<ActorAddress, ? extends ConfigBase> getAttachedPlacementForClusterRemoteConfig(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, ActorPlacementForCluster::getRemoteConfig);
    }


}
