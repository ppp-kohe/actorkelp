package csl.actor.cluster;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;
import csl.actor.cluster.ClusterCommands.ClusterUnit;
import csl.actor.persist.MailboxPersistableReplacement;
import csl.actor.persist.PersistentFileManager;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;
import csl.actor.util.*;

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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ClusterDeployment<AppConfType extends ConfigBase,
        PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>> implements AutoCloseable {
    public static String NON_DRIVER_SYMBOL_CONF = "-";
    public static String NON_DRIVER_PROPERTY_CONF = "csl.actor.cluster.conf";
    public static String NON_DRIVER_FILE_CONF = "cluster-config.txt"; //under appDir
    public static String NON_DRIVER_PROPERTY_APP_NAME = "csl.actor.cluster.appName";
    public static String NON_DRIVER_PROPERTY_FLAG = "csl.actor.cluster.driver";

    protected String primaryMainType; //for remoteDriver
    protected List<String> primaryMainArgs = new ArrayList<>(); //for remoteDriver

    protected boolean driverMode = true;
    protected Class<AppConfType> defaultConfType;
    protected Class<PlaceType> placeType;

    protected String appName;
    protected ClusterUnit<AppConfType> primary;
    protected List<ClusterUnit<AppConfType>> nodes = new ArrayList<>();
    protected Map<ClusterUnit<AppConfType>, Process> processes = new ConcurrentHashMap<>();
    protected ActorSystemRemote system;

    protected PlaceType primaryPlace;

    protected String attachKryoBuilderType = "";//KryoBuilderCluster.class.getName();
    protected ActorRef attachedPlace;

    protected ClusterHttp http;

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface PropertyInterface {
        String value() default "";
        String help() default "";
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.PARAMETER)
    public @interface PropertyInterfaceArgument {
        String name();
        String help() default "";
    }

    /**
     * starts cluster as remote-driver mode
     * @param args {configFilePath, primaryMainType, primaryMainArgs}
     * @throws Exception an exception
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        new ClusterDeployment<>(ConfigBase.class, ActorPlacementForCluster.class)
                .runAsRemoteDriver(args);
    }

    public void runAsRemoteDriver(String... args) throws Exception {
        if (args.length < 2 || args[0].equals("--help")) {
            showHelp();
            return;
        }
        String primaryMainType = args[0];
        String configFilePath = args[1];
        List<String> primaryMainArgs = Arrays.asList(args).subList(2, args.length);
        runAsRemoteDriver(configFilePath, primaryMainType, primaryMainArgs);
    }

    public void showHelp() {
        System.err.println("<configFilePath> <primaryMainType> <primaryMainArgs>...");
    }

    public void runAsRemoteDriver(String configFilePath, String primaryMainType, List<String> primaryMainArgs) throws Exception {
        int n = deployAsRemoteDriver(configFilePath, primaryMainType, primaryMainArgs)
                .waitFor();
        System.exit(n);
    }

    public void runAsRemoteDriver(List<ClusterUnit<AppConfType>> units, String primaryMainType, List<String> primaryMainArgs) throws Exception {
        int n = deployAsRemoteDriver(units, primaryMainType, primaryMainArgs)
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

    public ClusterUnit<AppConfType> getPrimary() {
        return primary;
    }

    public List<ClusterUnit<AppConfType>> getNodes() {
        return nodes;
    }

    public Map<ClusterUnit<AppConfType>, Process> getProcesses() {
        return processes;
    }

    public PlaceType getPrimaryPlace() {
        return primaryPlace;
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

    public String getPrimaryMainType() {
        return primaryMainType;
    }

    public void setPrimaryMainType(String primaryMainType) {
        this.primaryMainType = primaryMainType;
    }

    public List<String> getPrimaryMainArgs() {
        return primaryMainArgs;
    }

    public Process deployAsRemoteDriver(String confFile, String primaryMainType, List<String> primaryArgs) {
        return deployAsRemoteDriver(confFile, null, primaryMainType, primaryArgs);
    }

    public Process deployAsRemoteDriver(String confFile, ConfigDeployment mergeDeployConf, String primaryMainType, List<String> primaryArgs) {
        setPrimaryMainType(primaryMainType);
        getPrimaryMainArgs().addAll(primaryArgs);
        return deployAsRemoteDriver(confFile, mergeDeployConf);
    }

    public Process deployAsRemoteDriver(List<ClusterUnit<AppConfType>> units, String primaryMainType, List<String> primaryArgs) {
        setPrimaryMainType(primaryMainType);
        getPrimaryMainArgs().addAll(primaryArgs);
        return deployAsRemoteDriver(units);
    }

    public Process deployAsRemoteDriver(String confFile) {
        return deployAsRemoteDriver(confFile, null);
    }

    public Process deployAsRemoteDriver(String confFile, ConfigDeployment mergeDeployConf) {
        return deployAsRemoteDriver(loadConfigFile(confFile, mergeDeployConf));
    }

    public Process deployAsRemoteDriver(List<ClusterCommands.ClusterUnit<AppConfType>> units) {
        try {
            deployUnitsAsConfigFile(units);
            deployPrimaryInitPrimary(units);
            deployPrimaryInitAppName();
            deployFiles(primary, true);
            deployNodesAsRemoteDriver(units);
            deployNodeStartProcess(primary);
            attachAsRemoteDriver(ActorAddress.get(primary.getDeploymentConfig().getAddress()));
            return processes.get(primary);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void deployUnitsAsConfigFile(List<ClusterUnit<AppConfType>> units) {
        if (!units.isEmpty()) {
            ClusterCommands.CommandBlockRoot root = units.get(0).getOrCreateRootBlock();
            units.stream()
                    .filter(unit -> unit.getTopBlock(unit.getBlock()) != root)
                    .map(ClusterUnit::getOrCreateBlock)
                    .forEach(root.getBlocks()::add);
        }
    }

    public void deployNodesAsRemoteDriver(List<ClusterUnit<AppConfType>> units) {
        units.stream()
                .filter(u -> !u.getDeploymentConfig().primary)
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
                            new ToStringMessage()).get(30, TimeUnit.SECONDS);
                    primary.log("launched: %s", s);
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

    public static class ToStringMessage implements CallableMessage<Actor, String>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        @Override
        public String call(Actor self) {
            return self.toString();
        }
    }

    /**
     *  {@link #deploy(String)} with the special symbol "-" {@link #NON_DRIVER_SYMBOL_CONF}
     * @return a created system
     * @see #deploy(String)
     */
    public ActorSystemRemote deploy() {
        return deploy(NON_DRIVER_SYMBOL_CONF);
    }

    /**
     *
     *  {@link #deploy(String, ConfigBase)} )} with the special symbol "-" {@link #NON_DRIVER_SYMBOL_CONF}
     * @param mergedConf the mergedConf for all unit's appConfig
     * @return a created system
     */
    public ActorSystemRemote deploy(AppConfType mergedConf) {
        return deploy(NON_DRIVER_SYMBOL_CONF, mergedConf);
    }

    /**
     *  {@link #deploy(String, ConfigBase, ConfigDeployment)} )} with the special symbol "-" {@link #NON_DRIVER_SYMBOL_CONF}
     * @param mergedConf the mergedConf for all unit's appConfig
     * @param mergedDeployConf  the mergedDeployConf
     * @return a created system
     */
    public ActorSystemRemote deploy(AppConfType mergedConf, ConfigDeployment mergedDeployConf) {
        return deploy(NON_DRIVER_SYMBOL_CONF, mergedConf, mergedDeployConf);
    }

    /**
     * {@link #deploy(List)} with units from {@link #loadConfigFile(String)} which loads the confFile
     * @param confFile a cluster commands file parsed by {@link ClusterCommands#loadConfigFile(String)}, or
     *                   special symbol "-" ({@link #NON_DRIVER_SYMBOL_CONF}) and then it loads from the system property {@link #NON_DRIVER_PROPERTY_CONF}.
     *
     * @return a created system
     * @see #loadConfigFile(String)
     */
    public ActorSystemRemote deploy(String confFile) {
        try {
            return deploy(loadConfigFile(confFile));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * {@link #deploy(List)} with units from {@link #loadConfigFile(String, ConfigBase)} which loads the confFile with merging the mergedConf
     * @param confFile a cluster commands file parsed by {@link ClusterCommands#loadConfigFile(String)} )}, or
     *                   special symbol "-" ({@link #NON_DRIVER_SYMBOL_CONF}) and then it loads from the system property {@link #NON_DRIVER_PROPERTY_CONF}.
     * @param mergedConf the mergedConf for all unit's appConfig
     * @return a created system
     */
    public ActorSystemRemote deploy(String confFile, AppConfType mergedConf) {
        try {
            return deploy(loadConfigFile(confFile, mergedConf));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * {@link #deploy(List)} with units from {@link #loadConfigFile(String, ConfigBase, ConfigDeployment)} which loads the confFile with merging the mergedConf
     * @param confFile a cluster commands file parsed by {@link ClusterCommands#loadConfigFile(String)} )}, or
     *                   special symbol "-" ({@link #NON_DRIVER_SYMBOL_CONF}) and then it loads from the system property {@link #NON_DRIVER_PROPERTY_CONF}.
     * @param mergedConf the mergedConf for all unit's appConfig
     * @param mergedDeployConf  the mergedDeployConf
     * @return a created system
     */
    public ActorSystemRemote deploy(String confFile, AppConfType mergedConf, ConfigDeployment mergedDeployConf) {
        try {
            return deploy(loadConfigFile(confFile, mergedConf, mergedDeployConf));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * load specified config file by {@link ClusterCommands}.
     * if the confFile is "-" ({@link #NON_DRIVER_SYMBOL_CONF}), it loads the path obtained from the property {@link #NON_DRIVER_PROPERTY_CONF}.
     *  Also, it changes the {@link #isDriverMode()} flag from the property {@link #NON_DRIVER_PROPERTY_FLAG}
     *    (the default value is the current value of {@link #driverMode}; the initial value is "true").
     *  <p>
     *  If no conf file supplied by {@link #NON_DRIVER_PROPERTY_CONF} with {@link #NON_DRIVER_SYMBOL_CONF}
     *    (or the property is the empty string ""),
     *     then it just returns a single primary unit created by {@link #primary(ConfigDeployment)}.
     *  </p>
     *  <p>
     *    if mergedDeployConf is supplied, it will be merged to all deploymentConfig of units.
     *    if the conf file is supplied, then the properties customized by the file will be used over the mergedDeployConf.
     *    As the special exception, only the {@link #defaultConfType} will be overwritten by {@link ConfigDeployment#configType} of mergedDeployConf.
     *  </p>
     *  <p>
     *      Note: the default value of {@link #isDriverMode()} is true.
     *       This means that a regular java process using the cluster class runs as driver-mode.
     *       However, {@link #deploy(List)} does NOT launch a new process for the primary unit,
     *       which is different behavior from {@link #deployAsRemoteDriver(List)}.
     *  </p>
     * @param confFile the source file of configuration parsed by {@link ClusterCommands#loadConfigFile(String)}
     * @param mergedDeployConf the deployment conf will be merged at unit creation (nullable)
     * @return loaded units
     */
    public List<ClusterUnit<AppConfType>> loadConfigFile(String confFile, ConfigDeployment mergedDeployConf) {
        boolean fromProperty = false;
        if (confFile.equals(NON_DRIVER_SYMBOL_CONF)) {
            confFile = System.getProperty(NON_DRIVER_PROPERTY_CONF, "");
            fromProperty = true;
        }
        setDriverMode(System.getProperty(NON_DRIVER_PROPERTY_FLAG, Boolean.toString(driverMode)).equals("true"));
        confFile = confFile.replaceAll("/", File.separator);
        if (fromProperty && confFile.isEmpty()) {
            return Collections.singletonList(primary(mergedDeployConf));
        } else {
            return new ClusterCommands<>(defaultConfType, mergedDeployConf).loadConfigFile(confFile);
        }
    }

    /**
     * {@link #loadConfigFile(String, ConfigDeployment)} with (confFile, null)
     * @param confFile the conf file
     * @return loaded units
     */
    public List<ClusterUnit<AppConfType>> loadConfigFile(String confFile) {
        return loadConfigFile(confFile, (ConfigDeployment) null);
    }

    /**
     * load units by {@link #loadConfigFile(String, ConfigDeployment)} and merge a config to all unit's appConfig
     * @param confFile the conf file
     * @param mergedConf the merged app conf
     * @param mergedDeployConf the deployment conf will be merged at unit creation (nullable)
     * @return loaded units
     */
    public List<ClusterUnit<AppConfType>> loadConfigFile(String confFile, AppConfType mergedConf, ConfigDeployment mergedDeployConf) {
        List<ClusterUnit<AppConfType>> units = loadConfigFile(confFile, mergedDeployConf);
        if (mergedConf != null) {
            ConfigBase base = mergedConf.createDefault();
            units.forEach(u ->
                    u.getAppConfig().mergeChangedFields(mergedConf, base));
        }
        return units;
    }

    /**
     *  {@link #loadConfigFile(String, ConfigBase, ConfigDeployment)} with (confFile, mergedConf, null)
     * @param confFile  the confFile
     * @param mergedConf the merged app conf
     * @return loaded units
     */
    public List<ClusterUnit<AppConfType>> loadConfigFile(String confFile, AppConfType mergedConf) {
        return loadConfigFile(confFile, mergedConf, null);
    }

    public String getUnitMainType(ClusterUnit<AppConfType> unit) {
        if (unit.getDeploymentConfig().primary) {
            return primaryMainType;
        } else {
            return getNodeMainType().getName();
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public final ActorSystemRemote deployUnits(ClusterCommands.ClusterUnit<AppConfType>... units) {
        try {
            return deploy(Arrays.asList(units));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     *  {@link #deployPrimary(List)}, {@link #deployNode(ClusterUnit)},
     *   and {@link #awaitNodes()}.
     * @param units units including both a primary unit and node units
     * @return the system from the primary unit
     * @throws Exception the error
     */
    public ActorSystemRemote deploy(List<ClusterCommands.ClusterUnit<AppConfType>> units) throws Exception {
        deployPrimary(units);
        deployNodes(units);
        awaitNodes();
        return getSystem();
    }

    public void deployPrimary(List<ClusterUnit<AppConfType>> units) throws Exception {
        deployPrimaryInitPrimary(units);
        deployPrimaryInitAppName();
        deployPrimaryInitSystem();
        deployPrimaryInitUncaughtHandler();
        deployFiles(primary, units.size() != 1); //if unit is primary only, it does not need to create copies
        deployPrimaryStartSystem();
        deployPrimaryAfterSystemInit(units);
    }

    protected void deployPrimaryInitPrimary(List<ClusterUnit<AppConfType>> units) {
        primary = units.stream()
                .filter(u -> u.getDeploymentConfig().primary)
                .findFirst()
                .orElseThrow(RuntimeException::new);
        if (primary == null) {
            primary = primary();
        }
    }

    protected void deployPrimaryInitAppName() {
        this.appName = getAppName();
    }

    protected void deployPrimaryInitSystem() {
        primary.log("primary %s: create system with serializer %s", primary.getDeploymentConfig().getAddress(), primary.getDeploymentConfig().kryoBuilderType);
        system = createSystem(primary.getDeploymentConfig().kryoBuilder(KryoBuilderCluster.class),
                primary.getDeploymentConfig().throttle);
        system.getLocalSystem().setLogger(new ConfigBase.SystemLoggerHeader(system.getLogger(), primary.getAppConfig()));
    }

    @SuppressWarnings("unchecked")
    public static Class<? extends KryoBuilder> getBuilderType(String buildType, Class<? extends KryoBuilder> defaultBuildType) {
        try {

            Class<?> b = buildType.isEmpty() ? defaultBuildType : Class.forName(buildType);
            if (KryoBuilder.class.isAssignableFrom(b)) {
                return (Class<? extends KryoBuilder>) b;
            } else {
                throw new RuntimeException("not a KryoBuilder: " + b);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static ActorSystemRemote createSystem(Function<ActorSystem, Kryo> kryoFactory, boolean throttle) {
        try {
            if (throttle) {
                return ActorSystemCluster.createWithKryoBuilderThrottle(kryoFactory);
            } else {
                return ActorSystemCluster.createWithKryoBuilder(kryoFactory);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    protected void deployPrimaryInitUncaughtHandler() {
        setDefaultUncaughtHandler(primary.getDeploymentConfig());
    }

    protected void deployPrimaryStartSystem() {
        ConfigDeployment conf = primary.getDeploymentConfig();
        system.startWithoutWait(ActorAddress.get(conf.getAddress()), ActorAddress.get(conf.getAddressForServerStart()));
    }

    protected void deployPrimaryAfterSystemInit(List<ClusterUnit<AppConfType>> units) throws Exception {
        deployPrimaryAfterSystemInitLogColor();
        deployPrimaryAfterSystemInitPath();
        primary.log("primary %s: path %s", primary.getDeploymentConfig().getAddress(), PathModifier.getPathModifier(system));

        deployPrimaryAfterSystemInitPlace();
        primary.log("primary %s: started %s", primary.getDeploymentConfig().getAddress(), primaryPlace);

        deployPrimaryAfterSystemInitSendConfigToUnits(units);

        deployPrimaryAfterSystemInitHttp();
    }

    protected void deployPrimaryAfterSystemInitLogColor() {
        System.setProperty("csl.actor.logColor", Integer.toString(primary.getDeploymentConfig().getLogColorDefault()));
    }

    protected void deployPrimaryAfterSystemInitPath() throws Exception {
        PathModifier.PathModifierHost ph = primary.getDeploymentConfig().setPathModifierWithBaseDir(system);
        ph.setApp(getAppName());
        System.setProperty("csl.actor.path.app", ph.getApp());
        setLogFile(primary.getDeploymentConfig(), ph);
    }

    public static void setLogFile(ConfigDeployment conf, PathModifier pm) throws Exception {
        if (conf.logFile) {
            Path p = pm.getExpanded(conf.logFilePath);
            Path parent = p.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }
            System.setErr(new LogFileWriter(System.err, p, conf.logFilePreserveColor));
        }
    }

    protected void deployPrimaryAfterSystemInitPlace() throws Exception {
        primaryPlace = createPlace(placeType, system,
                new ActorPlacement.PlacementStrategyRoundRobin(0));
        primaryPlace.setDeployment(this);
        primaryPlace.setLogger(primary.getDeploymentConfig());
    }

    protected void deployPrimaryAfterSystemInitSendConfigToUnits(List<ClusterUnit<AppConfType>> units) {
        Map<ActorAddress, AppConfType> configMap = primaryPlace.getRemoteConfig();
        units.forEach(u ->
                configMap.put(ActorAddress.get(u.getDeploymentConfig().getAddress()), u.getAppConfig()));
    }

    protected void deployPrimaryAfterSystemInitHttp() {
        int port = primary.getDeploymentConfig().httpPort;
        if (port > 0) {
            http = new ClusterHttp(this);
            String host = primary.getDeploymentConfig().httpHost;
            if (host.isEmpty()) {
                host = primary.getDeploymentConfig().host;
            }
            http.start(host, port);
        }
    }

    public void deployNodes(List<ClusterUnit<AppConfType>> units) {
        units.stream()
                .filter(u -> !u.getDeploymentConfig().primary)
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
            deployFiles(unit, true);
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
        ClusterUnit<AppConfType> primary = getPrimary();
        String appName = getAppName();
        primaryPlace.awaitsJoining(appName, primary);
    }

    protected String getJavaCommand(ClusterUnit<AppConfType> unit) throws Exception {
        return String.format(unit.getDeploymentConfig().java,
                getJavaCommandOptions(unit),
                escape(getUnitMainType(unit)),
                getJavaCommandArgs(unit));
    }

    protected String getJavaCommandOptions(ClusterUnit<AppConfType> unit) throws Exception {
        PathModifier pm = PathModifier.getPathModifier(system);
        String pathProps = "";
        if (pm instanceof PathModifier.PathModifierHost) {
            PathModifier.PathModifierHost pmh = (PathModifier.PathModifierHost) pm;
            String app = pmh.getApp();
            if (app != null) {
                pathProps += propertyOption("csl.actor.path.app", app);
            }
        }

        if (unit.getDeploymentConfig().primary) { //if the class launches a process for a primary unit, the process runs as non-driver
            pathProps += propertyOption(NON_DRIVER_PROPERTY_CONF,
                            getAppName() + "/" + NON_DRIVER_FILE_CONF) +  //suppose the working dir is baseDir
                         propertyOption(NON_DRIVER_PROPERTY_FLAG, false);
        }

        String additionalOpt = unit.getDeploymentConfig().javaVmOption;

        String propOpts = unit.getDeploymentConfig().toJvmOptions("csl.actor").stream()
                .map(this::escape)
                .collect(Collectors.joining(" "));

        return propertyOption("csl.actor.logColor", unit.getAppConfig().get("logColor"))  +
                propOpts + " " +
                propertyOption(NON_DRIVER_PROPERTY_APP_NAME, getAppName()) +
                pathProps +
                getJavaCommandOptionsClassPath(unit) + " " +
                additionalOpt;
    }

    protected String propertyOption(String name, Object value) {
        return escape("-D" + name + "=" + value) + " ";
    }

    protected String getJavaCommandArgs(ClusterUnit<AppConfType> unit) throws Exception {
        if (unit.getDeploymentConfig().primary) {
            String args = "";
            if (unit.getDeploymentConfig().configPathAsPrimaryFirstArgument) {
                args += NON_DRIVER_SYMBOL_CONF + " ";
            }
            args += primaryMainArgs.stream()
                    .map(this::escape)
                    .collect(Collectors.joining(" "));
            return args;
        } else {
            return getJavaCommandArgSelfAddress(unit) + " " +
                    primary.getDeploymentConfig().getAddress() + " " +
                    escape(getPlaceType().getName());
        }
    }

    protected String getJavaCommandArgSelfAddress(ClusterUnit<AppConfType> unit) {
        String addr = unit.getDeploymentConfig().getAddress();
        String serverAddr = unit.getDeploymentConfig().getAddressForServerStart();
        if (addr.equals(serverAddr)) {
            return addr;
        } else {
            return addr + "/" + serverAddr;
        }
    }

    protected String escape(String s) {
        StringBuilder buf = new StringBuilder();
        buf.append('\'');
        for (char c : s.toCharArray()) {
            if (c == '\'') {
                buf.append("'\"'\"'");
            } else {
                buf.append(c);
            }
        }
        buf.append('\'');
        return buf.toString();
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

    /**
     * @return {@link #primary(ConfigDeployment)} with null
     */
    public ClusterUnit<AppConfType> primary() {
        return primary(null);
    }

    /**
     * @param mergedDeployConf the deployment conf will be merged to the unit's deployment conf
     * @return a localhost primary unit: it uses the conf-type returned by {@link #getDefaultConfType()}.
     *   So if you have custom app config, it should be set by the constructor
     */
    @SuppressWarnings("unchecked")
    public ClusterUnit<AppConfType> primary(ConfigDeployment mergedDeployConf) {
        ClusterUnit<AppConfType> unit = new ClusterUnit<>();
        ConfigDeployment conf = ConfigDeployment.createAndMerge(defaultConfType, mergedDeployConf);
        conf.primary = true;
        unit.setDeploymentConfig(conf);
        unit.setAppConfig((AppConfType) conf.createAppConfig(defaultConfType));
        unit.setName("localhost");
        return unit;
    }

    public ClusterUnit<AppConfType> node(String host, int port) {
        return node(host, port, null);
    }

    @SuppressWarnings("unchecked")
    public ClusterUnit<AppConfType> node(String host, int port, ConfigDeployment mergedDeployConf) {
        ClusterUnit<AppConfType> unit = new ClusterUnit<>();
        ConfigDeployment conf = ConfigDeployment.createAndMerge(defaultConfType, mergedDeployConf);
        conf.host = host;
        conf.port = port;
        unit.setDeploymentConfig(conf);
        unit.setAppConfig((AppConfType) conf.createAppConfig(defaultConfType));
        unit.setName(host + ":" + port);
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
        if (primary != null)  {
            head = primary.getDeploymentConfig().appNameHeader;
        } else {
            head = "app";
        }
        return PathModifier.getAppName(head);
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

    public void deployFiles(ClusterUnit<?> unit, boolean needToCopyFiles) throws Exception {
        ConfigDeployment conf = unit.getDeploymentConfig();

        if (conf.primary) { //suppose the code is running under the primary
            if (driverMode && needToCopyFiles) {
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
            if (driverMode && needToCopyFiles) {
                if (conf.sharedDeploy) {
                    String primaryBase = "^" + Pattern.quote(Paths.get(primary.getDeploymentConfig().baseDir).toString());
                    String unitBase = conf.baseDir;
                    unit.setClassPathList(primary.getClassPathList().stream()
                            .map(p -> p.replaceFirst(primaryBase, unitBase))
                            .collect(Collectors.toList()));
                } else {
                    new ClusterFiles.ClusterFilesSsh(getAppName(), unit).deployFiles();
                }
            } else {
                unit.setClassPathList(primary.getClassPathList());
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
        protected ConfigDeployment configDeployment; //from properties
        protected ActorAddress.ActorAddressRemote selfAddress;
        protected ActorAddress.ActorAddressRemote selfAddressForServer;
        protected ActorSystemRemote system;
        protected ActorPlacement.ActorPlacementDefault place;
        protected String kryoBuilderType;
        protected boolean throttle = System.getProperty("csl.actor.throttle", "false").equals("true");

        public void run(String[] args) throws Exception {
            initConfigDeployment();

            String selfAddr = args[0]; //host:port[/serverHost:port]
            String joinAddr = args[1];
            String placeType = (args.length > 2 ? args[2] : "");

            kryoBuilderType = initKryoBuilderType();
            ActorAddress.ActorAddressRemote[] addrs = initSelfAddress(selfAddr);
            selfAddress = addrs[0];
            selfAddressForServer = addrs[1];
            configDeployment.log(logColor, "%s: system  with serializer %s", selfAddress, kryoBuilderType);
            system = initSystem();

            initPath();
            configDeployment.log(logColor, "%s: path %s", selfAddress, PathModifier.getPathModifier(system));

            startSystem();
            place = initPlace(placeType);

            configDeployment.log(logColor, "%s: joining to %s", place, joinAddr);
            join(joinAddr);
        }

        protected void initConfigDeployment() {
            configDeployment = new ConfigDeployment();
            configDeployment.read("csl.actor", System.getProperties());
            setDefaultUncaughtHandler(configDeployment);
        }

        protected ActorAddress.ActorAddressRemote[] initSelfAddress(String selfAddr) {
            int i = selfAddr.indexOf('/');
            if (i > 0) {
                ActorAddress.ActorAddressRemote selfAddrObj = ActorAddress.get(selfAddr.substring(0, i));
                ActorAddress.ActorAddressRemote selfAddrServObj = ActorAddress.get(selfAddr.substring(i + 1));
                configDeployment.host = selfAddrObj.getHost();
                configDeployment.port = selfAddrObj.getPort();
                configDeployment.serverHost = selfAddrServObj.getHost();
                configDeployment.serverPort = selfAddrServObj.getPort();

                return new ActorAddress.ActorAddressRemote[]{selfAddrObj, selfAddrServObj};
            } else {
                ActorAddress.ActorAddressRemote selfAddrObj = ActorAddress.get(selfAddr);
                configDeployment.host = selfAddrObj.getHost();
                configDeployment.port = selfAddrObj.getPort();

                return new ActorAddress.ActorAddressRemote[]{selfAddrObj, selfAddrObj};
            }
        }

        protected String initKryoBuilderType() {
            return System.getProperty("csl.actor.kryoBuilderType", defaultBuilderType().getName());
        }

        protected ActorSystemRemote initSystem() {
            ActorSystemRemote system = createSystem(KryoBuilder.builder(getBuilderType(kryoBuilderType, defaultBuilderType())), throttle);
            system.getLocalSystem().setLogger(new ConfigBase.SystemLoggerHeader(system.getLogger(), configDeployment));
            return system;
        }

        protected Class<? extends KryoBuilder> defaultBuilderType() {
            return KryoBuilderCluster.class;
        }

        protected void initPath() throws Exception {
            //the working directory is the baseDir
            PathModifier.PathModifierHost ph = PathModifier.setPathModifierWithBaseDir(system,
                    Paths.get(".").toAbsolutePath().normalize().toString());
            ph.setHost(selfAddress.getHost(), selfAddress.getPort());
            String appName = System.getProperty("csl.actor.path.app", "");
            ph.setApp(appName);
            ph.setLocalOnlyBaseDir(configDeployment.localOnlyBaseDir);
            ph.setAllowAbsolutePath(configDeployment.allowAbsolutePath);
            setLogFile(configDeployment, ph);
        }

        protected void startSystem() {
            system.startWithoutWait(selfAddress, selfAddressForServer);
        }

        protected ActorPlacement.ActorPlacementDefault initPlace(String placeType) throws Exception {
            ActorPlacement.ActorPlacementDefault p = createPlace(placeType, system, new ActorPlacement.PlacementStrategyUndertaker());
            p.setLogger(configDeployment);
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
                        .tell(new ConfigSet(remoteConfig));
                configSetSent.add(addr);
                completed.offer(addr);
            }
        }

        @SuppressWarnings("unchecked")
        public void receiveConfigSet(ConfigSet set) {
            remoteConfig.putAll((Map<ActorAddress, AppConfType>) set.getRemoteConfig());
        }

        public void awaitsJoining(String appName, ClusterUnit<AppConfType> primary) {
            BlockingQueue<ActorAddress> completed = getCompleted();
            Set<ActorAddress> nodeAddrs = new HashSet<>(getRemoteConfig().keySet());
            nodeAddrs.remove(ActorAddress.get(primary.getDeploymentConfig().getAddress()));
            int limit = Math.max(3, nodeAddrs.size() / getTotalThreads());
            while (!nodeAddrs.isEmpty() && limit > 0) {
                try {
                    ActorAddress joinedAddr = completed
                            .poll(primary.getDeploymentConfig().joinTimeoutMs, TimeUnit.MILLISECONDS);
                    if (joinedAddr != null) {
                        ActorAddress joinedHost = joinedAddr.getHostAddress();
                        nodeAddrs.remove(joinedHost);
                        primary.log("%s: joined: %s  remaining=%,d", joinedHost, appName, nodeAddrs.size());
                    } else {
                        primary.log("%s: waiting join: limit=%,d remaining=%,d", appName, limit, nodeAddrs.size());
                        if (nodeAddrs.size() <= 5) {
                            primary.log("  remaining nodes: %s", nodeAddrs);
                        }
                        --limit;
                    }
                } catch (Exception ex) {
                    //
                    primary.log("%s %s", appName, ex);
                    break;
                }
            }
            primary.log("%s launched %,d nodes", appName, getClusterSize());
        }

        public ActorRef move(Actor actor, ActorAddress address) {
            try {
                return place(actor, getEntry(address).getPlacementActor())
                        .get(10, TimeUnit.SECONDS);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public <ActorType extends Actor> ActorType actor(BiFunction<ActorSystem, AppConfType, ActorType> creator) {
            return creator.apply(getSystem(), getPrimaryConfig());
        }

        public AppConfType getPrimaryConfig() {
            return this.deployment.getPrimaryConfig();
        }
    }

    public static class ConfigSet implements Serializable, KryoSerializable, Message.MessageDataSpecial {
        public static final long serialVersionUID = 1L;
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

    public static class ClusterStats implements Serializable, ToJson {
        public static final long serialVersionUID = 1L;
        public String appName;
        public Class<? extends ConfigBase> defaultConfType;
        public Class<?> nodeMainType;
        public Class<? extends ActorPlacementForCluster<?>> placeType;
        public ClusterUnit<?> primary;
        public List<ClusterUnit<?>> nodes;

        public ClusterStats set(ClusterDeployment<?,?> d) {
            appName = d.getAppName();
            defaultConfType = d.getDefaultConfType();
            nodeMainType = d.getNodeMainType();
            placeType = d.getPlaceType();
            primary = d.getPrimary();
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
            json.put("primary", toJson(valueConverter, primary, null));
            json.put("nodes", toJson(valueConverter, nodes, new ArrayList<>()));
            return json;
        }
    }

    public ActorRef getPlace(ActorRef actor) {
        if (actor instanceof ActorRefRemote) {
            ActorAddress address = ((ActorRefRemote) actor).getAddress();
            ActorPlacement.AddressListEntry entry = placeGet(new PlaceGetEntry<>(address));
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

    public static class PlaceGetEntry<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, ActorPlacement.AddressListEntry>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        public ActorAddress address;

        public PlaceGetEntry() {}

        public PlaceGetEntry(ActorAddress address) {
            this.address = address;
        }

        @Override
        public ActorPlacement.AddressListEntry call(PlaceType self) {
            return self.getEntry(address);
        }
    }

    @PropertyInterface("placement-move")
    public CompletableFuture<ActorRef> move(ActorRef actor, ActorAddress targetHost) {
        return ResponsiveCalls.<ActorPlacementForCluster<AppConfType>, ActorRef>sendTask(getSystem(),
                getPlace(actor),
                new CallableMessageMove<>(actor, targetHost));
    }

    public static class CallableMessageMove<AppConfType extends ConfigBase>
            implements CallableMessage<ActorPlacementForCluster<AppConfType>, ActorRef>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        public ActorRef actor;
        public ActorAddress targetHost;

        public CallableMessageMove() {}

        public CallableMessageMove(ActorRef actor, ActorAddress targetHost) {
            this.actor = actor;
            this.targetHost = targetHost;
        }

        @Override
        public ActorRef call(ActorPlacementForCluster<AppConfType> self) {
            return self.move(actor.asLocal(), targetHost);
        }
    }

    @PropertyInterface("actor-load-and-send")
    public CompletableFuture<?> loadAndSendToActor(ActorRef target, String serializedMailboxPath) {
        return ResponsiveCalls.sendTaskConsumer(getSystem(), target,
                new CallMessageLoadAndSendToActor(serializedMailboxPath));
    }

    public static class CallMessageLoadAndSendToActor implements CallableMessage.CallableMessageConsumer<Actor> {
        public static final long serialVersionUID = -1;
        public String serializedMailboxPath;

        public CallMessageLoadAndSendToActor() {}

        public CallMessageLoadAndSendToActor(String serializedMailboxPath) {
            this.serializedMailboxPath = serializedMailboxPath;
        }

        @Override
        public void accept(Actor a) {
            //temporary manager
            //TODO the serializedMailbxPath is expanded ?
            PersistentFileManager m = PersistentFileManager.createPersistentFile(serializedMailboxPath, a.getSystem());
            MailboxPersistableReplacement.MessageOnStorage msg = new MailboxPersistableReplacement.MessageOnStorage(
                    new PersistentFileManager.PersistentFileReaderSource(serializedMailboxPath, 0, m), 0);
            synchronized (msg) {
                Message<?> next = msg.readNext();
                while (next != null) {
                    a.tell(next.getData());
                    next = msg.readNext();
                }
            }
        }
    }

    @PropertyInterface("placement-shutdown")
    public CompletableFuture<?> shutdown(ActorAddress targetHost) {
        return ResponsiveCalls.<ActorPlacement.ActorPlacementDefault, String>sendTask(getSystem(),
                placeGet(new PlaceGetEntry<>(targetHost)).getPlacementActor().ref(getSystem()),
                new ShutdownTask());
    }

    public static class ShutdownTask implements CallableMessage<ActorPlacement.ActorPlacementDefault, String>, Message.MessageDataSpecial {
        public static final long serialVersionUID = 1L;
        @Override
        public String call(ActorPlacement.ActorPlacementDefault self) {
            self.close();
            new Thread() { public void run() { //the shutting down thread
                try {
                    Thread.sleep(3_000);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                self.getSystem().getLogger().log("close system %s", self.getSystem());
                self.getSystem().close();
            }}.start();
            return "shutdown: " + self.toString();
//            try {
//                self.getSystem().awaitClose(6_000, TimeUnit.MILLISECONDS);
//            } catch (Exception ex) {
//                throw new RuntimeException(ex);
//            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }

    public CompletableFuture<?> shutdownCluster() {
        List<CompletableFuture<?>> tasks = new ArrayList<>(primaryPlace.getClusterSize());
        for (ActorPlacement.AddressListEntry e : primaryPlace.getCluster()) {
            tasks.add(shutdown(e.getPlacementActor()));
        }
        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0]));
    }

    public void destroyClusterProcesses() {
        getProcesses().forEach((unit, proc) -> {
            if (proc.isAlive()) {
                proc.destroy();
                unit.log("%s : destroy process: %s", unit, proc);
            }
        });
    }

    @PropertyInterface("cluster-shutdown-all")
    public void shutdownAll() {
        getPrimary().log("shutdownAll");
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

    /**
     * do {@link #shutdownAll()}
     */
    public void close() {
        shutdownAll();
    }

    /////////

    @PropertyInterface("placement")
    public ActorRef getPlace() {
        if (primaryPlace != null) {
            return getPrimaryPlace();
        } else {
            return attachedPlace;
        }
    }

    @PropertyInterface("placement-config-primary")
    public AppConfType getPrimaryConfig() {
        if (primary != null) {
            return primary.getAppConfig();
        } else {
            return placeGet(new PlaceGetRemoteConfig<>());
        }
    }

    public static class PlaceGetRemoteConfig<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, AppConfType>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        @Override
        public AppConfType call(PlaceType self) {
            return self.getRemoteConfig().get(self.getSelfAddress().getHostAddress());
        }
    }

    public ActorRef getAttachedPlace() {
        return attachedPlace;
    }

    public void setAttachKryoBuilderType(String attachKryoBuilderType) {
        this.attachKryoBuilderType = attachKryoBuilderType;
    }

    public String getAttachKryoBuilderType() {
        return attachKryoBuilderType;
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
                    attachInitRun(new AttachInitRunGetAppName<>(), r -> this.appName = r),
                    attachInitRun(new AttachInitRunGetPrimary<>(), r -> primary = r),
                    attachInitRun(new AttachInitRunGetNodes<>(), r -> nodes = r)
                ).get(30, TimeUnit.SECONDS);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class AttachInitRunGetAppName<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
        implements CallableMessage<PlaceType, String>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        @Override
        public String call(PlaceType self) {
            return self.getDeployment().getAppName();
        }
    }
    public static class AttachInitRunGetPrimary<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, ClusterUnit<AppConfType>>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        @Override
        public ClusterUnit<AppConfType> call(PlaceType self) {
            return self.getDeployment().getPrimary();
        }
    }
    public static class AttachInitRunGetNodes<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, List<ClusterUnit<AppConfType>>>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        @Override
        public List<ClusterUnit<AppConfType>> call(PlaceType self) {
            return self.getDeployment().getNodes();
        }
    }


    protected void attachInitSystem() {
        system = createAttachInitSystem();
        int port;
        try (ServerSocket sock = new ServerSocket(0)) { //obtain a dynamic port
            port = sock.getLocalPort();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        system.startWithoutWait(port);
    }

    protected ActorSystemRemote createAttachInitSystem() {
        return createSystem(KryoBuilder.builder(getBuilderType(attachKryoBuilderType, KryoBuilderCluster.class)), false);
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
            addr = this.<ActorAddress.ActorAddressRemoteActor>placeGet(new PlaceGetEntryPlacementActor<>(host));
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

    public static class PlaceGetEntryPlacementActor<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, ActorAddress.ActorAddressRemoteActor>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        public ActorAddress.ActorAddressRemote host;

        public PlaceGetEntryPlacementActor() {}

        public PlaceGetEntryPlacementActor(ActorAddress.ActorAddressRemote host) {
            this.host = host;
        }

        @Override
        public ActorAddress.ActorAddressRemoteActor call(PlaceType self) {
            return self.getEntry(host).getPlacementActor();
        }
    }

    //////

    public interface MessageDataStats extends Message.MessageDataSpecial {
    }

    @PropertyInterface("system")
    public SystemStats getAttachedSystemStats() {
        return placeGet(new PlaceGetAttachedSystemStats<>());
    }

    @PropertyInterface("system-actors")
    public Map<String, ActorRef> getAttachedSystemNamedActorMap() {
        return placeGet(new PlaceGetAttachedSystemNamedActorMap<>());
    }

    @PropertyInterface("system-process-count")
    public int getAttachedSystemProcessingCount() {
        return placeGet(new PlaceGetAttachedSystemProcessingCount<>());
    }

    @PropertyInterface("system-connections")
    public Map<ActorAddress, NetworkStats> getAttachedSystemRemoteConnectionMap() {
        return placeGet(new PlaceGetAttachedSystemRemoteConnectionMap<>());
    }

    @PropertyInterface("system-server-receive")
    public NetworkStats getAttachedSystemRemoteServerReceive() {
        return placeGet(new PlaceGetAttachedSystemRemoteServerReceive<>());
    }

    @PropertyInterface("placement-total-threads")
    public int getAttachedPlacementTotalThreads() {
        return placeGet(new PlaceGetAttachedPlacementTotalThreads<>());
    }

    @PropertyInterface("placement-clusters")
    public List<ActorPlacement.AddressListEntry> getAttachedPlacementCluster() {
        return placeGet(new PlaceGetAttachedPlacementCluster<>());
    }

    @PropertyInterface("placement-clusters-with-self")
    public List<ActorPlacement.AddressListEntry> getAttachedPlacementClusterWithSelf() {
        return placeGet(new PlaceGetAttachedPlacementClusterWithSelf<>());
    }

    @PropertyInterface("placement-created-actors")
    public long getAttachedPlacementCreatedActors() {
        return placeGet(new PlaceGetAttachedPlacementCreatedActors<>());
    }

    @PropertyInterface("placement-remote-config")
    public Map<ActorAddress, ? extends ConfigBase> getAttachedPlacementForClusterRemoteConfig() {
        return placeGet(new PlaceGetAttachedPlacementForClusterRemoteConfig<>());
    }
    public static class PlaceGetAttachedSystemStats<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, SystemStats>, MessageDataStats {
        public static final long serialVersionUID = -1;
        @Override
        public SystemStats call(PlaceType self) {
            return new SystemStats().set(self);
        }
    }
    public static class PlaceGetAttachedSystemNamedActorMap<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, Map<String,ActorRef>>, MessageDataStats {
        public static final long serialVersionUID = -1;
        @Override
        public Map<String,ActorRef> call(PlaceType self) {
            return toRefMap(((ActorSystemRemote) self.getSystem()).getLocalSystem().getNamedActorMap());
        }
    }
    public static class PlaceGetAttachedSystemProcessingCount<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, Integer>, MessageDataStats {
        public static final long serialVersionUID = -1;
        @Override
        public Integer call(PlaceType self) {
            return ((ActorSystemRemote) self.getSystem()).getLocalSystem().getProcessingCount().get();
        }
    }
    public static class PlaceGetAttachedSystemRemoteConnectionMap<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, Map<ActorAddress, NetworkStats>>, MessageDataStats {
        public static final long serialVersionUID = -1;
        @Override
        public Map<ActorAddress, NetworkStats> call(PlaceType self) {
            return ((ActorSystemRemote) self.getSystem()).getConnectionMap().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> new NetworkStats().send(e.getValue())));
        }
    }
    public static class PlaceGetAttachedSystemRemoteServerReceive<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, NetworkStats>, MessageDataStats {
        public static final long serialVersionUID = -1;
        @Override
        public NetworkStats call(PlaceType self) {
            return new NetworkStats().receive((ActorSystemRemote) self.getSystem());
        }
    }
    public static class PlaceGetAttachedPlacementTotalThreads<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, Integer>, MessageDataStats {
        public static final long serialVersionUID = -1;
        @Override
        public Integer call(PlaceType self) {
            return self.getTotalThreads();
        }
    }
    public static class PlaceGetAttachedPlacementCluster<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, List<ActorPlacement.AddressListEntry>>, MessageDataStats {
        public static final long serialVersionUID = -1;
        @Override
        public List<ActorPlacement.AddressListEntry> call(PlaceType self) {
            return self.getCluster();
        }
    }
    public static class PlaceGetAttachedPlacementClusterWithSelf<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, List<ActorPlacement.AddressListEntry>>, MessageDataStats {
        public static final long serialVersionUID = -1;
        @Override
        public List<ActorPlacement.AddressListEntry> call(PlaceType self) {
            return self.getClusterWithSelf();
        }
    }
    public static class PlaceGetAttachedPlacementCreatedActors<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, Long>, MessageDataStats {
        public static final long serialVersionUID = -1;
        @Override
        public Long call(PlaceType self) {
            return self.getCreatedActors();
        }
    }
    public static class PlaceGetAttachedPlacementForClusterRemoteConfig<AppConfType extends ConfigBase,
            PlaceType extends ClusterDeployment.ActorPlacementForCluster<AppConfType>>
            implements CallableMessage<PlaceType, Map<ActorAddress, ? extends ConfigBase>>, MessageDataStats {
        public static final long serialVersionUID = -1;
        @Override
        public Map<ActorAddress, ? extends ConfigBase> call(PlaceType self) {
            return self.getRemoteConfig();
        }
    }

    public static Map<String, ActorRef> toRefMap(Map<String, Actor> map) {
        return map.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static class SystemStats implements Serializable, ToJson {
        public static final long serialVersionUID = 1L;
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

    public static class NetworkStats implements Serializable, ToJson {
        public static final long serialVersionUID = 1L;
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
        return placeGet(host, new PlaceGetAttachedSystemStats<>());
    }

    @PropertyInterface("system-actors")
    public Map<String, ActorRef> getAttachedSystemNamedActorMap(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, new PlaceGetAttachedSystemNamedActorMap<>());
    }

    @PropertyInterface("system-process-count")
    public int getAttachedSystemProcessingCount(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, new PlaceGetAttachedSystemProcessingCount<>());
    }

    @PropertyInterface("system-connections")
    public Map<ActorAddress, NetworkStats> getAttachedSystemRemoteConnectionMap(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, new PlaceGetAttachedSystemRemoteConnectionMap<>());
    }

    @PropertyInterface("system-server-receive")
    public NetworkStats getAttachedSystemRemoteServerReceive(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, new PlaceGetAttachedSystemRemoteServerReceive<>());
    }

    @PropertyInterface("placement-total-threads")
    public int getAttachedPlacementTotalThreads(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, new PlaceGetAttachedPlacementTotalThreads<>());
    }

    @PropertyInterface("placement-clusters")
    public List<ActorPlacement.AddressListEntry> getAttachedPlacementCluster(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, new PlaceGetAttachedPlacementCluster<>());
    }

    @PropertyInterface("placement-clusters-with-self")
    public List<ActorPlacement.AddressListEntry> getAttachedPlacementClusterWithSelf(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, new PlaceGetAttachedPlacementClusterWithSelf<>());
    }

    @PropertyInterface("placement-created-actors")
    public long getAttachedPlacementCreatedActors(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, new PlaceGetAttachedPlacementCreatedActors<>());
    }

    @PropertyInterface("placement-remote-config")
    public Map<ActorAddress, ? extends ConfigBase> getAttachedPlacementForClusterRemoteConfig(ActorAddress.ActorAddressRemote host) {
        return placeGet(host, new PlaceGetAttachedPlacementForClusterRemoteConfig<>());
    }
}
