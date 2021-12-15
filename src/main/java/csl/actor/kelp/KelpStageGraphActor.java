package csl.actor.kelp;

import csl.actor.*;
import csl.actor.cluster.ActorPlacement;
import csl.actor.kelp.behavior.ActorKelpStats;
import csl.actor.kelp.shuffle.ActorRefShuffle;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.util.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KelpStageGraphActor extends ActorDefault {
    protected UUID id;
    protected ActorSystem.SystemLogger logger;
    protected List<GraphActorNode> startNodes;
    protected Map<String, GraphStageNode> nameToStageNode;
    protected Map<String, GraphActorNode> nameToActorNode;
    protected List<GraphActorNode> actorNodes;
    protected List<GraphStageNode> stageNodes;

    protected List<ActorRef> startRefs;

    protected long buildRemaining;
    protected Set<String> buildStageNames;
    protected volatile CompletableFuture<Instant> buildTask;

    protected volatile StageMonitor monitor;

    protected BiConsumer<KelpStageGraphActor, GraphStageNode> stageExitHandler;
    protected BiConsumer<KelpStageGraphActor, GraphStageNode> stageEnterHandler;
    protected BiConsumer<KelpStageGraphActor, GraphStageNode> stageEndHandler;

    protected boolean logStageEnterExit;
    protected boolean logStageEnd;
    protected LogStatusPeriodic logPeriodic;

    public static KelpStageGraphActor get(ActorSystem system, ActorRef... firstNode) throws Exception {
        return get(system, Arrays.asList(firstNode));
    }

    public static KelpStageGraphActor get(ActorSystem system, Iterable<? extends ActorRef> firstNode) throws Exception {
        KelpStageGraphActor g = new KelpStageGraphActor(system);
        g.build(firstNode).get();
        return g;
    }

    public KelpStageGraphActor(ActorSystem system) {
        super(system);
        logger = system.getLogger();
        this.id = UUID.randomUUID();
        this.name = createName();
        this.nameToStageNode = new HashMap<>();
        this.nameToActorNode = new HashMap<>();
        this.buildStageNames = new HashSet<>();
        this.startNodes = new ArrayList<>();
        this.logStageEnterExit = false;
        this.logStageEnd = false;
        this.logPeriodic = new LogStatusPeriodic(this);
        system.register(this);
    }

    public KelpStageGraphActor(KelpStageGraphActor src) {
        super(src.getSystem());
        logger = src.logger;
        this.id = UUID.randomUUID();
        this.name = createName();
        this.nameToStageNode = new HashMap<>(src.nameToStageNode);
        this.nameToActorNode = new HashMap<>(src.nameToActorNode);
        this.buildStageNames = new HashSet<>(src.buildStageNames);
        this.startNodes = new ArrayList<>(src.startNodes);
        this.actorNodes = new ArrayList<>(src.actorNodes);
        this.stageNodes = new ArrayList<>(src.stageNodes);
        this.stageExitHandler = src.stageExitHandler;
        this.stageEnterHandler = src.stageEnterHandler;
        this.stageEndHandler = src.stageEndHandler;
        this.logStageEnterExit = src.logStageEnterExit;
        this.logStageEnd = src.logStageEnd;
        if (src.logPeriodic != null) {
            this.logPeriodic = src.logPeriodic.copy(this);
        }
        system.register(this);
    }

    public String createName() {
        return getClass().getName() + NAME_ID_SEPARATOR + id.toString();
    }

    public static class GraphStageNode {
        protected String name;
        protected int index;
        protected int enters;
        protected int enterIdStart;
        protected List<GraphActorNode> actorNodes = new ArrayList<>();
        protected List<GraphStageNode> prevNodes = new ArrayList<>();
        protected List<GraphStageNode> nextNodes = new ArrayList<>();

        public GraphStageNode(String name) {
            this.name = name;
        }

        public void addActorNode(GraphActorNode a) {
            if (!actorNodes.contains(a)) {
                actorNodes.add(a);
                a.setStageNode(this);
            }
        }

        public void addNext(GraphStageNode next) {
            if (!nextNodes.contains(next)) {
                nextNodes.add(next);
                next.prevNodes.add(this);
            }
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }

        public void setEnters(int enters) {
            this.enters = enters;
        }

        public int getEnters() {
            return enters;
        }

        public void setEnterIdStart(int enterIdStart) {
            this.enterIdStart = enterIdStart;
        }

        public int getEnterIdStart() {
            return enterIdStart;
        }

        public IntStream getEnterIds() {
            return IntStream.range(enterIdStart, enterIdStart + enters);
        }

        public String getName() {
            return name;
        }

        public List<GraphStageNode> getPrevNodes() {
            return prevNodes;
        }

        public List<GraphStageNode> getNextNodes() {
            return nextNodes;
        }

        public List<GraphActorNode> getActorNodes() {
            return actorNodes;
        }

        public List<ActorRef> getActors() {
            return actorNodes.stream()
                    .map(GraphActorNode::getActor)
                    .collect(Collectors.toList());
        }
        public List<Integer> getActorIndices() {
            return actorNodes.stream()
                    .map(GraphActorNode::getIndex)
                    .collect(Collectors.toList());
        }
    }

    public static class GraphActorNode {
        protected String name;
        protected int index;
        protected ActorRef actor;
        protected GraphStageNode stageNode;
        protected List<GraphActorNode> prevNodes = new ArrayList<>();
        protected List<GraphActorNode> nextNodes = new ArrayList<>();

        public GraphActorNode(String name, ActorRef actor) {
            this.name = name;
            this.actor = actor;
        }

        public void addNext(GraphActorNode next) {
            if (!nextNodes.contains(next)) {
                next.prevNodes.add(this);
                nextNodes.add(next);
            }
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }

        public void setStageNode(GraphStageNode stageNode) {
            this.stageNode = stageNode;
        }

        public GraphStageNode getStageNode() {
            return stageNode;
        }

        public List<GraphActorNode> getPrevNodes() {
            return prevNodes;
        }

        public List<GraphActorNode> getNextNodes() {
            return nextNodes;
        }

        public String getName() {
            return name;
        }

        public ActorRef getActor() {
            return actor;
        }

    }


    @Override
    protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilder builder) {
        return builder
                .match(GraphBuilding.class, this::buildAdd);
    }

    /**
     * creating a copy of the constructed graph, which can starts another checking task.
     * Note: node instances are immutable and shared.
     * @return copy of the graph
     */
    public KelpStageGraphActor copy() {
        return new KelpStageGraphActor(this);
    }

    /////// building

    public CompletableFuture<Instant> build(Iterable<? extends ActorRef> startActor) {
        buildTask = new CompletableFuture<>();
        buildStageNames.clear();
        List<ActorRef> refs = new ArrayList<>();
        startActor.forEach(refs::add);
        buildStart(refs);
        return buildTask;
    }

    protected void buildStart(List<ActorRef> refs) {
        Map<String, List<ActorRef>> nameToMemTotal = new HashMap<>();
        this.startRefs = new ArrayList<>(refs);
        for (ActorRef ref: refs) {
            if (ref instanceof Staging.StagingPoint) {
                ((Staging.StagingPoint) ref).getStageNameToMemberActors().forEach((name, list) ->
                        nameToMemTotal.computeIfAbsent(name, _n -> new ArrayList<>())
                                .addAll(list));
            } else if (ref instanceof Staging.StagingNonSubject) {
                String name = Staging.name(ref);
                List<? extends ActorRef> sa = ((Staging.StagingNonSubject) ref).getStagingSubjectActors();
                nameToMemTotal.computeIfAbsent(name, _n -> new ArrayList<>())
                        .addAll(sa);
            } else {
                String name = Staging.name(ref);
                nameToMemTotal.computeIfAbsent(name, _n -> new ArrayList<>())
                        .add(ref);
            }
        }
        buildRemaining += nameToMemTotal.values().stream()
                .mapToInt(List::size)
                .sum();
        nameToMemTotal.forEach((name, list) ->
                list.forEach(e -> e.tell(new GraphBuilding(this, name).withSender(this))));
    }

    public static class GraphBuilding implements CallableMessage.CallableMessageConsumer<Actor>, Serializable, Message.MessageDataSpecial {
        public static final long serialVersionUID = 1L;
        public ActorRef graph;

        public ActorRef from;
        public String fromName;
        public String fromStageName;

        public ActorRef to;
        public String toName;
        public String toStageName;

        public Map<String,List<ActorRef>> toNextStageNodes;

        public static Map<Actor, String> actorProcessUniqueName = new WeakHashMap<>();

        public GraphBuilding() {}

        public GraphBuilding(ActorRef graph, String toStageName) {
            this.graph = graph;
            this.toStageName = toStageName;
        }

        public GraphBuilding(ActorRef graph,
                             ActorRef from, String fromName, String fromStageName,
                             ActorRef to, String toName, String toStageName,
                             Map<String,List<ActorRef>> toNextStageNodes) {
            this.graph = graph;
            this.from = from;
            this.fromName = fromName;
            this.fromStageName = fromStageName;
            this.to = to;
            this.toName = toName;
            this.toStageName = toStageName;
            this.toNextStageNodes = toNextStageNodes;
        }

        @Override
        public void accept(Actor to) { //for node
            String toName = to.getName();
            if (toName == null) {
                toName = getUniqueName(to);
            }
            String toStageName = (this.toStageName == null ? toName : this.toStageName);
            Map<String,List<ActorRef>> toNexts = new HashMap<>();
            if (to instanceof Staging.StagingSupported) {
                Staging.StagingSupported stg = (Staging.StagingSupported) to;
                ActorRef nextStage = stg.nextStageActor();
                if (nextStage instanceof Staging.StagingPoint) {
                    toNexts = ((Staging.StagingPoint) nextStage).getStageNameToMemberActors();
                } else if (nextStage instanceof Staging.StagingNonSubject) {
                    String nextName = Staging.name(nextStage);
                    toNexts = new HashMap<>();
                    toNexts.put(nextName, new ArrayList<>(((Staging.StagingNonSubject) nextStage).getStagingSubjectActors()));
                } else {
                    List<ActorRef> toNextNodes = new ArrayList<>();
                    stg.nextStageActors()
                            .forEach(toNextNodes::add);
                    toNexts.put(Staging.name(nextStage), toNextNodes);
                }
            }
            graph.tell(new GraphBuilding(graph, from, fromName ,fromStageName, to, toName, toStageName,
                    toNexts));
        }

        public String getUniqueName(Actor to) {
            synchronized (GraphBuilding.class) {
                return actorProcessUniqueName.computeIfAbsent(to,
                        _t -> Staging.stageNameArray(_t.getClass().getSimpleName()));
            }
        }

        public GraphBuilding createToNext(String toNextStageName) {
            return new GraphBuilding(graph,
                    to, toName, toStageName,
                    null, null, toNextStageName, null);
        }
    }

    public void buildAdd(GraphBuilding add) {
        buildRemaining--;
        boolean toAlreadyVisited = nameToActorNode.containsKey(add.toName);

        GraphActorNode to = getActorNode(add.toName, add.to);
        GraphStageNode toStage = getStageNode(add.toStageName);
        toStage.addActorNode(to);

        if (add.from == null) { //starting
            startNodes.add(to);
        } else {
            getActorNode(add.fromName, add.from).addNext(to);
            getStageNode(add.fromStageName).addNext(toStage);
        }
        if (!toAlreadyVisited) {
            for (Map.Entry<String, List<ActorRef>> toNextStage : add.toNextStageNodes.entrySet()) {
                buildRemaining += toNextStage.getValue().size();
                GraphBuilding toNextMsg = add.createToNext(toNextStage.getKey());
                toNextStage.getValue().forEach(a -> a.tell(toNextMsg.withSender(this)));
            }
        }
        if (buildRemaining <= 0) {
            buildFinish();
        }
    }
    public void buildFinish() {
        LinkedList<GraphActorNode> remain = new LinkedList<>(getStartNodes());
        Set<GraphActorNode> visited = new HashSet<>();
        actorNodes = new ArrayList<>(nameToActorNode.size());
        int i = 0;
        while (!remain.isEmpty()) {
            GraphActorNode next = remain.removeFirst();
            visited.add(next);

            next.setIndex(i);
            actorNodes.add(next);
            ++i;

            next.getNextNodes().stream()
                    .filter(n -> !remain.contains(n) && !visited.contains(n))
                    .forEach(remain::add);
        }
        buildFinishStage();
        buildTask.complete(Instant.now());
    }

    public void buildFinishStage() {
        LinkedList<GraphStageNode> remain = new LinkedList<>(getStartStageNodes());
        Set<GraphStageNode> visited = new HashSet<>();
        stageNodes = new ArrayList<>(nameToStageNode.size());
        int i = 0;
        while (!remain.isEmpty()) {
            GraphStageNode next = remain.removeFirst();
            visited.add(next);

            next.setIndex(i);
            stageNodes.add(next);
            ++i;

            next.getNextNodes().stream()
                    .filter(n -> !remain.contains(n) && !visited.contains(n))
                    .forEach(remain::add);
        }

        stageNodes.stream()
                    .filter(n -> n.getNextNodes().isEmpty())
                    .forEach(this::countBackStageEnters);
        int ei = 1;
        for (GraphStageNode n : stageNodes) {
            n.setEnterIdStart(ei);
            ei += n.getEnters();
        }
    }

    protected int countBackStageEnters(GraphStageNode n) {
        if (n.getEnters() != 0) {
            return n.getEnters();
        } else {
            int c;
            if (n.getPrevNodes().isEmpty()) {
                c = 1;
            } else {
                c = n.getPrevNodes().stream()
                        .mapToInt(this::countBackStageEnters)
                        .sum();
            }
            n.setEnters(c);
            return c;
        }
    }

    public GraphActorNode getActorNode(String name, ActorRef actor) {
        return nameToActorNode.computeIfAbsent(name, _n -> new GraphActorNode(name, actor));
    }

    public GraphStageNode getStageNode(String name) {
        return nameToStageNode.computeIfAbsent(name, GraphStageNode::new);
    }

    public List<GraphActorNode> getActorNodes() {
        return actorNodes;
    }

    public List<GraphActorNode> getStartNodes() {
        return startNodes;
    }

    public List<GraphStageNode> getStartStageNodes() {
        return getStartNodes().stream()
                .map(GraphActorNode::getStageNode)
                .distinct()
                .collect(Collectors.toList());
    }

    public List<GraphStageNode> getStageNodes() {
        return stageNodes;
    }

    public List<ActorRef> getStartRefs() {
        return startRefs;
    }

    ////////////// checking

    public KelpStageGraphActor start() {
        return start(Instant.now());
    }

    public KelpStageGraphActor start(Instant startTime) {
        if (monitor != null) {
            //TODO error ?
        }
        monitor = new StageMonitor(this, startTime);
        monitor.start();
        return this;
    }

    public CompletableFuture<KelpStageGraphActor> await() {
        monitor.startCompletion();//start in actor-thread: CompletedTask.accept -> checkStageNext(null)
        return monitor.getStageFuture()
                .thenApply(e -> this);
    }

    public CompletableFuture<KelpStageGraphActor> await(Runnable task) {
        task.run();
        return await();
    }

    public static class StageMonitor {
        protected KelpStageGraphActor graph;
        protected AtomicReference<Instant> startTime = new AtomicReference<>(Instant.EPOCH);
        protected volatile CompletableFuture<Instant> stageFuture;
        protected Set<StageStatusKey> stageCurrent;
        protected Map<String, Iterator<Integer>> stageEnterId;
        protected Map<StageStatusKey, StageStatusI> stageStatusMap;
        protected LogStatusPeriodic logPeriodic;

        protected Map<StageEndBackTask, GraphActorNode> remainingStageEndTasks;

        public StageMonitor(KelpStageGraphActor graph, Instant startTime) {
            CompletableFuture<Instant> cf = new CompletableFuture<>();
            this.startTime.set(startTime);
            stageFuture = cf;
            stageEnterId = new ConcurrentHashMap<>(graph.getStageNodes().size());
            stageCurrent = Collections.synchronizedSet(new HashSet<>());
            stageStatusMap = new ConcurrentHashMap<>();
            remainingStageEndTasks = new ConcurrentHashMap<>();
            this.graph = graph;
        }

        public CompletableFuture<Instant> getStageFuture() {
            return stageFuture;
        }

        public void start() {
            LogStatusPeriodic log = graph.logPeriodic;
            if (log != null) {
                logPeriodic = log.copy(graph);
                logPeriodic.submitNext();
            }
            registerToPlacement(true);
        }

        public void startCompletion() {
            graph.tell(new CompletedTask(null, -1).withSender(graph));
        }

        protected void registerToPlacement(boolean register) {
            ActorPlacementKelp<?> p = ActorPlacementKelp.getPlacement(graph.getSystem());
            if (p != null) {
                p.sendMessageConsumerToCluster(register ?
                        new ActorPlacementKelp.StageGraphAdd<>(graph) :
                        new ActorPlacementKelp.StageGraphRemove<>(graph));
            }
        }

        public void checkStageNext(StageStatusKey finishedKey) {
            List<StageStatusKey> nexts;
            if (finishedKey == null) {
                nexts = graph.getStartStageNodes().stream()
                        .flatMap(s -> s.getEnterIds().mapToObj(i -> new StageStatusKey(s.getName(), i)))
                        .collect(Collectors.toList());
                graph.getStartStageNodes()
                        .forEach(s -> stageEnterId.put(s.getName(), Collections.emptyIterator()));
            } else {
                stageCurrent.remove(finishedKey);
                GraphStageNode exitStage = graph.getStageNode(finishedKey.stageName);
                boolean finishStage = !stageEnterId.get(finishedKey.stageName).hasNext();
                graph.callBackExitStage(finishedKey, exitStage, finishStage);
                if (finishStage) {
                    exitStage.getActorNodes().forEach(n -> sendStageEndTask(n, finishedKey));
                }
                nexts = exitStage.getNextNodes().stream()
                        .map(this::nextKey)
                        .collect(Collectors.toList());
            }
            stageCurrent.addAll(nexts);
            nexts.forEach(k ->
                    checkStage(graph.getStageNode(k.stageName), k)); //TODO how to handle loop?

            if (remainingStageEndTasks.isEmpty() && stageCurrent.isEmpty()) {
                checkStatusCompleteAll(finishedKey);
            }
        }

        protected void checkStatusCompleteAll(StageStatusKey finishedKey) {
            StageStatusI s = stageStatusMap.get(finishedKey);
            Instant time = (s == null ? null : s.getFinishedTime());
            Instant finishTime = time == null ? Instant.now() : time;
            graph.callbackFinish(finishedKey, finishTime);
            stageFuture.complete(finishTime);

            if (logPeriodic != null) {
                logPeriodic.close();
            }
            registerToPlacement(false);
        }


        protected StageStatusKey nextKey(GraphStageNode n) {
            Iterator<Integer> i = stageEnterId.computeIfAbsent(n.getName(), nm -> n.getEnterIds().iterator());
            if (i.hasNext()) {
                return new StageStatusKey(n.getName(), i.next());
            } else {
                //TODO error?
                stageEnterId.remove(n.getName()); //clear
                return nextKey(n);
            }
        }


        public boolean checkStageStatusCompleted(StageStatusKey key, int actorNodeIndex) {
            StageStatusI s = stageStatusMap.get(key);
            if (s instanceof StageStatus && ((StageStatus) s).finishIndex(actorNodeIndex)) { //all finish
                checkStageNext(key);
                return true;
            } else {
                return false;
            }
        }

        protected void checkStage(GraphStageNode stage, StageStatusKey key) {
            graph.callBackEnterStage(key, stage);
            stageStatusMap.put(key, new StageStatus(key));
            stage.getActorNodes()
                    .forEach(n -> checkStageActor(key, n));
        }

        protected void checkStageActor(StageStatusKey key, GraphActorNode node) {
            ActorRef a = node.getActor();
            //Map<ActorAddress.ActorAddressRemote, Integer> clockTable = collectClockTable(graph.getSystem(), a, false);
            ((StageStatus) stageStatusMap.computeIfAbsent(key, StageStatus::new))
                    .addIndex(node.getIndex());

            a.tell(graph.createWatchTask(key, node.getIndex(), null).withSender(graph));
        }

        public synchronized StageStatusI getStageStatusImplAll() {
            return new StageStatusMulti(null,
                    graph.getStageNodes().stream()
                            .flatMap(n -> getStageStatusList(n.getName()).stream())
                            .collect(Collectors.toList()));
        }

        public StageStatusI getStageStatusImplCurrent() {
            return new StageStatusMulti(null,
                    new HashSet<>(stageCurrent).stream()
                            .map(stageStatusMap::get)
                            .collect(Collectors.toList()));
        }

        public StageStatusI getStageStatusImplStage(String stageName) {
            return new StageStatusMulti(stageName,
                    getStageStatusList(stageName));
        }


        protected List<StageStatusI> getStageStatusList(String stageName) {
            return graph.getStageNode(stageName).getEnterIds()
                    .mapToObj(i -> new StageStatusKey(stageName, i))
                    .map(this::getStageStatusCurrentOrEmpty)
                    .collect(Collectors.toList());
        }

        protected StageStatusI getStageStatusCurrentOrEmpty(StageStatusKey key) {
            StageStatusI s = stageStatusMap.get(key);
            if (s != null) {
                return s.copy();
            } else {
                return new StageStatus(key, graph.getStageNode(key.stageName).getActorIndices());
            }
        }

        public Instant getStageStartTime() {
            return startTime.get();
        }

        public Instant getStageEndTime() {
            try {
                if (getStageFuture().isDone()) {
                    return getStageFuture().get();
                } else {
                    return null;
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public Duration getStageElapsedTime() {
            return Duration.between(getStageStartTime(), getStageEndTime());
        }


        public void sendStageEndTask(GraphActorNode node, StageStatusKey key) {
            remainingStageEndTasks.put(new StageEndBackTask(key, node.getIndex()), node);
            node.getActor().tell(new StageEndTask(key, node.getIndex(), graph).withSender(graph));
        }

        public void receiveStageEndBackTask(StageEndBackTask task) {
            remainingStageEndTasks.remove(task);
            if (remainingStageEndTasks.isEmpty() && stageCurrent.isEmpty()) {
                checkStatusCompleteAll(task.key);
            }
        }
    }

    public WatchTask createWatchTask(StageStatusKey key, int actorNodeIndex,
                                     Map<ActorAddress.ActorAddressRemote, Integer> clockTable) {
        return new WatchTask(key, actorNodeIndex, this, clockTable);
    }

    public void callBackExitStage(StageStatusKey finishedKey, GraphStageNode exitStage, boolean finishStage) {
        if (logStageEnterExit) {
            logStatusAll(" exit stage: %s", finishedKey.format());
        }
        if (stageExitHandler != null) {
            stageExitHandler.accept(this, exitStage);
        }
        if (finishStage && logStageEnd) {
            logStatusAll("  end stage: %s", finishedKey.format());
        }
        if (finishStage && stageEndHandler != null) {
            stageEndHandler.accept(this, exitStage);
        }
    }

    public void callBackEnterStage(StageStatusKey key, GraphStageNode stage) {
        if (logStageEnterExit) {
            logStatusAll("enter stage: %s", key.format());
        }
        if (stageEnterHandler != null) {
            stageEnterHandler.accept(this, stage);
        }
    }

    public void callbackFinish(StageStatusKey lastKey, Instant finishTime) {
    }

    public static class StageEndTask implements CallableMessage.CallableMessageConsumer<Actor> { //non-special
        public static final long serialVersionUID = -1;
        public StageStatusKey key;
        public int actorIndex;
        public ActorRef graph;
        public StageEndTask() {}

        public StageEndTask(StageStatusKey key, int actorIndex, ActorRef graph) {
            this.key = key;
            this.graph = graph;
            this.actorIndex = actorIndex;
        }

        @Override
        public void accept(Actor self) {
            try {
                if (self instanceof Staging.StagingSupported) {
                    ((Staging.StagingSupported) self).stageEnd();
                }
            } finally {
                graph.tell(new StageEndBackTask(key, actorIndex).withSender(self));
            }
        }
    }

    public static class StageEndBackTask implements CallableMessage.CallableMessageConsumer<KelpStageGraphActor>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        public StageStatusKey key;
        public int actorIndex;

        public StageEndBackTask() {}

        public StageEndBackTask(StageStatusKey key, int actorIndex) {
            this.key = key;
            this.actorIndex = actorIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StageEndBackTask that = (StageEndBackTask) o;
            return actorIndex == that.actorIndex && Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, actorIndex);
        }

        @Override
        public void accept(KelpStageGraphActor self) {
            self.monitor.receiveStageEndBackTask(this);
        }
    }

    public static class CompletedTask implements CallableMessage.CallableMessageConsumer<KelpStageGraphActor>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        public StageStatusKey key;
        public int actorNodeIndex;

        public CompletedTask() {}

        public CompletedTask(StageStatusKey key, int actorNodeIndex) {
            this.key = key;
            this.actorNodeIndex = actorNodeIndex;
        }

        @Override
        public void accept(KelpStageGraphActor self) {
            if (key == null) { //start
                self.checkStageNext(null);
            } else {
                self.checkStageStatusCompleted(key, actorNodeIndex);
            }
        }

        @Override
        public String toString() {
            return "CompletedTask{" +
                    "key=" + key +
                    ", actorNodeIndex=" + actorNodeIndex +
                    '}';
        }
    }

    public void checkStageNext(StageStatusKey finishedKey) {
        this.startRefs.forEach(ActorRefShuffle::flush); //if ActorRef is ActorRefShuffleKelp, then it is expanded as a graph member.
        this.monitor.checkStageNext(finishedKey);
    }

    public boolean checkStageStatusCompleted(StageStatusKey key, int actorNodeIndex) {
        return monitor.checkStageStatusCompleted(key, actorNodeIndex);
    }

    public void logStatusAll(String fmt, Object... args) {
        logger.log("%s  %s",
                monitor.getStageStatusImplAll().format(monitor.getStageStartTime()),
                String.format(fmt, args));
    }
    static SampleTiming collectClockLogTiming = new SampleTiming();
    public static Map<ActorAddress.ActorAddressRemote, Integer> collectClockTable(ActorSystem system, ActorRef a, boolean sendUpdate) {
        Map<ActorAddress.ActorAddressRemote,Integer> hostToClock = new HashMap<>();
        ActorPlacement p = ActorPlacement.getPlacement(system);
        if (p instanceof ActorPlacement.ActorPlacementDefault) {
            List<ActorPlacement.AddressListEntry> cluster = ((ActorPlacement.ActorPlacementDefault) p).getCluster();
            //collect clock tables
            boolean log = ActorKelp.logDebugKelp && collectClockLogTiming.next();
            if (log) {
                system.getLogger().log("collectClockTable for %s : cluster=%s", a, cluster);
            }
            for (ActorPlacement.AddressListEntry e : cluster) {
                try {
                    GetClockTask task = remoteClock(system, e.getPlacementActor(), a, sendUpdate).get();
                    if (log) {
                        system.getLogger().log("collectClockTable     host=%s : clock=%,d", task.targetHost, task.resultClock);
                    }
                    if (task.isValid()) {
                        hostToClock.put(task.targetHost, task.resultClock);
                    }
                } catch (Exception ex) {
                    //TODO error
                    system.getLogger().log(true, 10, ex, "error collectClockTable %s", e);
                }
            }
        }
        return hostToClock;
    }

    public static CompletableFuture<GetClockTask> remoteClock(ActorSystem system, ActorAddress.ActorAddressRemoteActor place, ActorRef target, boolean sendUpdate) {
        return ResponsiveCalls.sendTask(system, place, new GetClockTask(target, sendUpdate));
    }


    public static class StageStatusKey implements Serializable {
        public static final long serialVersionUID = -1;
        public String stageName;
        public int progress;

        public StageStatusKey() {}

        public StageStatusKey(String stageName, int progress) {
            this.stageName = stageName;
            this.progress = progress;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StageStatusKey that = (StageStatusKey) o;
            return progress == that.progress && Objects.equals(stageName, that.stageName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stageName, progress);
        }

        @Override
        public String toString() {
            return "StageStatusKey{" +
                    "stageName='" + stageName + '\'' +
                    ", progress=" + progress +
                    '}';
        }

        public String format() {
            return String.format("<%s:%,d>", Staging.shortenId(stageName), progress);
        }
    }

    public interface StageStatusI {
        String getStageName();
        List<StageStatusKey> getKeys();
        Map<Integer, Double> getIndexToProgress();
        double getMaxProgress();
        Instant getFinishedTime();
        StageStatusI copy();

        default double getTotalProgress() {
            return getIndexToProgress().values().stream()
                    .mapToDouble(Double::doubleValue)
                    .sum();
        }

        default String format(Instant startTime) {
            String name = (getStageName() == null ? "" : getStageName());
            String ftStr = "";
            Instant ft = getFinishedTime();
            int ftWidth = startTime != null ? 11 : 30;
            if (ft != null && startTime != null) {
                ft = Instant.now();
            }
            ftStr = (ft != null && startTime != null ?
                    Duration.between(startTime, ft).toString() :
                    (ft == null ? "---" : ft.toString()));

            return String.format("%s %s%-" + ftWidth + "s",
                    formatProgress(), name, ftStr);
        }
        default String formatProgress() {
            double t = getTotalProgress();
            double m = getMaxProgress();

            double p = (m == 0 ? 0 : t / m);
            return String.format("[%,3.0f/%,3.0f (%3.0f%%)]", t, m, p * 100.0);
        }

        void printDetail(KelpStageGraphActor graph, PrintWriter out, int depth, Instant startTime, Duration nextDuration, StageStatusI lastStatus);

        default String formatProgress(Instant startTime, Instant finishedTime, Iterable<Integer> remainingIndices) {
            String str = formatProgress();
            if (finishedTime != null) {
                return str + " finish " + OffsetDateTime.ofInstant(finishedTime, ZoneId.systemDefault())
                        .format(DateTimeFormatter.ISO_DATE_TIME) +
                        (startTime == null ? "" : (" (" + Duration.between(startTime, finishedTime) + ")"));
            } else {
                return str + " remaining actors " + formatActorIndices(remainingIndices);
            }
        }

        static String formatActorIndices(Iterable<Integer> remainingIndices) {
            List<Integer> ns = new ArrayList<>();
            remainingIndices.forEach(ns::add);
            ns.sort(Comparator.naturalOrder());
            int rangeStart = Integer.MIN_VALUE;
            int rangeEnd = Integer.MIN_VALUE;
            StringBuilder buf = new StringBuilder();
            buf.append("{");
            for (int next : ns) {
                if (rangeEnd + 1 == next) {
                    rangeEnd = next;
                } else {
                    if (rangeStart >= 0) {
                        if (rangeStart == rangeEnd + 1) {
                            buf.append(", ").append(rangeEnd);
                        } else if (rangeStart < rangeEnd) {
                            buf.append("..").append(rangeEnd);
                        }
                        buf.append(", ");
                    }
                    rangeStart = next;
                    rangeEnd = next;
                    buf.append(next);
                }
            }
            if (rangeStart >= 0) {
                if (rangeStart == rangeEnd + 1) {
                    buf.append(", ").append(rangeEnd);
                } else if (rangeStart < rangeEnd) {
                    buf.append("..").append(rangeEnd);
                }
            }
            buf.append("}");
            return buf.toString();
        }
    }

    public static class StageStatus implements StageStatusI, Cloneable, Serializable {
        public static final long serialVersionUID = -1;
        public StageStatusKey key;
        public Set<Integer> remainingIndices = new HashSet<>();
        public Set<Integer> finishedIndices = new HashSet<>();
        public Instant finishedTime;

        protected Map<Integer, ActorKelpStats.ActorStats> lastActorStats = new HashMap<>();

        public StageStatus() {}

        public StageStatus(StageStatusKey key) {
            this.key = key;
            remainingIndices = new HashSet<>();
            finishedIndices = new HashSet<>();
        }

        public StageStatus(StageStatusKey key, List<Integer> is) {
            this.key = key;
            remainingIndices = new HashSet<>(is);
            finishedIndices = Collections.emptySet();
        }

        public Set<Integer> getRemainingIndices() {
            return remainingIndices;
        }

        public Set<Integer> getFinishedIndices() {
            return finishedIndices;
        }

        @Override
        public StageStatus copy() {
            try {
                StageStatus c = (StageStatus) clone();
                c.key = key;
                c.remainingIndices = new HashSet<>(remainingIndices);
                c.finishedIndices = new HashSet<>(finishedIndices);
                return c;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getStageName() {
            return key.stageName;
        }

        public StageStatusKey getKey() {
            return key;
        }

        @Override
        public List<StageStatusKey> getKeys() {
            return Collections.singletonList(key);
        }

        @Override
        public Map<Integer, Double> getIndexToProgress() {
            int size = size();
            Map<Integer, Double> res = new HashMap<>(size);
            remainingIndices.forEach(i -> res.put(i, 0.0));
            finishedIndices.forEach(i -> res.put(i, 1.0));
            return res;
        }

        @Override
        public double getMaxProgress() {
            return size();
        }

        public int size() {
            return remainingIndices.size() + finishedIndices.size();
        }

        public void addIndex(int actorNodeIndex) {
            remainingIndices.add(actorNodeIndex);
        }

        public boolean finishIndex(int actorNodeIndex) {
            if (remainingIndices.remove(actorNodeIndex) && remainingIndices.isEmpty()) {
                finishedTime = Instant.now();
                remainingIndices = Collections.emptySet();
            }
            finishedIndices.add(actorNodeIndex);
            return remainingIndices.isEmpty();
        }

        @Override
        public Instant getFinishedTime() {
            return finishedTime;
        }

        @Override
        public String toString() {
            return format(null);
        }

        @Override
        public void printDetail(KelpStageGraphActor graph, PrintWriter out, int depth,
                                Instant startTime, Duration nextDuration, StageStatusI lastStatus) {
            out.printf("%s%s %s%n", "  ".repeat(depth),
                    key.format(), formatProgress(startTime, finishedTime, remainingIndices));

            GraphStageNode sn = graph.getStageNode(key.stageName);
            Map<Integer, ActorRef> actors = sn.getActorNodes().stream()
                    .collect(Collectors.toMap(GraphActorNode::getIndex, GraphActorNode::getActor));
            String indent = "  ".repeat(depth + 1);

            List<Integer> idx = remainingIndices.stream()
                            .sorted()
                            .collect(Collectors.toList());

            Instant start = Instant.now();
            Map<Integer, CompletableFuture<ActorKelpStats.ActorStats>> stats = idx.stream()
                    .map(i -> Map.entry(i, getActorInfo(graph.getSystem(), actors.get(i), start)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            Duration awaitTime = awaitingTime(nextDuration, idx.size());

            idx.stream()
                    .map(i -> String.format("%s[%,3d]: %s: %s", indent, i,
                            getActorHeadInfo(actors.get(i)),
                            getActorInfo(start, i, graph.getSystem(), actors.get(i), stats.get(i), awaitTime, lastStatus)))
                    .forEach(out::println);
        }

        protected Duration awaitingTime(Duration nextDuration, int actorSize) {
            Duration d = nextDuration.dividedBy(100).multipliedBy(actorSize);
            Duration lowerBound = Duration.ofSeconds(5);
            if (d.compareTo(lowerBound) > 0) {
                return d;
            } else {
                return lowerBound;
            }
        }

        public String getActorHeadInfo(ActorRef a) {
            if (a instanceof Actor) {
                return Staging.shortenId(((Actor) a).getName());
            } else if (a instanceof ActorRefRemote) {
                ActorAddress addr = ((ActorRefRemote) a).getAddress();
                if (addr instanceof ActorAddress.ActorAddressRemoteActor) {
                    ActorAddress.ActorAddressRemoteActor ra = (ActorAddress.ActorAddressRemoteActor) addr;
                    String host = ra.toStringHostPort();
                    String aName = ra.getActorName();
                    return "remote(" + host + "/" + Staging.shortenId(aName) + ")";
                } else {
                    return "remote(" + addr + ")";
                }
            } else if (a instanceof ActorRefLocalNamed) {
                String name = ((ActorRefLocalNamed) a).getName();
                return Staging.shortenId(name);
            } else {
                return "" + a;
            }
        }

        public String getActorInfo(Instant start, int i, ActorSystem system,
                                   ActorRef a,
                                   CompletableFuture<ActorKelpStats.ActorStats> f,
                                   Duration awaitTime,
                                   StageStatusI lastStatus) {
            ActorKelpStats.ActorStats last = (lastStatus instanceof StageStatus ?
                    ((StageStatus) lastStatus).lastActorStats.get(i) : null);
            Instant checkStart = Instant.now();
            try {
                ActorKelpStats.ActorStats next;
                Duration remain = awaitTime.minus(Duration.between(start, checkStart));
                Duration lowerBound = Duration.ofSeconds(1);
                if (remain.compareTo(lowerBound) <= 0) {
                    remain = lowerBound;
                }
                if (ActorKelpStats.logDebug) {
                    system.getLogger().log(ActorKelpStats.logColor,
                            "start getActorInfo %s :  await=%s from=(%s %s)",
                            getActorHeadInfo(a), remain,
                            start, Duration.between(start, Instant.now()));
                }
                try {
                    next = f.get(remain.toMillis(), TimeUnit.MILLISECONDS);
                } catch (TimeoutException ex) {
                    next = f.get(1, TimeUnit.SECONDS);
                }
                if (ActorKelpStats.logDebug) {
                    system.getLogger().log(ActorKelpStats.logColor,
                            "end   getActorInfo %s : awaited=%s from=(%s %s) -> %s",
                            getActorHeadInfo(a), Duration.between(checkStart, Instant.now()),
                            start, Duration.between(start, Instant.now()),
                            next.toString(null));
                }
                lastActorStats.put(i, next);
                return next.toString(last);
            } catch (Exception ex) {
                system.getLogger().log("getActorInfo: %s %s : %s %s", ex, Thread.currentThread(),
                        getActorHeadInfo(a), Duration.between(checkStart, Instant.now()));
                return "error: " + ex;
            }

        }

        public CompletableFuture<ActorKelpStats.ActorStats> getActorInfo(ActorSystem system, ActorRef a, Instant start) {
            return ResponsiveCalls.sendTask(system, a, new ActorKelpStats.GetActorStatTask(start));
        }
    }

    public static class GetClockTask implements CallableMessage<Actor,GetClockTask>, Serializable {
        public static final long serialVersionUID = -1;
        public ActorRef target;
        public ActorAddress.ActorAddressRemote targetHost;
        public int resultClock = -1;
        public boolean sendUpdate;

        public GetClockTask() {}

        public GetClockTask(ActorRef target, boolean sendUpdate) {
            this.target = target;
            this.sendUpdate = sendUpdate;
        }

        @Override
        public GetClockTask call(Actor place) {
            //place.system might hold the table entry for the remote target, targetHost:targetActor <---<resultClock:msg>--- someLocalActor
            if (place.getSystem() instanceof ActorSystemRemote && target instanceof ActorRefRemote) {
                target.tellMessage(new Message.MessageNone(target)); //update clock by sending an empty message from the host

                ActorSystemRemote system = (ActorSystemRemote) place.getSystem();
                ActorRefRemote remoteRef = (ActorRefRemote) target;
                targetHost = system.getServerAddress().getHostAddress();
                Integer c = system.getClockTable().get(remoteRef);
                if (c != null) {
                    resultClock = c;
                }
            }
            return this;
        }

        public boolean isValid() {
            return resultClock != -1;
        }
    }

    public static class WatchTask implements CallableMessage.CallableMessageConsumer<Actor>, Message.MessageDataDelayed {
        public static final long serialVersionUID = -1;
        public StageStatusKey key;
        public int actorNodeIndex;
        public ActorRef graph;
        public Map<ActorAddress.ActorAddressRemote, Integer> remoteClocks;
        public int retryCount;

        public WatchTask() {}

        public WatchTask(StageStatusKey key, int actorNodeIndex, ActorRef graph, Map<ActorAddress.ActorAddressRemote, Integer> remoteClocks) {
            this.key = key;
            this.actorNodeIndex = actorNodeIndex;
            this.graph = graph;
            this.remoteClocks = remoteClocks; //can be null
        }

        @Override
        public void accept(Actor self) {
            if (checkEmptyMailbox(self) && complete(self)) {
                if (ActorKelp.logDebugKelp) self.getSystem().getLogger().log(ActorKelp.logDebugKelpColor,
                        "%s complete %s", this, self);
                this.graph.tell(new CompletedTask(key, actorNodeIndex).withSender(self));
            } else {
                incrementRetryCount(self);
                self.tell(this.withSender(null)); //retry : no sender
            }
        }
        protected void incrementRetryCount(Actor self) {
            if ((retryCount % 1000 == 0) && ActorKelp.logDebugKelp) {
                self.getSystem().getLogger().log(ActorKelp.logDebugKelpColor,
                        "%s incrementRetryCount retryCount=%,d: %s empty?=%s, remoteClocks=%s",
                        this, retryCount, self, checkEmptyMailbox(self), remoteClocks);
            }
            if (retryCount >= 100) {
                try {
                    Thread.sleep(10);
                } catch (Exception ex) {
                    self.getSystem().getLogger().log(ActorKelp.logDebugKelp, ActorKelp.logDebugKelpColor,
                            ex, "%s retryCount=%,d : %s", this, retryCount, self);
                }
            }
            retryCount++;
        }

        protected boolean checkEmptyMailbox(Actor self) {
            return self.isEmptyMailbox();
        }

        public boolean complete(Actor self) {
            ActorRefShuffle.flush(self);
            if (checkCompleteClocks(self)) { //true if all task sent by remote hosts are completed
                if (self instanceof Staging.StagingSupported) {
                    return ((Staging.StagingSupported) self).processStageExited(key);
                } else {
                    return true;
                }
            } else {
                return false;
            }
        }

        protected boolean checkCompleteClocks(Actor self) {
            if (remoteClocks == null) {
                remoteClocks = collectClockTable(self.getSystem(), self, true);
            }
            remoteClocks = new HashMap<>(remoteClocks);
            self.getClocks()
                    .forEach(this::complete);
            boolean b = remoteClocks.isEmpty();
            if (b) {
                return true;
            } else {
                if (retryCount % 100 == 0) {
                    self.getSystem().getLogger().log(10, "retry=%,d remainingClocks=%s localClocks=%s collectClockTable for %s",
                            retryCount, remoteClocks, self.getClocks(), self);
                }
                remoteClocks = collectClockTable(self.getSystem(), self, true);
                return false;
            }
        }

        public void complete(Object from, int localClock) {
            if (from instanceof ActorAddress.ActorAddressRemote) {
                remoteClocks.computeIfPresent((ActorAddress.ActorAddressRemote) from,
                        (v, remoteClock) -> localClock >= remoteClock ? null : remoteClock); //drop completed
            }
        }
    }

    ///obtaining current checking status
    public static class StageStatusMulti implements StageStatusI, Serializable {
        public static final long serialVersionUID = -1;
        public String stageName;
        public List<StageStatusI> statusList;

        public StageStatusMulti() {}

        public StageStatusMulti(String stageName, List<StageStatusI> statusList) {
            this.stageName = stageName;
            this.statusList = statusList;
        }

        @Override
        public StageStatusMulti copy() {
            return new StageStatusMulti(stageName, statusList.stream()
                    .map(StageStatusI::copy)
                    .collect(Collectors.toList()));
        }

        @Override
        public String getStageName() {
            return stageName;
        }

        @Override
        public List<StageStatusKey> getKeys() {
            return statusList.stream()
                    .flatMap(s -> s.getKeys().stream())
                    .collect(Collectors.toList());
        }

        public List<StageStatusI> getStatusList() {
            return statusList;
        }

        @Override
        public Map<Integer, Double> getIndexToProgress() {
            Map<Integer, Double> total = new HashMap<>();
            for (StageStatusI m : statusList) {
                Map<Integer, Double> p = m.getIndexToProgress();
                p.forEach((k,v) ->
                        total.compute(k, (_k,ev) -> (ev == null ? 0 : ev) + v));
            }
            return total;
        }

        @Override
        public double getMaxProgress() {
            return statusList.stream()
                    .mapToDouble(StageStatusI::getMaxProgress)
                    .sum();
        }

        @Override
        public Instant getFinishedTime() {
            Instant t = null;
            for (StageStatusI f : statusList) {
                Instant ft = f.getFinishedTime();
                if (ft == null) {
                    return null;
                } else if (t == null || ft.isAfter(t)) {
                    t = ft;
                }
            }
            return t;
        }

        @Override
        public String toString() {
            return format(null);
        }

        @Override
        public void printDetail(KelpStageGraphActor graph, PrintWriter out, int depth,
                                Instant startTime, Duration nextDuration, StageStatusI lastStatus) {
            out.printf("%s %s%n", "  ".repeat(depth),
                    formatProgress());

            if (lastStatus != null && Objects.equals(lastStatus.getStageName(), getStageName())
                    && lastStatus instanceof StageStatusMulti) {
                Function<StageStatusI, StageStatusI> finder =
                        s -> ((StageStatusMulti) lastStatus).getStatusList().stream()
                                .filter(ls -> Objects.equals(ls.getStageName(), s.getStageName()))
                                .findFirst().orElse(null);
                statusList.forEach(e ->
                        e.printDetail(graph, out, depth + 1, startTime, nextDuration, finder.apply(e)));
            } else {
                statusList.forEach(e ->
                        e.printDetail(graph, out, depth + 1, startTime, nextDuration, null));
            }
        }
    }

    public CompletableFuture<StageStatusI> getStageStatusAll() {
        return ResponsiveCalls.sendTask(getSystem(), this,
                new GetStageStatusTask(GetStageStatusOp.All, null));
    }

    public CompletableFuture<StageStatusI> getStateStatusCurrent() {
        return ResponsiveCalls.sendTask(getSystem(), this,
                new GetStageStatusTask(GetStageStatusOp.Current, null));
    }

    public CompletableFuture<StageStatusI> getStageStatus(String stageName) {
        return ResponsiveCalls.sendTask(getSystem(), this,
                new GetStageStatusTask(GetStageStatusOp.Stage, stageName));
    }

    public StageStatusI getStageStatusImpl(GetStageStatusOp op, String stageName) {
        switch (op) {
            case Stage:
                return monitor.getStageStatusImplStage(stageName);
            case All:
                return monitor.getStageStatusImplAll();
            case Current:
                return monitor.getStageStatusImplCurrent();
        }
        return null;
    }

    public enum GetStageStatusOp implements Serializable {
        Stage,
        All,
        Current;

        public static final long serialVersionUID = -1;
    }

    public static class GetStageStatusTask implements CallableMessage<KelpStageGraphActor, StageStatusI>, Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;
        public GetStageStatusOp op;
        public String stageName;

        public GetStageStatusTask() {}

        public GetStageStatusTask(GetStageStatusOp op, String stageName) {
            this.op = op;
            this.stageName = stageName;
        }

        @Override
        public StageStatusI call(KelpStageGraphActor self) {
            return self.getStageStatusImpl(op, stageName);
        }
    }

    ////// save


    public void saveGraph(Writer w, boolean actorNodes, boolean stageNodes) throws IOException  {
        List<GraphActorNode> ns = getActorNodes();
        List<GraphStageNode> ss = new ArrayList<>(this.nameToStageNode.values());
        w.write("digraph {\n");
        if (actorNodes) {
            for (KelpStageGraphActor.GraphActorNode node : ns) {
                w.write("n" + ns.indexOf(node) +
                        " [label=\"[" + node.getIndex() + "] " +
                                Staging.shortenId(node.getName()) +
                        "\", shape=\"box\"]" +
                        ";\n");
            }
            for (KelpStageGraphActor.GraphActorNode node : ns) {
                for (KelpStageGraphActor.GraphActorNode to : node.getNextNodes()) {
                    w.write("n" + ns.indexOf(node) + " -> " +
                            "n" + ns.indexOf(to) + ";\n");
                }
            }
        }
        if (stageNodes) {
            for (GraphStageNode node : ss) {
                w.write("s" + ss.indexOf(node) +
                        " [label=\"{" + node.getEnterIdStart() + "+" + node.getEnters() + "} " +
                            Staging.shortenId(node.getName()) +
                            " [" + node.getActors().size() + "]" +
                        "\", shape=\"box\"]" +
                        ";\n");
            }
            for (GraphStageNode node : ss) {
                for (GraphStageNode to : node.getNextNodes()) {
                    w.write("s" + ss.indexOf(node) + " -> " +
                            "s" + ss.indexOf(to) + ";\n");
                }
            }
            if (actorNodes) {
                for (GraphStageNode node : ss) {
                    for (GraphActorNode a : node.getActorNodes()) {
                        w.write("s" + ss.indexOf(node) + " -> " +
                                "n" + ns.indexOf(a) +
                                " [style=dotted]" +
                                ";\n");
                    }
                }
            }
        }

        w.write("\n}");
    }

    //////////////

    public KelpStageGraphActor withSaveGraph(String path) {
        Path file = PathModifier.getPathModifier(system).get(path);
        Path parent = file.getParent();
        try {
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }
        } catch (Exception ex) {
            logger.log(ex, "saveGraph error %s", parent);
        }
        try (Writer out = Files.newBufferedWriter(file)) {
            saveGraph(out, false, true);
            logger.log("saveGraph finish %s", file);
        } catch (Exception ex) {
            logger.log(ex, "saveGraph error %s", file);
        }
        return this;
    }

    public KelpStageGraphActor withStageEnterHandler(BiConsumer<KelpStageGraphActor, GraphStageNode> handler) {
        this.stageEnterHandler = handler;
        return this;
    }
    public KelpStageGraphActor withStageExitHandler(BiConsumer<KelpStageGraphActor, GraphStageNode> handler) {
        this.stageExitHandler = handler;
        return this;
    }
    public KelpStageGraphActor withStageEndHandler(BiConsumer<KelpStageGraphActor, GraphStageNode> handler) {
        this.stageEndHandler = handler;
        return this;
    }

    public KelpStageGraphActor withLogStageEnterExit(boolean flag) {
        this.logStageEnterExit = flag;
        return this;
    }

    public KelpStageGraphActor withLogStageEnd(boolean flag) {
        this.logStageEnd = flag;
        return this;
    }
    public KelpStageGraphActor withLogPeriodicOff() {
        this.logPeriodic = null;
        return this;
    }
    public KelpStageGraphActor withLogPeriodic(Duration duration) {
        return withLogPeriodic(duration, 1, duration, true);
    }
    public KelpStageGraphActor withLogPeriodic(Duration duration, int delayFactory, Duration maxDuration) {
        return withLogPeriodic(duration, delayFactory, maxDuration, true);
    }
    public KelpStageGraphActor withLogPeriodic(Duration duration, int delayFactory, Duration maxDuration, boolean detail) {
        this.logPeriodic = new LogStatusPeriodic(this, duration, delayFactory, maxDuration, detail);
        return this;
    }

    public CompletableFuture<KelpStageGraphActor> startAwait() {
        return startAwait(Instant.now());
    }

    public CompletableFuture<KelpStageGraphActor> startAwait(Instant startTime) {
        return start(startTime)
                .await(() -> {})
                .thenApply(e -> this);
    }

    public CompletableFuture<KelpStageGraphActor> startAwaitTellFile(String path) {
        return startAwaitTellFile(Instant.now(), path);
    }

    public CompletableFuture<KelpStageGraphActor> startAwaitTell(Object msg) {
        return startAwaitTell(Instant.now(), msg);
    }

    public CompletableFuture<KelpStageGraphActor> startAwaitTellFile(Instant startTime, String path) {
        return start(startTime)
                .awaitTellFile(path);
    }

    public CompletableFuture<KelpStageGraphActor> startAwaitTell(Instant startTime, Object msg) {
        return start(startTime)
                .awaitTell(msg);
    }


    public CompletableFuture<KelpStageGraphActor> awaitTellFile(String path) {
        Object msg;
        if (getSystem() instanceof ActorKelpBuilder) {
            msg = ((ActorKelpBuilder) getSystem()).internalFactory().getFileSplitInitMessage(this, path);
        } else {
            msg = new FileSplitter.FileSplit(path);
        }
        return awaitTell(msg);
    }

    public CompletableFuture<KelpStageGraphActor> awaitTell(Object msg) {
        return await(() -> getStartRefs().forEach(a -> a.tell(msg)));
    }

    public Instant getStageStartTime() {
        return monitor.getStageStartTime();
    }

    public Instant getStageEndTime() {
        return monitor.getStageEndTime();
    }

    public Duration getStageElapsedTime() {
        return monitor.getStageElapsedTime();
    }

    /////// log periodic

    public void logStatusDetail() {
        new LogStatusPeriodic(this).logDetailNow();
    }

    public static class LogStatusPeriodic {
        protected KelpStageGraphActor graph;

        protected Duration duration;
        protected Duration nextDuration;
        protected int delayFactor;
        protected Duration maxDuration;
        protected boolean detail;

        protected Future<?> nextTask;
        protected boolean closed;
        protected double previousStatus;

        protected Instant nextDetail = Instant.EPOCH;

        protected StageStatusI lastStatus;

        public LogStatusPeriodic(KelpStageGraphActor graph) {
            this(graph, Duration.ofMinutes(3), 2, Duration.ofHours(1), true);
        }

        public LogStatusPeriodic(KelpStageGraphActor graph, Duration duration, int delayFactor, Duration maxDuration, boolean detail) {
            this.duration = duration;
            this.nextDuration = duration;
            this.delayFactor = delayFactor;
            this.maxDuration = maxDuration;
            this.detail = detail;
            this.graph = graph;
        }

        public LogStatusPeriodic copy(KelpStageGraphActor graph) {
            return new LogStatusPeriodic(graph, this.duration, this.delayFactor, this.maxDuration, this.detail);
        }

        public Duration nextDuration() {
            Duration res = nextDuration;
            nextDuration = nextDuration.multipliedBy(Math.max(1, delayFactor));
            if (nextDuration.compareTo(maxDuration) > 0) {
                nextDuration = maxDuration;
            }
            return res;
        }

        public void logAndSubmit() {
            try {
                log();
            } catch (Throwable ex) {
                ex.printStackTrace();
            } finally {
                submitNext();
            }
        }

        public void log() {
            StageMonitor check = graph.monitor;
            StageStatusI status = check.getStageStatusImplAll();
            double progress = status.getTotalProgress();
            Instant startTime = check.getStageStartTime();
            Duration totalElapsed = Duration.between(startTime, Instant.now());
            String str = String.format("%s  %s",
                    status.format(check.getStageStartTime()),
                    String.format(" status: after %s, total %s, progress:%,3.2f -> %,3.2f",
                            nextDuration, totalElapsed, previousStatus, progress));
            if (detail) {
                String detail = logDetailIfOver(status);
                if (!detail.isEmpty()) {
                    str = String.format("%s%n%s", str, detail);
                }
            }
            graph.logger.log("%s", str);
            previousStatus = progress;
        }

        public void logDetailNow() { //explicit call
            StageMonitor check = graph.monitor;
            StageStatusI status = check.getStageStatusImplAll();
            double progress = status.getTotalProgress();
            Instant startTime = check.getStageStartTime();
            Duration totalElapsed = Duration.between(startTime, Instant.now());
            String str = String.format("%s  %s",
                    status.format(check.getStageStartTime()),
                    String.format(" status: after total %s, progress:%,3.2f",
                            totalElapsed, progress));
            String detail = logDetailIfOver(status);
            if (!detail.isEmpty()) {
                str = String.format("%s%n%s", str, detail);
            }
            graph.logger.log("%s", str);
        }

        public String logDetailIfOver(StageStatusI status) {
            Instant startDetail = Instant.now();
            if (startDetail.isAfter(nextDetail)) {
                String res = logDetail(status);
                Instant afterDetail = Instant.now();
                Duration detailTime = Duration.between(startDetail, afterDetail)
                        .plus(nextDuration.dividedBy(10));
                if (detailTime.compareTo(nextDuration) < 0) {
                    detailTime = nextDuration;
                }
                nextDetail = afterDetail.plus(detailTime);
                return res;
            } else {
                return "";
            }
        }

        public String logDetail(StageStatusI status) {
            StringWriter sw = new StringWriter();
            PrintWriter out = new PrintWriter(sw);
            status.printDetail(graph, out, 1, graph.monitor.getStageStartTime(), nextDuration, lastStatus);
            out.flush();
            lastStatus = status;
            return sw.getBuffer().toString();
        }

        public synchronized void submitNext() {
            if (closed) {
                return;
            }
            Duration d = nextDuration();
            nextTask = graph.getSystem().getScheduledExecutor()
                    .schedule(this::logAndSubmit, Math.max(1L, d.getSeconds()), TimeUnit.SECONDS);
        }

        public synchronized void close() {
            closed = true;
            if (nextTask != null) {
                nextTask.cancel(true);
            }
        }

        public Duration getDuration() {
            return duration;
        }

        public void setDuration(Duration duration) {
            this.duration = duration;
            this.nextDuration = duration;
        }

        public float getDelayFactor() {
            return delayFactor;
        }

        public void setDelayFactor(int delayFactor) {
            this.delayFactor = delayFactor;
        }

        public Duration getMaxDuration() {
            return maxDuration;
        }

        public void setMaxDuration(Duration maxDuration) {
            this.maxDuration = maxDuration;
        }

        public boolean isDetail() {
            return detail;
        }

        public void setDetail(boolean detail) {
            this.detail = detail;
        }
    }
}


