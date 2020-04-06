package csl.actor.example.exp.delayedlabel;

import csl.actor.*;
import csl.actor.cluster.PhaseShift;
import csl.actor.kelp.Config;
import csl.actor.kelp.MessageNoRouting;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DelayedLabelManual {
    public static class DelayedLabelConfig extends Config {
        public static final long serialVersionUID = 1L;
        @CommandArgumentOption(abbrev = "-n", help = "number of instances.")
        public int instances = 100_000;
        @CommandArgumentOption(abbrev = "-c", help = "number of labels.")
        public int classes = 100;
        @CommandArgumentOption(abbrev = "-v", help = "length of feature vector.")
        public int vectorLength = 300;
        @CommandArgumentOption(abbrev = "-c", help = "number of label delay.")
        public int delay = 100;
    }

    public DelayedLabelConfig config = new DelayedLabelConfig();

    public static void main(String[] args) {
        new DelayedLabelManual().run(args);
    }

    public void run(String... args) {
        config.read("", System.getProperties());

        List<String> rest = config.readArgs(args);
        if (rest.contains("--help")) {
            System.out.println(getClass().getName());
            config.showHelp();
            return;
        }
        String file = null;
        if (!rest.isEmpty()) {
            String nums = rest.get(0);
            try {
                config.instances = (Integer) config.readValue(Integer.class, nums);
            } catch (NumberFormatException ne) {
                config.instances = readHead(nums);
                file = nums;
            }
        }

        config.log("%s", config.toString());

        run(file);
    }

    public int readHead(String file) {
        try (BufferedReader r = Files.newBufferedReader(Paths.get(file))) {
            return Integer.parseInt(r.readLine());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void run(String src) {
        ActorSystem system = new ActorSystemDefault();
        ActorSystem.SystemLogger out = system.getLogger();
        Iterator<Object> inputs = inputs(out, src);

        Instant startTime = Instant.now();

        ResultActor resultActor = resultActor(system, out, startTime);
        ActorRef learnerActor = learnerActor(system, out, resultActor);

        while (inputs.hasNext()) {
            learnerActor.tell(inputs.next());
        }
        try {
            PhaseShift.start(system, learnerActor, startTime).get();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        system.close();
    }

    public Iterator<Object> inputs(ActorSystem.SystemLogger out, String src) {
        int numInstances = config.instances;
        if (src == null) {
            Instant startGenTime = Instant.now();
            List<Object> inputs = generateInput(12345L, config.classes, config.vectorLength, numInstances, config.delay);
            out.log(String.format("#generateInput: %,d %s", numInstances, Duration.between(startGenTime, Instant.now())));
            return inputs.iterator();
        } else {
            Instant startGenTime = Instant.now();
            List<Object> buf = new ArrayList<>(numInstances * 2);
            for (Iterator<Object> i = readInputs(src); i.hasNext(); ) {
                buf.add(i.next());
            }
            out.log(String.format("#readInput: %,d %s %s", numInstances, Duration.between(startGenTime, Instant.now()), src));
            return buf.iterator();
        }
    }

    private Iterator<Object> readInputs(String src) {
        try {
            final BufferedReader r = Files.newBufferedReader(Paths.get(src));
            r.readLine(); //skip head
            return new Iterator<Object>() {
                String next = r.readLine();
                @Override
                public boolean hasNext() {
                    return next != null;
                }

                @Override
                public Object next() {
                    String n = next;
                    try {
                        next = r.readLine();
                        if (next == null) {
                            r.close();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    return parseLine(n);
                }
            };
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public Object parseLine(String line) {
        String[] cols = line.split(",");
        if (cols[0].equals("L")) {
            return new LabelInstance(Integer.parseInt(cols[1]), Integer.parseInt(cols[2]));
        } else if (cols[0].equals("F")) {
            return new FeatureInstance(Integer.parseInt(cols[1]), Arrays.stream(Arrays.copyOfRange(cols, 2, cols.length))
                    .mapToDouble(Double::parseDouble)
                    .toArray());
        } else {
            System.err.println("#error: " + line);
            return null;
        }
    }

    public ActorRef learnerActor(ActorSystem system, ActorSystem.SystemLogger out, ActorRef resultActor) {
        return new LearnerActor(system, resultActor);
    }

    public ResultActor resultActor(ActorSystem system, ActorSystem.SystemLogger out, Instant startTime) {
        return new ResultActor(system, out, startTime, config.instances);
    }

    public static class ResultActor extends ActorDefault {
        Instant startTime;
        long numInstances;
        long finishedInstances;
        ActorSystem.SystemLogger out;
        Instant lastTime;

        public ResultActor(ActorSystem system, ActorSystem.SystemLogger out, Instant startTime, int numInstances) {
            super(system, "resultActor");
            this.startTime = startTime;
            this.numInstances = numInstances;
            this.out = out;
            this.lastTime = startTime;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchWithSender(Integer.class, this::receive)
                    .build();
        }

        public void receive(int next, ActorRef sender) {
            finishedInstances++;
            lastTime = Instant.now();
            if (numInstances <= finishedInstances) {
                Duration d = Duration.between(startTime, Instant.now());
                out.log(String.format("#finish: %,d %s", finishedInstances, d));
            }
        }
    }

    public static class LearnerActor extends ActorDefault implements PhaseShift.StageSupported {
        Map<Integer, double[]> featureBuffer = new HashMap<>();
        LearnerModel model;

        ActorRef nextStage;

        @Override
        public ActorRef nextStage() {
            return nextStage;
        }

        public void setNextStage(ActorRef nextStage) {
            this.nextStage = nextStage;
        }

        @Override
        public void logPhase(String str, Object... args) {
            getSystem().getLogger().log(str, args);
        }

        public LearnerActor(ActorSystem system, String name, ActorRef resultActor) {
            super(system, name);
            this.model = new LearnerModel(resultActor);
            nextStage = resultActor;
        }

        public LearnerActor(ActorSystem system, ActorRef resultActor) {
            super(system);
            this.model = new LearnerModel(resultActor);
            nextStage = resultActor;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(FeatureInstance.class, this::testAndKeep)
                    .match(LabelInstance.class, this::train)
                    .build();
        }

        public void testAndKeep(FeatureInstance i) {
            featureBuffer.put(i.getId(), i.getVector());
        }

        public void train(LabelInstance i) {
            double[] vs = featureBuffer.remove(i.getId());
            DataInstance di = new DataInstance(i.getId(), vs, i.getLabel());

            model.train(di, this);
        }
    }

    public static class LearnerModel {
        public long numSamples = 0;
        public Map<Integer, LearnerEntry> model = new HashMap<>();
        public ActorRef resultActor;

        public LearnerModel(ActorRef resultActor) {
            this.resultActor = resultActor;
        }

        public void train(DataInstance di, ActorRef sender) {
            ++numSamples;
            model.computeIfAbsent(di.getLabel(), l -> new LearnerEntry(l, di.getVector().length))
                    .add(di);
            int id = di.getId();
            resultActor.tell(id, sender);
        }
    }

    public static class Finish implements MessageNoRouting {
        public long numInstances;

        public Finish(long numInstances) {
            this.numInstances = numInstances;
        }
    }

    public static class FeatureInstance {
        int id;
        double[] vector;

        public FeatureInstance(int id, double[] vector) {
            this.id = id;
            this.vector = vector;
        }

        public int getId() {
            return id;
        }

        public double[] getVector() {
            return vector;
        }
    }

    public static class LabelInstance {
        int id;
        int label;

        public LabelInstance(int id, int label) {
            this.id = id;
            this.label = label;
        }

        public int getId() {
            return id;
        }

        public int getLabel() {
            return label;
        }
    }

    public static class DataInstance {
        int id;
        double[] vector;
        int label;

        public DataInstance(int id, double[] vector, int label) {
            this.id = id;
            this.vector = vector;
            this.label = label;
        }

        public int getId() {
            return id;
        }

        public double[] getVector() {
            return vector;
        }

        public int getLabel() {
            return label;
        }
    }

    public static class LearnerEntry {
        public int label;
        public double[] vector;
        public long count;

        public LearnerEntry(int label, int size) {
            this.label = label;
            this.vector = new double[size];
        }

        public void add(DataInstance instance) {
            double[] vs = instance.getVector();
            for (int i = 0, l = vector.length; i < l; ++i) {
                vector[i] += vs[i];
            }
            count++;
        }
    }

    public List<Object> generateInput(long seed, int classes, int vectorLength, int instances, int delay) {
        Random r = new Random(seed);
        ArrayList<DelayedData> res = new ArrayList<>(instances * 2);

        List<Integer> ids = IntStream.range(0, instances)
                .boxed()
                .collect(Collectors.toList());
        Collections.shuffle(ids, r);

        for (int i = 0; i < instances; ++i) {
            int id = ids.get(i);
            long featureTime;
            long labelTime;
            if (i < delay) {
                featureTime = i;
                labelTime = delay + 2 * i;
            } else {
                featureTime = 1 + 2 * i - delay;
                labelTime = 2 * i + delay;
            }
            double[] data = r.doubles(vectorLength, 0, 1.0).toArray();
            res.add(new DelayedData(featureTime, new FeatureInstance(id, data)));
            res.add(new DelayedData(labelTime, new LabelInstance(id, r.nextInt(classes))));
        }

        return res.stream()
                .sorted(Comparator.comparingLong(DelayedData::getTime))
                .map(DelayedData::getItem)
                .collect(Collectors.toList());
    }

    public static class DelayedData {
        long time;
        Object item;

        public DelayedData(long time, Object item) {
            this.time = time;
            this.item = item;
        }

        public long getTime() {
            return time;
        }

        public Object getItem() {
            return item;
        }
    }
}
