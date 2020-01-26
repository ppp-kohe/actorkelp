package csl.actor.example.delayedlabel;

import csl.actor.*;

import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DelayedLabelManual {
    public static void main(String[] args) {
        run(new DelayedLabelManual(), args);
    }

    public static void run(DelayedLabelManual r, String... args) {
        String nums = args[0];
        int numInstances = Integer.parseInt(nums.replaceAll("_", ""));
        PrintWriter out = new PrintWriter(System.out, true);
        r.run(out, numInstances);
    }

    public void run(PrintWriter out, int numInstances) {
        Instant startGenTime = Instant.now();
        List<Object> inputs = generateInput(12345L, 100, 300, numInstances, 100);
        out.println(String.format("#generateInput: %,d %s", numInstances, Duration.between(startGenTime, Instant.now())));

        Instant startTime = Instant.now();

        ActorSystem system = new ActorSystemDefault();
        ResultActor resultActor = new ResultActor(system, out, startTime, numInstances);
        ActorRef learnerActor = learnerActor(system, out, resultActor, numInstances);

        for (Object i : inputs) {
            learnerActor.tell(i, null);
        }
    }

    public ActorRef learnerActor(ActorSystem system, PrintWriter out, ActorRef resultActor, int numInstances) {
        return new LearnerActor(system, resultActor);
    }

    public static class ResultActor extends ActorDefault {
        Instant startTime;
        long numInstances;
        long finishedInstances;
        PrintWriter out;

        public ResultActor(ActorSystem system, PrintWriter out, Instant startTime, int numInstances) {
            super(system);
            this.startTime = startTime;
            this.numInstances = numInstances;
            this.out = out;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchWithSender(Integer.class, this::receive)
                    .build();
        }

        public void receive(int next, ActorRef sender) {
            finishedInstances++;
            if (numInstances <= finishedInstances) {
                Duration d = Duration.between(startTime, Instant.now());
                out.println(String.format("#finish: %,d %s", finishedInstances, d));
                sender.tell(new Finish(finishedInstances), this);
                //finish
                //((ActorSystemDefault) getSystem()).stop();
            }
        }
    }

    public static class LearnerActor extends ActorDefault {
        Map<Integer, double[]> featureBuffer = new HashMap<>();
        LearnerModel model;

        public LearnerActor(ActorSystem system, String name, ActorRef resultActor) {
            super(system, name);
            this.model = new LearnerModel(resultActor);
        }

        public LearnerActor(ActorSystem system, ActorRef resultActor) {
            super(system);
            this.model = new LearnerModel(resultActor);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(FeatureInstance.class, this::testAndKeep)
                    .match(LabelInstance.class, this::train)
                    .match(Finish.class, this::finish)
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

        public void finish(Finish f) {
            System.exit(0);
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

    public static class Finish {
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
