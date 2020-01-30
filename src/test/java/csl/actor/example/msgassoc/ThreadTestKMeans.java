package csl.actor.example.msgassoc;

import csl.actor.ActorRef;
import csl.actor.ActorSystem;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ThreadTestKMeans extends ThreadTest {
    public static void main(String[] args) {
        new ThreadTestKMeans().run(args);
    }

    @Override
    public ThreadTest create() {
        return new ThreadTestKMeans();
    }

    @Override
    ReadActor newReadActor(ActorSystem sys, int r, ActorRef fa) {
        return new ReadActorKMeans(sys, r, fa);
    }

    static class ReadActorKMeans extends ThreadTest.ReadActor {
        double diff;
        public ReadActorKMeans(ActorSystem system, int n, ActorRef target) {
            super(system, n, target);
        }

        @Override
        void receive(Object s) {
            diff = kMeans();
            super.receive(diff);
        }

        @Override
        public String toString() {
            return super.toString() + String.format(":%3.2f", diff);
        }
    }

    static Data argMin(Data d, List<Data> ks) {
        return ks.stream()
                .reduce((prev, next) ->
                        d.distance(prev) < d.distance(next)?
                                prev : next).orElse(ks.get(0));
    }

    @Override
    protected ThreadComp createThreadComp(int th, String title) {
        return new ThreadCompKMeans(th, title);
    }

    @Override
    public Object process(ThreadComp tc, Object o) {
        o = ((ThreadCompKMeans) tc).diff = kMeans();
        return super.process(tc, o);
    }

    static class ThreadCompKMeans extends ThreadComp {
        public double diff;
        public ThreadCompKMeans(int th, String title) {
            super(th, title);
        }

        @Override
        public String toString() {
            return super.toString() + String.format(":%3.2f", diff);
        }
    }


    public static double kMeans() {

        Random rand = new Random();

        List<Data> data = IntStream.range(0, 100)
                .mapToObj(Data::new)
                .map(d -> d.set(rand))
                .collect(Collectors.toList());

        List<Integer> is = IntStream.range(0, data.size())
                .boxed().collect(Collectors.toList());
        Collections.shuffle(is, rand);
        List<Data> ks = is.subList(0, 10).stream()
                .map(i -> data.get(i).copy())
                .collect(Collectors.toList());
        List<Data> iks = ks;
        IntStream.range(0, iks.size()).forEach(i -> iks.get(i).n = i);

        double diff = 0;
        for (int i = 0; i < 100; ++i) {
            List<Data> prev = ks;
            TreeMap<Integer,List<Data>> map = new TreeMap<>();
            data.forEach(d ->
                    map.computeIfAbsent(argMin(d, prev).n, k -> new ArrayList<>()).add(d));
            List<Data> nks = new ArrayList<>(prev);
            map.forEach((k,v) -> nks.set(k, prev.get(k).copy().set(v)));

            diff = IntStream.range(0, prev.size())
                    .mapToDouble(j -> prev.get(j).distance(nks.get(j)))
                    .average().orElse(0);
            if (diff < 0.001){
                break;
            }

            ks = nks;
        }
        return diff;
    }

    static class Data {
        public int n;
        public double[] vec;

        public Data(int n) {
            this.n = n;
        }
        public Data set(Random rand) {
            vec = rand.doubles(100).toArray();
            return this;
        }
        public double distance(Data d) {
            double diff = 0;
            for (int i = 0, l = vec.length; i < l; ++i) {
                diff += Math.abs(vec[i] - d.vec[i]);
            }
            return diff / vec.length;
        }
        public Data copy() {
            Data d = new Data(n);
            d.vec = Arrays.copyOf(vec, vec.length);
            return d;
        }

        public Data set(List<Data> vs) {
            Arrays.fill(vec, 0);
            for (Data v : vs) {
                v.n = n;
                for (int i = 0, l = vec.length; i < l; ++i) {
                    vec[i] += v.vec[i];
                }
            }
            for (int i = 0, l = vec.length; i < l; ++i) {
                vec[i] /= vs.size();
            }
            return this;
        }
    }
}
