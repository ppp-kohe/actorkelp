package csl.actor.example.keyaggregate;

import csl.actor.ActorRef;
import csl.actor.ActorSystem;

import java.util.Arrays;
import java.util.Random;

public class ThreadTestVecSum extends ThreadTest {
    public static void main(String[] args) {
        new ThreadTestVecSum().run(args);
    }

    @Override
    public ThreadTest create() {
        return new ThreadTestVecSum();
    }

    @Override
    protected ThreadComp createThreadComp(int th, String title) {
        return new ThreadCompVecSum(th, title);
    }

    @Override
    ReadActor newReadActor(ActorSystem sys, int r, ActorRef fa) {
        return new ReadActorVecSum(sys, r, fa);
    }

    static int vecSize = 2048;

    static class ThreadCompVecSum extends ThreadComp {
        double[] vec = new double[vecSize];
        Random rand = new Random();
        public ThreadCompVecSum(int th, String title) {
            super(th, title);
        }

        double sum() {
            return ThreadTestVecSum.sum(vec, rand);
        }

        @Override
        public String toString() {
            return super.toString() + String.format(":%3.2f", sum());
        }
    }

    @Override
    public Object process(ThreadComp self, Object o) {
        o = ((ThreadCompVecSum) self).sum();
        return super.process(self, o);
    }

    public static double sum(double[] vec, Random rand) {
        double [] v = rand.doubles(vec.length).toArray();
        for (int i = 0, l = vec.length; i < l; ++i) {
            vec[i] += v[i];
        }
        return Arrays.stream(vec).average().orElse(0);
    }



    static class ReadActorVecSum extends ThreadTest.ReadActor {
        double[] vec = new double[vecSize];
        Random rand = new Random();
        public ReadActorVecSum(ActorSystem system, int n, ActorRef target) {
            super(system, n, target);
        }

        @Override
        void receive(Object o) {
            o = sum(vec, rand);
            super.receive(o);
        }

        @Override
        public String toString() {
            return super.toString() + String.format(":%3.2f", sum(vec, rand));
        }
    }
}
