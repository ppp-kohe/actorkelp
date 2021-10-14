package csl.example.exp;

import csl.actor.ActorRef;
import csl.actor.ActorSystem;

import java.util.Arrays;
import java.util.Random;

public class ExpThreadTestVecSum extends ExpThreadTest {
    public static void main(String[] args) {
        new ExpThreadTestVecSum().run(args);
    }

    @Override
    public ExpThreadTest create() {
        return new ExpThreadTestVecSum();
    }

    @Override
    protected ThreadComp createThreadComp(int th, String title) {
        return new ThreadCompVecSum(th, title, config.vecLen);
    }

    @Override
    ReadActor newReadActor(ActorSystem sys, int r, ActorRef fa) {
        return new ReadActorVecSum(sys, r, fa, config.vecLen);
    }

    static class ThreadCompVecSum extends ThreadComp {
        double[] vec;
        Random rand = new Random();
        public ThreadCompVecSum(int th, String title, int vecSize) {
            super(th, title);
            vec = new double[vecSize];
        }

        double sum() {
            return ExpThreadTestVecSum.sum(vec, rand);
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



    static class ReadActorVecSum extends ExpThreadTest.ReadActor {
        double[] vec;
        Random rand = new Random();
        public ReadActorVecSum(ActorSystem system, int n, ActorRef target, int vecSize) {
            super(system, n, target);
            vec = new double[vecSize];
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
