package csl.actor.kelp.behavior;

import csl.actor.ActorRef;
import csl.actor.Message;
import csl.actor.kelp.ActorKelpFunctions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * suppose each {@link DispatchRef} is used only within an actor instance.
 * So it can be stateful without considering thread synchronization
 */
public interface KelpDispatcher extends Serializable {
    /**
     * a dispatcher is created for each {@link DispatchRef}, and it can be a stateful object.
     * @return a copy (or this if stateless)
     */
    default KelpDispatcher copy() {
        return this;
    }

    void dispatch(DispatchRef ref, ActorKelpFunctions.KeyExtractor<?, Object> keyExtractor, Message<?> message);


    static void tellMessageShuffle(KelpDispatcher.DispatchRef self, List<KelpDispatcher.SelectiveDispatcher> selectiveDispatchers,
                                          Message<?> message) {
        for (KelpDispatcher.SelectiveDispatcher e : selectiveDispatchers) {
            if (e.dispatch(self, message)) {
                return;
            }
        }
        DEFAULT_DISPATCHER.dispatch(self, ActorKelpFunctions.DEFAULT_KEY_EXTRACTOR, message);
    }

    KelpDispatcher.DispatcherShuffle DEFAULT_DISPATCHER = new KelpDispatcher.DispatcherShuffle();

    interface DispatchRef {
        List<? extends DispatchUnit> getDispatchUnits();
        int getDispatchUnitSize();
        DispatchUnit getDispatchUnit(int index);
    }

    interface DispatchUnit extends ActorRef {
        int getIndex();
        void flush(ActorRef sender);

        default void flush() {
            flush(null);
        }
    }

    class SelectiveDispatcher implements Serializable, Cloneable {
        public static final long serialVersionUID = -1;
        public List<ActorKelpFunctions.KeyExtractor<?,?>> keyExtractors;
        public KelpDispatcher dispatcher;

        public SelectiveDispatcher() {}

        public SelectiveDispatcher(List<ActorKelpFunctions.KeyExtractor<?, ?>> keyExtractors, KelpDispatcher dispatcher) {
            this.keyExtractors = keyExtractors;
            this.dispatcher = dispatcher;
        }

        public KelpDispatcher getDispatcher() {
            return dispatcher;
        }

        public List<ActorKelpFunctions.KeyExtractor<?, ?>> getKeyExtractors() {
            return keyExtractors;
        }

        public SelectiveDispatcher copy() {
            try {
                SelectiveDispatcher d = (SelectiveDispatcher) super.clone();
                d.keyExtractors = new ArrayList<>(keyExtractors);
                d.dispatcher = dispatcher.copy();
                return d;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings({"unchecked"})
        public boolean dispatch(DispatchRef ref, Message<?> message) {
            Object data = message.getData();
            for (ActorKelpFunctions.KeyExtractor<?,?> ke : keyExtractors) {
                if (ke.matchValue(data)) {
                    dispatcher.dispatch(ref, (ActorKelpFunctions.KeyExtractor<?, Object>) ke, message);
                    return true;
                }
            }
            return false;
        }
    }

    class DispatcherAll implements KelpDispatcher {
        public static final long serialVersionUID = -1;
        @Override
        public void dispatch(DispatchRef ref, ActorKelpFunctions.KeyExtractor<?, Object> keyExtractor, Message<?> message) {
            ref.getDispatchUnits().forEach(u ->
                    u.tellMessage(message));
        }
    }

    class DispatcherShuffle implements KelpDispatcher {
        public static final long serialVersionUID = -1;
        @Override
        public void dispatch(DispatchRef ref, ActorKelpFunctions.KeyExtractor<?, Object> keyExtractor, Message<?> message) {
            int size = ref.getDispatchUnitSize();
            if (size != 0) {
                DispatchUnit entry;
                if (size == 1) {
                    entry = ref.getDispatchUnit(0);
                } else {
                    int hash = hashMessage(keyExtractor, message);
                    int index = hashMod(hash, size);
                    entry = ref.getDispatchUnit(index);
                }
                entry.tellMessage(message);
            }
        }

        protected int hashMessage(ActorKelpFunctions.KeyExtractor<?, Object> keyExtractor, Message<?> message) {
            return hash(keyExtractor.toKey(message.getData()));
        }

        protected int hash(Object key) {
            return Objects.hashCode(key);
        }

        /**
         * @param hash hashCode
         * @param size a max+1 value, &gt;= 0 , usually relatively smaller than hash
         * @return the index within size
         */
        public static int hashMod(int hash, int size) {
            if (size <= 1) {
                return 0;
            } else {
                int h = hash;
                int sh = Integer.highestOneBit(size);
                int sizeWidth = Integer.numberOfTrailingZeros(sh);
                //max of sizeWidth is 30
                int result = 0;
                int remainingBits = 32;
                while (remainingBits > 0) {
                    result ^= h;
                    h >>>= sizeWidth;
                    remainingBits -= sizeWidth;
                }
                return Math.abs(result % size);
            }
        }
    }

    class DispatcherRandomOne implements KelpDispatcher, Cloneable {
        public static final long serialVersionUID = -1;
        public Random random;

        public DispatcherRandomOne() {
            this.random = new Random();
        }

        public DispatcherRandomOne(Random random) {
            this.random = random;
        }

        @Override
        public KelpDispatcher copy() {
            try {
                DispatcherRandomOne d = (DispatcherRandomOne) super.clone();
                d.random = new Random();
                return d;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void dispatch(DispatchRef ref, ActorKelpFunctions.KeyExtractor<?, Object> keyExtractor, Message<?> message) {
            ref.getDispatchUnit(random.nextInt(ref.getDispatchUnitSize()))
                    .tellMessage(message);
        }
    }

    class DispatcherRandomPoisson1 extends DispatcherRandomOne {
        public static final long serialVersionUID = -1;
        public DispatcherRandomPoisson1() { }

        public DispatcherRandomPoisson1(Random random) {
            super(random);
        }

        @Override
        public void dispatch(DispatchRef ref, ActorKelpFunctions.KeyExtractor<?, Object> keyExtractor, Message<?> message) {
            for (DispatchUnit u : ref.getDispatchUnits()) {
                if (nextBoolean()) {
                    u.tellMessage(message);
                }
            }
        }

        boolean nextBoolean() {
            return random.nextDouble() > POISSON_1_0;
        }
    }

    double POISSON_1_0 = Math.exp(-1); //k=0,lambda=1.0

    static int poisson(double lambda, Random random) {
        if (lambda < POISSON_LIMIT) {
            double prevStep = 1.0;
            double sum = prevStep;
            double p = random.nextDouble() * Math.exp(lambda);
            int i = 1;
            int max = 10 * (int) Math.ceil(lambda);
            while (i < max && sum <= p) {
                prevStep *= lambda / i;
                sum += prevStep;
                ++i;
            }
            return i - 1;
        } else {
            return (int) Math.floor(Math.max(0, lambda + Math.sqrt(lambda) * random.nextGaussian()));
        }
    }

    double POISSON_LIMIT = 100;

}
