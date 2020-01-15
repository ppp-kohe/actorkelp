package csl.actor.msgassoc;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class KeyHistograms {

    public interface Histogram {
        void put(Object v);
        Comparable<?> findDividingPoint();
        int compareToDividingPoint(Object v1, Comparable<?> v2);

        Histogram create();
    }

    public static class HistogramComparable implements Histogram {
        protected long total;
        protected TreeMap<Comparable<?>, Long> counts = new TreeMap<>();

        @Override
        public void put(Object v) {
            total++;
            counts.compute((Comparable<?>) v, (k,count) -> count == null ? 0L : (count + 1L));
        }

        public Comparable<?> findDividingPoint() {
            long acc = 0;

            long half = total / 2L;

            Comparable<?> last = null;
            for (Map.Entry<Comparable<?>, Long> e : counts.entrySet()) {
                last = e.getKey();
                acc += e.getValue();
                if (acc >= half) {
                    return last;
                }
            }
            return last;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public int compareToDividingPoint(Object v1, Comparable<?> v2) {
            return ((Comparable) v1).compareTo(v2);
        }

        @Override
        public Histogram create() {
            return new HistogramComparable();
        }
    }

    public static class HistogramNonComparable extends HistogramComparable {
        @Override
        public void put(Object v) {
            Comparable<?> hc = toKey(v);
            super.put(hc);
        }

        public Comparable<?> toKey(Object v) {
            return Objects.hashCode(v);
        }

        @Override
        public int compareToDividingPoint(Object v1, Comparable<?> v2) {
            return super.compareToDividingPoint(toKey(v1), v2);
        }

        @Override
        public Histogram create() {
            return new HistogramNonComparable();
        }
    }
}
