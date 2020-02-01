package csl.actor.msgassoc;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConfigAggregationReplicable implements Serializable {
    public int mailboxThreshold = 1000;
    public int mailboxTreeSize = 32;
    public float lowerBoundThresholdFactor = 0.5f;
    public int minSizeOfEachMailboxSplit = 10;
    public float maxParallelRoutingThresholdFactor = 5.0f;
    public int historyEntrySize = 10;
    public float historyEntryLimitThresholdFactor = 0.1f;
    public float historyExceededLimitThresholdFactor = 0.3f;
    public float mergeRatioThreshold = 0.2f;
    public float pruneGreaterThanLeafThresholdFactor = 2f;
    public float pruneLessThanNonZeroLeafRate = 0.2f;
    public long toLocalWaitMs = 2000;
    public boolean logSplit = true;
    public int logColor = 33;

    public static ConfigAggregationReplicable readConfig(Map<Object, Object> properties) {
        return readConfig("csl.actor.msgassoc", properties);
    }

    public static ConfigAggregationReplicable readConfig(String propHead, Map<Object, Object> properties) {
        return new ConfigAggregationReplicable().read(propHead, properties);
    }

    public ConfigAggregationReplicable read(String propHead, Map<Object, Object> properties) {
        Arrays.stream(ConfigAggregationReplicable.class.getFields())
                .filter(ConfigAggregationReplicable::isConfigProperty)
                .forEach(f -> readConfigProperty(propHead, properties, f));
        return this;
    }

    public static boolean isConfigProperty(Field f) {
        return (Modifier.isPublic(f.getModifiers()) &&
                !Modifier.isStatic(f.getModifiers()) &&
                !Modifier.isSynchronized(f.getModifiers()) &&
                !f.getDeclaringClass().equals(Object.class));
    }

    public void readConfigProperty(String propHead, Map<Object, Object> properties,
                                          Field f) {

        String fld = f.getName();
        String propName = (propHead.isEmpty() ? "" : (propHead + ".")) + fld;
        Object v = properties.get(propName);
        if (v != null) {
            Class<?> type = f.getType();
            try {
                Object fv = null;
                if (type.equals(Integer.class) || type.equals(int.class)) {
                    if (v instanceof Number) {
                        fv = ((Number) v).intValue();
                    } else if (v instanceof String) {
                        fv = Integer.parseInt(((String) v).replaceAll("[_,]", ""));
                    } else {
                        throw new RuntimeException("invalid type");
                    }
                } else if (type.equals(Long.class) || type.equals(long.class)) {
                    if (v instanceof Number) {
                        fv = ((Number) v).longValue();
                    } else if (v instanceof String) {
                        fv = Long.parseLong(((String) v).replaceAll("[_,]", ""));
                    } else {
                        throw new RuntimeException("invalid type");
                    }
                } else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
                    if (v instanceof Boolean) {
                        fv = v;
                    } else if (v instanceof String) {
                        fv = v.equals("true");
                    } else {
                        throw new RuntimeException("invalid type");
                    }
                } else if (type.equals(Float.class) || type.equals(float.class)) {
                    if (v instanceof Number) {
                        fv = ((Number) v).floatValue();
                    } else if (v instanceof String) {
                        fv = Float.parseFloat(((String) v).replaceAll("[_,]", ""));
                    } else {
                        throw new RuntimeException("invalid type");
                    }
                } else if (type.equals(Double.class) || type.equals(double.class)) {
                    if (v instanceof Number) {
                        fv = ((Number) v).doubleValue();
                    } else if (v instanceof String) {
                        fv = Double.parseDouble(((String) v).replaceAll("[_,]", ""));
                    } else {
                        throw new RuntimeException("invalid type");
                    }
                } else if (type.equals(String.class)) {
                    fv = v.toString();
                }
                f.set(this, fv);
            } catch (Exception ex) {
                System.err.println(String.format("#failed config property: name=%s, requiredType=%s, valueType=%s, value=%s : %s",
                        propName,
                        type.getSimpleName(),
                        v.getClass().getSimpleName(),
                        v,
                        ex));
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + Integer.toHexString(System.identityHashCode(this)) + ") " + toStringConfig();
    }

    public String toStringConfig() {
        List<String> ls = Arrays.stream(ConfigAggregationReplicable.class.getFields())
                .filter(ConfigAggregationReplicable::isConfigProperty)
                .map(this::toStringConfigProperty)
                .collect(Collectors.toList());
        int maxHead = ls.stream()
                .mapToInt(l -> l.indexOf(':'))
                .max().orElse(0);

        return ls.stream()
                .map(l -> padding(l, maxHead))
                .collect(Collectors.joining(",\n  ", "{\n  ", "\n}"));
    }

    private static String padding(String s, int maxHead) {
        int n = s.indexOf(':');
        return s.substring(0, n) + IntStream.range(0, Math.max(0, maxHead - n))
                .mapToObj(i -> " ")
                .collect(Collectors.joining()) + s.substring(n);
    }

    public String toStringConfigProperty(Field f) {
        String head = "\"" + f.getName() + "\" : ";
        try {
            Object v = f.get(this);
            if (v instanceof Integer) {
                return String.format("%s%s", head, String.format("%,d", v).replaceAll("[,]", "_"));
            } else if (v instanceof Long) {
                return String.format("%s%s", head, String.format("%,d", v).replaceAll("[,]", "_"));
            } else if (v instanceof Boolean) {
                return String.format("%s%s", head, v);
            } else if (v instanceof Float) {
                return String.format("%s%s", head, String.format("%,f", v).replaceAll("[,]", "_"));
            } else if (v instanceof Double) {
                return String.format("%s%s", head, String.format("%,f", v).replaceAll("[,]", "_"));
            } else if (v instanceof String) {
                StringBuilder buf = new StringBuilder();
                buf.append(head);
                buf.append("\"");
                for (char c : ((String) v).toCharArray()) {
                    switch (c) {
                        case '\n': buf.append("\\n"); break;
                        case '\r': buf.append("\\r"); break;
                        case '\t': buf.append("\\t"); break;
                        case '\"': buf.append("\\\""); break;
                        case '\\': buf.append("\\\\"); break;
                        default:
                            buf.append(c);
                    }
                }
                buf.append("\"");
                return buf.toString();
            } else {
                return head + v;
            }
        } catch (Exception ex) {
            return head + "//error: " + ex;
        }
    }


}
