package csl.actor.msgassoc;

import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Config implements Serializable {
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
    public long traverseDelayTimeMs = 300;
    public String persistMailboxPath = "";
    public long persistMailboxSizeLimit = Integer.MAX_VALUE / 64;
    public long persistMailboxOnMemorySize = 100_000L;
    public int reduceRuntimeCheckingThreshold = 100_000;
    public double reduceRuntimeRemainingBytesToSizeRatio = 0.003;

    public int histogramPersistHistoryEntrySize = 10;
    public int histogramPersistHistoryEntryLimit = 100;
    public long histogramPersistSizeLimit = 1000;
    public long histogramPersistOnMemorySize = 100;
    public double histogramPersistSizeRatioThreshold = 0.00001;
    public long histogramPersistRandomSeed = 0;

    protected PrintWriter logOut;

    public static Config readConfig(Map<Object, Object> properties) {
        return readConfig("csl.actor.msgassoc", properties);
    }

    public static Config readConfig(String propHead, Map<Object, Object> properties) {
        return new Config().read(propHead, properties);
    }

    public Config read(String propHead, Map<Object, Object> properties) {
        Arrays.stream(getClass().getFields())
                .filter(Config::isConfigProperty)
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
                readProperty(f, v);
            } catch (Exception ex) {
                log(String.format("#failed config property: name=%s, requiredType=%s, valueType=%s, value=%s : %s",
                        propName,
                        type.getSimpleName(),
                        v.getClass().getSimpleName(),
                        v,
                        ex));
            }
        }
    }

    public void readProperty(Field f, Object v) throws Exception {
        Class<?> type = f.getType();
        f.set(this, readValue(type, v));
    }

    public Object readValue(Class<?> type, Object v) {
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
        return fv;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + Integer.toHexString(System.identityHashCode(this)) + ") " + toStringConfig();
    }

    public String toStringConfig() {
        List<String> ls = Arrays.stream(getClass().getFields())
                .filter(Config::isConfigProperty)
                .sorted(getPropertyFieldComparator())
                .map(this::toStringConfigProperty)
                .collect(Collectors.toList());
        return toStringPadding(":", ls).stream()
                .collect(Collectors.joining(",\n  ", "{\n  ", "\n}"));
    }

    public List<String> toStringPadding(String delim, List<String> ls) {
        int maxHead = ls.stream()
                .mapToInt(l -> l.indexOf(delim))
                .max().orElse(0);
        return ls.stream()
                .map(l -> padding(l, maxHead, delim))
                .collect(Collectors.toList());
    }

    private static String padding(String s, int maxHead, String delim) {
        int n = s.indexOf(delim);
        return s.substring(0, n) + IntStream.range(0, Math.max(0, maxHead - n))
                .mapToObj(i -> " ")
                .collect(Collectors.joining()) + s.substring(n);
    }

    public String toStringConfigProperty(Field f) {
        return "\"" + f.getName() + "\" : " + toStringConfigPropertyValue(f);
    }

    public String toStringConfigPropertyValue(Field f) {
        try {
            Object v = f.get(this);
            if (v instanceof Integer) {
                return String.format("%,d", v).replaceAll("[,]", "_");
            } else if (v instanceof Long) {
                return String.format("%,d", v).replaceAll("[,]", "_");
            } else if (v instanceof Boolean) {
                return "" + v;
            } else if (v instanceof Float) {
                return String.format("%,f", v).replaceAll("[,]", "_");
            } else if (v instanceof Double) {
                return String.format("%,f", v).replaceAll("[,]", "_");
            } else if (v instanceof String) {
                StringBuilder buf = new StringBuilder();
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
                return "" + v;
            }
        } catch (Exception ex) {
            return "//error: " + ex;
        }
    }

    public List<String> readArgs(String... args) {
        return readArgs(Arrays.asList(args));
    }

    public List<String> readArgs(List<String> args) {
        List<String> rest = new ArrayList<>();
        List<CommandArgument> com = getCommandArguments();
        for (int i = 0; i < args.size();) {
            String arg = args.get(i);
            if (arg.equals("--help")) {
                rest.add("--help");
                break;
            } else {
                int fromIndex = i + 1;
                int nextIndex = com.stream()
                        .filter(c -> c.argument.equals(arg) || c.abbrev.equals(arg))
                        .findFirst()
                        .map(c -> readArg(fromIndex, args, c))
                        .orElse(i);
                if (nextIndex <= i) {
                    rest.add(arg);
                    ++i;
                } else {
                    i = nextIndex;
                }
            }
        }
        return rest;
    }

    public int readArg(int index, List<String> args, CommandArgument comArg) {
        try {
            readProperty(comArg.field, args.get(index));
            return index + 1;
        } catch (Exception ex) {
            log("#failed parsing argument at: " + index + " for property: " + comArg.argument + " : " + ex);
            return index + 1;
        }
    }

    public void showHelp() {
        println(getLogOut(), helpString());
    }

    public String helpString() {
        return getClass().getName() + ":\n" +
                toStringPadding(":",
                        getCommandArguments().stream()
                            .map(this::helpString)
                            .collect(Collectors.toList())).stream()
                .collect(Collectors.joining("\n    ", "    ", ""));
    }

    public String helpString(CommandArgument arg) {
        return arg.argument + (arg.abbrev.isEmpty() ? "" : ("|" + arg.abbrev))
                + " " + helpStringArgumentType(arg.type) + " : " + (arg.help) +
                " default: " + toStringConfigPropertyValue(arg.field);
    }

    public String helpStringArgumentType(Class<?> type) {
        return "<" + type.getSimpleName() + ">";
    }

    public List<CommandArgument> getCommandArguments() {
        List<CommandArgument> args = new ArrayList<>();
        for (Field field : getClass().getFields()) {
            if (isConfigProperty(field)) {
                CommandArgument arg = new CommandArgument(field);
                CommandArgumentOption opt = field.getAnnotation(CommandArgumentOption.class);
                if (opt != null) {
                    arg.argument = opt.value();
                    arg.abbrev = opt.abbrev();
                    arg.help = opt.help();
                }
                if (arg.argument.isEmpty()) {
                    arg.argument = "--" + field.getName();
                }
                arg.type = field.getType();
                if (arg.type.isPrimitive()) {
                    if (arg.type.equals(int.class)) {
                        arg.type = Integer.class;
                    } else if (arg.type.equals(long.class)) {
                        arg.type = Long.class;
                    } else if (arg.type.equals(boolean.class)) {
                        arg.type = Boolean.class;
                    } else if (arg.type.equals(float.class)) {
                        arg.type = Float.class;
                    } else if (arg.type.equals(double.class)) {
                        arg.type = Double.class;
                    }
                }
                args.add(arg);
            }
        }
        Comparator<Field> f = getPropertyFieldComparator();
        args.sort((l,r) -> f.compare(l.field, r.field));
        return args;
    }

    public Comparator<Field> getPropertyFieldComparator() {
        Class<?> c = getClass();
        List<Class<?>> configs = new ArrayList<>();
        while (!c.equals(Config.class)) {
            configs.add(c);
            c = c.getSuperclass();
        }
        Collections.reverse(configs);
        return (l,r) -> {
            int i = Integer.compare(configs.indexOf(l.getDeclaringClass()), configs.indexOf(r.getDeclaringClass()));
            if (i != 0) {
                return i;
            } else {
                return l.getName().compareTo(r.getName());
            }
        };
    }

    protected static class CommandArgument {
        public String argument;
        public String abbrev;
        public String help;
        public Class<?> type;
        public Field field;

        public CommandArgument(Field field) {
            this.argument = "";
            this.abbrev = "";
            this.help = "";
            type = String.class;
            this.field = field;
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface CommandArgumentOption {
        String value() default "";
        String abbrev() default "";
        String help() default "";
    }

    public void log(String msg) {
        PrintWriter out = getLogOut();
        println(out, String.format("!!! [%s] %s", Instant.now(), msg));
    }

    public PrintWriter getLogOut() {
        PrintWriter out = logOut;
        if (out == null) {
            logOut = new PrintWriter(System.err, true);
            out = logOut;
        }
        return out;
    }

    public void println(PrintWriter out, String line) {
        out.println(toConsoleLine(line));
    }

    public String toConsoleLine(String line) {
        int c = logColor;
        if (c > -1) {
            return String.format("\033[38;5;%dm%s\033[0m", c, line);
        } else {
            return line;
        }
    }
}
