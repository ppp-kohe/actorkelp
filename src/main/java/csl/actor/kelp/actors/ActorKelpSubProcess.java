package csl.actor.kelp.actors;

import csl.actor.ActorBehaviorBuilder;
import csl.actor.ActorSystem;
import csl.actor.MessageBundle;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.behavior.ActorBehaviorBuilderKelp;
import csl.actor.util.PathModifier;

import java.io.*;
import java.util.*;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

public class ActorKelpSubProcess extends ActorKelp<ActorKelpSubProcess> {
    protected ProcessSource process;

    protected volatile String prevKey;
    protected volatile Process startedProcess;
    protected volatile Future<?> reading;
    protected volatile Writer writing;

    //shuffle constructor
    public ActorKelpSubProcess(ActorSystem system, String name, ConfigKelp config, Object consState) {
        super(system, name, config, consState);
    }

    public ActorKelpSubProcess(ActorSystem system, String name, ConfigKelp config, ProcessSource consState) {
        super(system, name, config, consState);
    }

    public ActorKelpSubProcess(ActorSystem system, String name, ConfigKelp config, Function<ProcessSource, ProcessSource> init) {
        this(system, name, config, init.apply(new ProcessSource()));
    }

    public ActorKelpSubProcess(ActorSystem system, ConfigKelp config, Function<ProcessSource, ProcessSource> init) {
        this(system, null, config, init.apply(new ProcessSource()));
        setNameRandom();
    }

    public ActorKelpSubProcess(ActorSystem system, Function<ProcessSource, ProcessSource> init) {
        this(system, null, null, init.apply(new ProcessSource()));
        setNameRandom();
    }


    @Override
    public Object getConstructionState() {
        return process;
    }

    @Override
    protected void initConstructionState(Object constructionState) {
        process = (ProcessSource) constructionState;
    }

    public static class ProcessSource implements Serializable {
        public static final long serialVersionUID = -1;
        public List<String> command = new ArrayList<>();
        public Map<String, String> env = new HashMap<>();
        public Map<String, String> envPaths = new HashMap<>();
        public String workingDirectory = ".";
        public ProcessInputTypeI inputType = ProcessInputType.KeyValueLine;
        public ProcessOutputTypeI outputType = ProcessOutputType.KeyValueLine;
        public ProcessSource() {}

        public Process start(ActorKelpSubProcess self) throws Exception {
            ProcessBuilder b = new ProcessBuilder()
                    .command(command)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .redirectOutput(ProcessBuilder.Redirect.PIPE)
                    .redirectInput(ProcessBuilder.Redirect.PIPE);
            Map<String, String> benv = b.environment();
            benv.putAll(env);
            PathModifier pm = PathModifier.getPathModifier(self.getSystem());
            envPaths.forEach((k,v) ->
                    benv.put(k, pm.expandPath(v)));
            b.directory(pm.getExpanded(workingDirectory).toFile());
            inputType.initProcessBuilder(self, b);
            outputType.initProcessBuilder(self, b);
            return b.start();
        }

        public ProcessSource addCommand(String... args) {
            command.addAll(Arrays.asList(args));
            return this;
        }
        public ProcessSource addCommand(Iterable<String> args) {
            args.forEach(command::add);
            return this;
        }

        public ProcessSource putEnv(String key, String value) {
            env.put(key, value);
            return this;
        }

        public ProcessSource putEnvAsExpandedPath(String key, String path) {
            envPaths.put(key, path);
            return this;
        }

        public ProcessSource directory(String path) {
            this.workingDirectory = path;
            return this;
        }

        public ProcessSource inputType(ProcessInputTypeI inputType) {
            this.inputType =inputType;
            return this;
        }

        public ProcessSource outputType(ProcessOutputTypeI outputType) {
            this.outputType = outputType;
            return this;
        }

    }

    public interface ProcessInputTypeI extends Serializable{
        default void initProcessBuilder(ActorKelpSubProcess self, ProcessBuilder builder) {}
        ActorBehaviorBuilder initBehavior(ActorKelpSubProcess self, ActorBehaviorBuilderKelp builder);
    }
    public interface ProcessOutputTypeI extends Serializable {
        default void initProcessBuilder(ActorKelpSubProcess self, ProcessBuilder builder) {}
        Consumer<String> initBehavior(ActorKelpSubProcess self);
    }

    public enum ProcessInputType implements ProcessInputTypeI {
        //TODO Py4j,

        /**
         * <ul>
         *     <li>{@link Map.Entry}: key, value</li>
         *     <li>{@link String}: <code>"key\tvalue"</code> (if no tab line, key will be empty "")</li>
         * </ul>
         * processed by {@link ActorKelpSubProcess#processKeyValue(String, Object)} :
         *    the value is formatted to a String by {@link ActorKelpSubProcess#processFormat(Object)},
         *    key and value are escaped with <code>\r \n \t \\</code>,
         *    and combined as the line <code>key\tvalue\n</code>.
         *    Also, as the special rule, the subsequent same key will be <code>\@</code> like
         *    <pre>
         *        hello \t v1
         *        hello \t v2
         *        hello \t v3
         *        =&gt;
         *        hello \t v1 \n
         *        \@    \t v2 \n
         *        \@    \t v3 \n
         *    </pre>
         */
        KeyValueLine {
            @Override
            public ActorBehaviorBuilder initBehavior(ActorKelpSubProcess self, ActorBehaviorBuilderKelp builder) {
                return builder
                        .matchKey(Map.Entry.class, self::getKey, Map.Entry::getValue)
                        .or(String.class, self::getKey, self::getValue)
                        .forEachKeyValue(self::processKeyValue);
            }
        },
        /**
         * the given data is immediately processed by {@link ActorKelpSubProcess#processLine(Object)}:
         *    the data is formatted by {@link ActorKelpSubProcess#processFormat(Object)} and escaped with <code>\r \n \t \\</code>,
         *    and combined as the line with append <code>\n</code>.
         */
        Line {
            @Override
            public ActorBehaviorBuilder initBehavior(ActorKelpSubProcess self, ActorBehaviorBuilderKelp builder) {
                return builder.matchAnyData(self::processLine);
            }
        },
        /**
         * the given data will immediately processed by {@link ActorKelpSubProcess#processDataNoEscape(Object)}:
         *    formatted by {@link ActorKelpSubProcess#processFormat(Object)} and just write it.
         */
        RawString {
            @Override
            public ActorBehaviorBuilder initBehavior(ActorKelpSubProcess self, ActorBehaviorBuilderKelp builder) {
                return builder.matchAnyData(self::processDataNoEscape);
            }
        };
        public abstract ActorBehaviorBuilder initBehavior(ActorKelpSubProcess self, ActorBehaviorBuilderKelp builder);
    }
    public enum ProcessOutputType implements ProcessOutputTypeI {
        /**
         *  {@link ActorKelpSubProcess#readFromProcessLine(String)}:
         *     unescape each line as <code>key\tvalue</code> and {@link Map.Entry}
         */
        KeyValueLine {
            @Override
            public Consumer<String> initBehavior(ActorKelpSubProcess self) {
                return self::readFromProcessLineKeyValue;
            }
        },
        Line {
            @Override
            public Consumer<String> initBehavior(ActorKelpSubProcess self) {
                return self::readFromProcessLine;
            }
        },
        RawLine {
            @Override
            public Consumer<String> initBehavior(ActorKelpSubProcess self) {
                return self::readFromProcessLineNoEscape;
            }
        },
        None {
            @Override
            public void initProcessBuilder(ActorKelpSubProcess self, ProcessBuilder builder) {
                builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            }

            @Override
            public Consumer<String> initBehavior(ActorKelpSubProcess self) {
                return null;
            }
        };
        public abstract Consumer<String> initBehavior(ActorKelpSubProcess self);
    }
    //////

    //////

    @Override
    protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilderKelp builder) {
        return process.inputType.initBehavior(this, builder);
    }
    public String getKey(Map.Entry<?,?> data) {
        Object v = data.getKey();
        return processFormat(v);
    }

    public String getKey(String data) {
        int i = data.indexOf('\t');
        if (i < 0) {
            return "";
        } else {
            return data.substring(0, i);
        }
    }
    public Object getValue(String data) {
        int i = data.indexOf('\t');
        if (i < 0) {
            return data;
        } else {
            return data.substring(i + 1);
        }
    }


    @Override
    public void processMessageBundle(MessageBundle<Object> mb) {
        prevKey = null;
        super.processMessageBundle(mb);
        prevKey = null;
    }

    public void processKeyValue(String key, Object value) {
        boolean samePrevKey = Objects.equals(prevKey, key);
        this.prevKey = key;
        String valueStr = processFormat(value);
        StringBuilder buf = new StringBuilder((int) ((key.length() + valueStr.length()) * 1.2));
        if (samePrevKey) {
            buf.append("\\@");
        } else {
            escape(buf, key);
        }
        buf.append('\t');
        escape(buf, valueStr);
        buf.append('\n');
        send(buf.toString());
    }

    public void processLine(Object data) {
        String str = processFormat(data);
        StringBuilder buf = new StringBuilder((int) (str.length() * 1.2));
        escape(buf, str);
        buf.append('\n');
        send(buf.toString());
    }
    public void processDataNoEscape(Object data) {
        send(processFormat(data));
    }

    public String processFormat(Object data) {
        return Objects.toString(data);
    }

    public static void escape(StringBuilder buf, String str) {
        for (char c : str.toCharArray()) {
            switch (c) {
                case '\t':
                    buf.append("\\t");
                    break;
                case '\n':
                    buf.append("\\n");
                    break;
                case '\r':
                    buf.append("\\r");
                    break;
                case '\\':
                    buf.append("\\\\");
                    break;
                default:
                    buf.append(c);
            }
        }
    }

    public static void unescape(StringBuilder buf, String str) {
        boolean afterQuote = false;
        for (char c : str.toCharArray()) {
            if (afterQuote) {
                switch (c) {
                    case '\n':
                        buf.append('\n');
                        break;
                    case '\r':
                        buf.append('\r');
                        break;
                    case '\\':
                        buf.append('\\');
                        break;
                    default:
                        buf.append('\\');
                        buf.append(c);
                        break;
                }
                afterQuote = false;
            } else {
                if (c == '\\') {
                    afterQuote = true;
                } else {
                    buf.append(c);
                }
            }
        }
        if (afterQuote) {
            buf.append('\\');
        }
    }

    public void send(String data) {
        try {
            if (startedProcess != null) {
                initProcess();
            }
            writing.write(data);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public synchronized void initProcess() throws Exception {
        startedProcess = process.start(this);
        Consumer<String> reader = process.outputType.initBehavior(this);
        if (reader != null) {
            reading = getSystem().getScheduledExecutor()
                    .submit(new ProcessReader(startedProcess, reader));
        }
        writing = new OutputStreamWriter(startedProcess.getOutputStream());
    }

    public static class ProcessReader implements Runnable {
        Process process;
        Consumer<String> behavior;

        public ProcessReader(Process process, Consumer<String> behavior) {
            this.process = process;
            this.behavior = behavior;
        }

        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;

                while ((line = reader.readLine()) != null) {
                    behavior.accept(line);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void readFromProcessLine(String line) {
        nextStageTell(unescape(line));
    }

    public void readFromProcessLineNoEscape(String line) {
        nextStageTell(line);
    }

    public void readFromProcessLineKeyValue(String line) {
        int i = line.indexOf('\t');
        String key;
        String value;
        if (i < 0) {
            key = "";
            value = line;
        } else {
            key = line.substring(0, i);
            value = line.substring(i + 1);
        }
        readFromProcess(Map.entry(unescape(key), unescape(value)));
    }

    private String unescape(String v) {
        StringBuilder buf = new StringBuilder((int) (v.length() * 1.2));
        unescape(buf, v);
        return buf.toString();
    }

    public void readFromProcess(Map.Entry<String, String> e) {
        nextStageTell(e);
    }

    @Override
    public void flush() {
        super.flush();
        synchronized (this) {
            Process p = startedProcess;
            if (p != null && !p.isAlive()) {
                startedProcess = null;
                reading = null;
                writing = null;
            }
            if (writing != null) {
                try {
                    writing.flush();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    @Override
    public void close() {
        super.close();
        stageEnd();
    }

    @Override
    public void stageEnd() {
        flush();
        clearProcess();
    }

    public synchronized void clearProcess() {
        Process p = startedProcess;
        if (p != null) {
            p.destroy();
            startedProcess = null;
            reading = null;
            writing = null;
        }
    }
}
