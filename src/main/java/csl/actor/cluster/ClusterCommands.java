package csl.actor.cluster;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ClusterCommands<AppConfType extends ConfigBase> {
    protected Class<AppConfType> defaultConfType;

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        ClusterCommands<ConfigBase> c = new ClusterCommands<>((Class<ConfigBase>) Class.forName(args[0]));
        CommandBlockRoot root = c.parseConfigFile(args[1]);
        List<ClusterUnit<ConfigBase>> units = c.loadNamed(root);
        root.write(System.out::println);

        units.forEach(u -> {
            u.log(u.getName() + ":");
            u.log("    clusterConfig: " + u.getDeploymentConfig().toString());
            if (u.getAppConfig() != null) {
                u.log("    appConfig: " + u.getAppConfig().toString());
            }
        });
    }

    public ClusterCommands(Class<AppConfType> defaultConfType) {
        this.defaultConfType = defaultConfType;
    }

    public List<ClusterUnit<AppConfType>> loadConfigFile(String path) {
        CommandBlockRoot root = parseConfigFile(path);
        return loadNamed(root);
    }

    public List<ClusterUnit<AppConfType>> loadNamed(CommandBlockRoot root) {
        return root.getNameToBlock().values().stream()
                .filter(c -> !c.isClassType())
                .map(this::load)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    public ClusterUnit<AppConfType> load(CommandBlock block) {
        ClusterUnit<AppConfType> unit = new ClusterUnit<>();
        ConfigDeployment conf = new ConfigDeployment(defaultConfType);
        unit.setDeploymentConfig(conf);
        try {
            if (block instanceof CommandBlockNamed) {
                String name = ((CommandBlockNamed) block).getName().getData();
                unit.setName(name);
                if (block.getConfigLines().stream()
                        .noneMatch(cs -> cs.get(0).getData().equals("host"))) {
                    conf.set("host", name);
                }
            }
            unit.setBlock(block);

            block.getClusterLines()
                    .forEach(cs -> set(conf, cs));
            unit.setAppConfig((AppConfType) Class.forName(conf.configType).getConstructor()
                    .newInstance());

            unit.setAppConfigLogHeader();

            block.getConfigLines()
                    .forEach(cs -> set(unit.getAppConfig(), cs));
        } catch (Exception e) {
            conf.log(e, "appConfig error: %s : %s", conf.configType);
        }
        return unit;
    }


    protected void set(ConfigBase conf, List<CommandToken> cs) {
        try {
            String name = cs.get(0).getData();
            if (name.equals("extends")) {
                return;
            }
            Field fld = conf.getClass().getField(name);
            String data = cs.subList(1, cs.size()).stream()
                    .map(CommandToken::getData)
                    .collect(Collectors.joining(" "));
            conf.set(fld.getName(), data);
        } catch (Exception e) {
            conf.log("property error: " + conf + " : " + cs + " : " + e);
        }
    }

    public CommandBlockRoot parseConfigFile(String path) {
        try {
            return parseConfig(Files.readAllLines(Paths.get(path)));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public CommandBlockRoot parseConfig(Iterable<String> lines) {
        CommandBlockRoot root = new CommandBlockRoot();
        CommandBlock state = root;
        for (String line : lines) {
            List<CommandToken> tokens = parse(line);
            state = state.parse(new LinkedList<>(tokens));
        }
        root.link();
        return root;
    }

    public List<CommandToken> parse(String line) {
        List<CommandToken> tokens = new ArrayList<>();
        CommandLineParser parser = new CommandLineParser(line);
        tokens.add(new CommandToken(CommandTokenType.Indent, parser.eatWhitespaces()));

        if (parser.peek() == '#') {
            tokens.add(new CommandToken(CommandTokenType.LineEnd, parser.eatToEnd()));
        }

        while (parser.hasNext()) {
            char c = parser.peek();
            if (c == '#') {
                tokens.add(new CommandToken(CommandTokenType.LineEnd, parser.eatToEnd()));
            } else if (c == '"') {
                tokens.add(new CommandToken(CommandTokenType.String, parser.eatStringLiteral()));
            } else if (c == ':') {
                tokens.add(new CommandToken(CommandTokenType.Colon, "" + parser.eat()));
            } else if ('0' <= c && c <= '9') {
                tokens.add(new CommandToken(CommandTokenType.Number, parser.eatNonWhitespace()));
            } else {
                tokens.add(new CommandToken(CommandTokenType.Identifier, parser.eatNonWhitespace()));
            }

            if (parser.hasNext() && Character.isWhitespace(parser.peek())) {
                parser.eatWhitespaces();
            }
        }
        tokens.add(new CommandToken(CommandTokenType.LineEnd, ""));
        return tokens;
    }


    public static class CommandLineParser {
        protected String line;
        protected int index;

        public CommandLineParser(String line) {
            this.line = line;
        }

        public int getIndex() {
            return index;
        }

        public boolean hasNext() {
            return index < line.length();
        }

        public char peek() {
            if (hasNext()) {
                return line.charAt(index);
            } else {
                return 0;
            }
        }

        public char eat() {
            char c = peek();
            if (hasNext()) {
                ++index;
            }
            return c;
        }

        public String eatWhitespaces() {
            StringBuilder buf = new StringBuilder();
            while (hasNext() && Character.isWhitespace(peek())) {
                buf.append(eat());
            }
            return buf.toString();
        }

        public String eatNonWhitespace() {
            StringBuilder buf = new StringBuilder();
            while (hasNext()) {
                char c = peek();
                if (Character.isWhitespace(c) ||
                    c == '#' || c == ':') {
                    break;
                }
                buf.append(eat());
            }
            return buf.toString();
        }

        public String eatToEnd() {
            StringBuilder buf = new StringBuilder();
            while (hasNext()) {
                buf.append(eat());
            }
            return buf.toString();
        }

        public String eatStringLiteral() {
            if (hasNext() && peek() == '"') {
                eat();
                StringBuilder buf = new StringBuilder();
                while (hasNext() && peek() != '"') {
                    char s = eat();
                    if (s == '\\') {
                        s = eat();
                        if (s == 'n') {
                            buf.append("\n");
                        } else if (s == 'r') {
                            buf.append("\r");
                        } else if (s == 't') {
                            buf.append("\t");
                        } else if (s == 'f') {
                            buf.append("\f");
                        } else if (s == 'b') {
                            buf.append("\b");
                        } else if (s == '\\') {
                            buf.append("\\");
                        } else if (s == '\'') {
                            buf.append("'");
                        } else if (s == '\"') {
                            buf.append("\"");
                        } else if (s == 'u') {
                            char u1 = eat();
                            char u2 = eat();
                            char u3 = eat();
                            char u4 = eat();
                            if (isHex(u1) && isHex(u2) && isHex(u3) && isHex(u4)) {
                                buf.append((char) Integer.parseInt("" + u1 + u2 + u3 + u4, 16));
                            } else {
                                throw new RuntimeException("pos:" + (getIndex() - 1) + " line:" + line);
                            }
                        } else {
                            throw new RuntimeException("pos:" + (getIndex() - 1) + " line:" + line);
                        }
                    } else {
                        buf.append(s);
                    }
                }
                eat();
                return buf.toString();
            } else {
                return null;
            }
        }

        private boolean isHex(char c) {
            c = Character.toLowerCase(c);
            return ('0' <= c && c <= '9') || (c == 'a' || c == 'b' || c == 'c' || c == 'd' || c == 'e' || c == 'f');
        }
    }

    public static class CommandToken implements Serializable {
        public static final long serialVersionUID = 1L;
        protected CommandTokenType type;
        protected String data;

        public CommandToken(CommandTokenType type, String data) {
            this.type = type;
            this.data = data;
        }

        public CommandTokenType getType() {
            return type;
        }

        public String getData() {
            return data;
        }

        @Override
        public String toString() {
            return "(" + type  + "," + data  + ")";
        }

        public String toSource() {
            if (type.equals(CommandTokenType.String)) {
                StringBuilder buf = new StringBuilder();
                buf.append("\"");
                for (char c : data.toCharArray()) {
                    if (c == '\n') {
                        buf.append("\\").append("n");
                    } else if (c == '\t') {
                        buf.append("\\").append("t");
                    } else if (c == '\f') {
                        buf.append("\\").append("f");
                    } else if (c == '\r') {
                        buf.append("\\").append("r");
                    } else if (c == '\b') {
                        buf.append("\\").append("b");
                    } else if (c == '\"') {
                        buf.append("\\").append("\"");
                    } else if (c == '\\') {
                        buf.append("\\").append("\\");
                    } else if (Character.isISOControl(c)) {
                        buf.append(String.format("\\u%04x", (int) c));
                    } else {
                        buf.append(c);
                    }
                }
                buf.append("\"");
                return buf.toString();
            } else {
                return data;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CommandToken that = (CommandToken) o;
            return type == that.type &&
                    Objects.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, data);
        }
    }

    public enum CommandTokenType {
        Indent,
        Identifier,
        String,
        Number,
        Colon,
        LineEnd
    }

    public static class CommandBlockRoot extends CommandBlock {
        public static final long serialVersionUID = 1L;
        protected Map<String, CommandBlockNamed> nameToBlock = new LinkedHashMap<>();
        protected List<CommandBlock> blocksAll = new ArrayList<>();

        public Map<String, CommandBlockNamed> getNameToBlock() {
            return nameToBlock;
        }

        public List<CommandBlock> getBlocksAll() {
            return blocksAll;
        }

        public void link() {
            collect(this);
            blocksAll.forEach(this::link);
            blocksAll.forEach(this::inherit);
        }

        public void collect(CommandBlock block) {
            if (block instanceof CommandBlockNamed) {
                CommandBlockNamed n = (CommandBlockNamed) block;
                String name = n.getName().getData();
                if (nameToBlock.containsKey(name)) {
                    throw new RuntimeException("error duplicated name: " + name + " : " + nameToBlock.get(name) + " vs " + block);
                }
                nameToBlock.put(name, n);
            }
            blocksAll.add(block);
            block.getBlocks()
                    .forEach(this::collect);
        }

        public void link(CommandBlock block) {
            block.getClusterLines().stream()
                    .filter(cs -> cs.get(0).getData().equals("extends"))
                    .map(cs -> nameToBlock.get(cs.get(1).getData()))
                    .findFirst()
                    .ifPresent(block::setSuperBlock);
        }

        public void inherit(CommandBlock block) {
            inherit(block, block.getSuperBlock(), Collections.singletonList(block));
        }

        public void inherit(CommandBlock block, CommandBlock nextSuper, List<CommandBlock> history) {
            if (nextSuper != null) {
                if (history.contains(nextSuper)) { //cyclic
                    throw new RuntimeException("cyclic extends: " + nextSuper + " : " + history);
                }
                List<CommandBlock> nextHistory = new ArrayList<>(history);
                nextHistory.add(nextSuper);

                inheritCommand(true, block, nextSuper);
                inheritCommand(false, block, nextSuper);

                inherit(block, nextSuper.getSuperBlock(), nextHistory);
            }
        }

        protected void inheritCommand(boolean cluster, CommandBlock block, CommandBlock nextSuper) {
            Function<CommandBlock,List<List<CommandToken>>> lineGetter = cluster ? CommandBlock::getClusterLines : CommandBlock::getConfigLines;
            lineGetter.apply(nextSuper)
                    .forEach(cs -> inheritCommand(cluster,
                            lineGetter.apply(block),
                            findCommand(lineGetter.apply(block), cs),
                            cs));
        }

        protected void inheritCommand(boolean cluster, List<List<CommandToken>> lines, List<CommandToken> existing, List<CommandToken> cs) {
            if (existing == null) {
                if (cluster && match(cs, "extends")) {
                    return; //skip
                }
                lines.add(cs);
            }
        }

        private List<CommandToken> findCommand(List<List<CommandToken>> csLines, List<CommandToken> cs) {
            return cs.size() > 1 ? csLines.stream()
                    .filter(ecs -> ecs.get(0).getData().equals(cs.get(0).getData()))
                    .findFirst()
                    .orElse(null) :
                    Collections.emptyList();
        }
    }

    public static class CommandBlock  implements Serializable {
        public static final long serialVersionUID = 1L;
        protected List<List<CommandToken>> clusterLines = new ArrayList<>();
        protected List<List<CommandToken>> configLines = new ArrayList<>();
        protected List<CommandBlock> blocks = new ArrayList<>();
        protected CommandBlock superBlock;

        public List<List<CommandToken>> getClusterLines() {
            return clusterLines;
        }

        public List<List<CommandToken>> getConfigLines() {
            return configLines;
        }

        public List<CommandBlock> getBlocks() {
            return blocks;
        }

        public CommandBlock getSuperBlock() {
            return superBlock;
        }

        public void setSuperBlock(CommandBlock superBlock) {
            this.superBlock = superBlock;
        }

        public CommandBlock parse(LinkedList<CommandToken> tokens) {
            CommandToken indent = new CommandToken(CommandTokenType.Indent, "");
            if (match(tokens, CommandTokenType.Indent)) {
                indent = tokens.removeFirst();
            }
            return parse(indent, tokens);
        }

        public CommandBlock parse(CommandToken indent, LinkedList<CommandToken> tokens) {
            if (match(tokens, CommandTokenType.Identifier)) {
                CommandToken next = tokens.removeFirst();
                if (next.getData().equals("class") || next.getData().equals("node")) {
                    if (match(tokens,
                            EnumSet.of(CommandTokenType.Identifier, CommandTokenType.String),
                            CommandTokenType.Colon,
                            CommandTokenType.LineEnd)) {
                        CommandToken name = tokens.removeFirst();
                        CommandBlockNamed sub = new CommandBlockNamed(indent, next.getData().equals("class"), name, this);
                        blocks.add(sub);
                        return sub;
                    } else {
                        throw new RuntimeException("error: " + next + " " + tokens);
                    }
                } else {
                    List<CommandToken> commands = new ArrayList<>();
                    boolean cluster = isClusterCommand(next.getData());
                    commands.add(next);
                    boolean lineContinue = false;
                    CommandToken lineSepToken = new CommandToken(CommandTokenType.Identifier, "\\");
                    while (!tokens.isEmpty() && !(match(tokens, CommandTokenType.LineEnd))) {
                        CommandToken arg = tokens.removeFirst();
                        if (arg.equals(lineSepToken)) {
                            lineContinue = true;
                            break;

                        }
                        commands.add(arg);
                    }
                    if (cluster) {
                        this.clusterLines.add(commands);
                    } else {
                        this.configLines.add(commands);
                    }
                    if (lineContinue) {
                        return new CommandBlockLineContinue(this, commands);
                    } else {
                        return this;
                    }
                }
            } else if (match(tokens, CommandTokenType.LineEnd)) {
                return this;
            } else {
                throw new RuntimeException("error: " + tokens);
            }
        }

        static List<String> clusterCommands;

        public static boolean isClusterCommand(String data) {
            if (clusterCommands == null) {
                clusterCommands = Arrays.stream(ConfigDeployment.class.getFields())
                        .filter(ConfigBase::isConfigProperty)
                        .map(Field::getName)
                        .collect(Collectors.toList());
            }
            return data.equals("extends") ||
                    clusterCommands.contains(data);
        }

        public boolean match(List<CommandToken> tokens, Object... patterns) {
            if (tokens.size() >= patterns.length) {
                for (int i = 0; i < patterns.length; ++i) {
                    CommandToken t = tokens.get(i);
                    if (!matchToken(t, patterns[i])) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        }

        @SuppressWarnings("unchecked")
        public boolean matchToken(CommandToken token, Object pat) {
            if (pat instanceof CommandTokenType) {
                return token.getType().equals(pat);
            } else if (pat instanceof Collection<?>) {
                return ((Collection<?>) pat).contains(token.getType());
            } else if (pat instanceof Pattern) {
                return ((Pattern) pat).matcher(token.getData()).matches();
            } else if (pat instanceof String) {
                return token.getData().equals(pat);
            } else if (pat instanceof Predicate<?>) {
                return ((Predicate<Object>) pat).test(token);
            } else {
                throw new RuntimeException(String.format("invalid pattern: %s", pat));
            }
        }

        public void write(Consumer<String> out) {
            getClusterLines().forEach(ls ->
                    writeLine(out, ls));
            getConfigLines().forEach(ls ->
                    writeLine(out, ls));
            out.accept("");
            getBlocks().forEach(b ->
                    b.write(out));
        }

        public void writeLine(Consumer<String> out, List<CommandToken> line) {
            out.accept(line.stream()
                    .map(CommandToken::toSource)
                    .collect(Collectors.joining(" ")));
        }
    }

    public static class CommandBlockLineContinue extends CommandBlock {
        public static final long serialVersionUID = 1L;
        protected CommandBlock base;
        protected List<CommandToken> commands;

        public CommandBlockLineContinue(CommandBlock base, List<CommandToken> commands) {
            this.base = base;
            this.commands = commands;
        }

        @Override
        public CommandBlock parse(CommandToken indent, LinkedList<CommandToken> tokens) {
            boolean end = false;
            CommandToken lineSepToken = new CommandToken(CommandTokenType.Identifier, "\\");
            while (!tokens.isEmpty() && !(match(tokens, CommandTokenType.LineEnd))) {
                CommandToken arg = tokens.removeFirst();
                if (arg.equals(lineSepToken)) {
                    end = false;
                    break;
                }
                commands.add(arg);
                end = true;
            }
            if (end) {
                return base;
            } else {
                return this;
            }
        }
    }

    public static class CommandBlockNamed extends CommandBlock {
        public static final long serialVersionUID = 1L;
        protected int indentLevel;
        protected boolean isClass;
        protected CommandToken name;
        protected CommandBlock parent;
        public CommandBlockNamed(CommandToken indent, boolean isClass, CommandToken name, CommandBlock parent) {
            this.parent = parent;
            this.isClass = isClass;
            this.name = name;
            this.indentLevel = indentLength(indent);
        }

        public CommandBlock getParent() {
            return parent;
        }

        public void setParent(CommandBlock parent) {
            this.parent = parent;
        }

        public boolean isClassType() {
            return isClass;
        }

        public CommandToken getName() {
            return name;
        }

        public int indentLength(CommandToken indent) {
            int n = 0;
            for (char c : indent.getData().toCharArray()) {
                if (c == '\t') {
                    n += 4;
                } else {
                    n++;
                }
            }
            return n;
        }

        @Override
        public CommandBlock parse(CommandToken indent, LinkedList<CommandToken> tokens) {
            int len = indentLength(indent);
            if (this.indentLevel < len) {
                return super.parse(indent, tokens);
            } else {
                tokens.addFirst(indent);
                return parent.parse(tokens);
            }
        }

        @Override
        public void write(Consumer<String> out) {
            out.accept((isClassType() ? "class" : "node") + " " + getName().toSource() + ":");
            super.write(l -> out.accept("   " + l));
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" +
                    (isClassType() ? "class" : "node") +
                    ", " + name +
                    ')';
        }
    }

    public static class ClusterUnit<AppConfType extends ConfigBase> implements Serializable, ClusterHttp.ToJson {
        public static final long serialVersionUID = 1L;
        protected String name;
        protected ConfigDeployment deploymentConfig;
        protected AppConfType appConfig;
        protected CommandBlock block;

        protected List<String> classPathList;

        public void setBlock(CommandBlock block) {
            this.block = block;
        }

        public CommandBlock getBlock() {
            return block;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public ConfigDeployment getDeploymentConfig() {
            return deploymentConfig;
        }

        public AppConfType getAppConfig() {
            return appConfig;
        }

        public void setDeploymentConfig(ConfigDeployment deploymentConfig) {
            this.deploymentConfig = deploymentConfig;
        }

        public void setAppConfig(AppConfType appConfig) {
            this.appConfig = appConfig;
        }

        public void log(String str, Object... args) {
            if (appConfig != null) {
                appConfig.log(str, args);
            } else {
                deploymentConfig.log(str, args);
            }
        }

        public void setClassPathList(List<String> classPathList) {
            this.classPathList = classPathList;
        }

        public List<String> getClassPathList() {
            return classPathList;
        }

        public ClusterUnit<AppConfType> edit(Consumer<ClusterUnit<AppConfType>> f) {
            f.accept(this);
            return this;
        }


        public void setAppConfigLogHeader() {
            try {
                String lh = getDeploymentConfig().logMessageHeaderHostPort().format();
                getAppConfig().set("logHeader", lh);
            } catch (Exception ex) {
                //ignore
            }
        }

        @Override
        public Map<String, Object> toJson(Function<Object, Object> valueConverter) {
            Map<String,Object> json = new LinkedHashMap<>();
            json.put("name", toJson(valueConverter, name, ""));
            json.put("deploymentConfig", toJson(valueConverter, deploymentConfig, ""));
            json.put("appConfig", toJson(valueConverter, appConfig, null));
            json.put("classPathList", toJson(valueConverter, classPathList, new ArrayList<>()));
            Object blk = "";
            if (block != null) {
                StringBuilder buf = new StringBuilder();
                block.write(s -> buf.append(s).append("\n"));
                blk = buf.toString();
            }
            json.put("block", blk);
            return json;
        }

        public CommandBlock getOrCreateBlock() {
            if (this.block == null) {
                CommandBlockNamed block = new CommandBlockNamed(
                        new CommandToken(CommandTokenType.Indent, ""),
                        false,
                        new CommandToken(CommandTokenType.String, name), null);

                addToBlock(block.getClusterLines(), deploymentConfig);
                addToBlock(block.getConfigLines(), appConfig);
                this.block = block;
            }
            return this.block;
        }

        public void addToBlock(List<List<CommandToken>> block, ConfigBase conf) {
            if (conf != null) {
                Arrays.stream(conf.getClass().getFields())
                        .filter(ConfigBase::isConfigProperty)
                        .map(f -> Arrays.asList(
                                new CommandToken(CommandTokenType.Indent, "    "),
                                new CommandToken(CommandTokenType.Identifier, f.getName()),
                                toValueToken(conf.get(f))))
                        .forEach(block::add);
            }
        }

        public CommandToken toValueToken(Object o) {
            if (o instanceof String) {
                return new CommandToken(CommandTokenType.String, (String) o);
            } else if (o instanceof Number) {
                return new CommandToken(CommandTokenType.Number, o.toString());
            } else {
                return new CommandToken(CommandTokenType.Identifier, Objects.toString(o));
            }
        }

        public CommandBlockRoot getOrCreateRootBlock() {
            CommandBlock top = block;
            if (block != null) {
                top = getTopBlock(block);
            } else {
                block = getOrCreateBlock();
            }
            CommandBlockRoot root;
            if (top instanceof CommandBlockRoot) {
                root = (CommandBlockRoot) top;
            } else {
                root = new CommandBlockRoot();
                add(root, block);
            }
            return root;
        }

        protected void add(CommandBlock block, CommandBlock child) {
            block.getBlocks().add(child);
            if (child instanceof CommandBlockNamed) {
                ((CommandBlockNamed) child).setParent(block);
            }
        }

        protected ClusterCommands.CommandBlock getTopBlock(ClusterCommands.CommandBlock block) {
            while (block instanceof ClusterCommands.CommandBlockNamed) {
                ClusterCommands.CommandBlock p = ((ClusterCommands.CommandBlockNamed) block).getParent();
                if (p == null) {
                    break;
                } else {
                    block = p;
                }
            }
            return block;
        }
    }
}
