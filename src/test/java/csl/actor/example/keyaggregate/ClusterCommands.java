package csl.actor.example.keyaggregate;

import csl.actor.keyaggregate.Config;
import csl.actor.keyaggregate.ConfigBase;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClusterCommands {

    public List<ClusterDeployment.ClusterUnit> loadConfigFile(String path) {
        CommandBlockRoot root = parseConfigFile(path);
        return root.getNameToBlock().values().stream()
            .filter(CommandBlockNamed::isClassType)
            .map(this::load)
            .collect(Collectors.toList());
    }

    protected ClusterDeployment.ClusterUnit load(CommandBlock block) {
        ClusterDeployment.ClusterUnit unit = new ClusterDeployment.ClusterUnit();
        ClusterConfig conf = new ClusterConfig();
        unit.setClusterConfig(conf);
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
            unit.setAppConfig((Config) Class.forName(conf.configType).getConstructor()
                    .newInstance());

            block.getConfigLines()
                    .forEach(cs -> set(unit.getAppConfig(), cs));
        } catch (Exception e) {
            conf.log("appConfig error: " + conf.configType + " : " + e);
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
            while (hasNext() && !Character.isWhitespace(peek()) &&
                    peek() != '#' || peek() != ':') {
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
                while (hasNext() && peek() == '"') {
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

    public static class CommandToken {
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
        protected Map<String, CommandBlockNamed> nameToBlock = new HashMap<>();
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

                inheritCommand(true, block, nextSuper, b -> b.getConfigLines().stream()
                        .filter(cs -> !cs.get(0).getData().equals("extends"))
                        .collect(Collectors.toList()));
                inheritCommand(false, block, nextSuper, CommandBlock::getConfigLines);

                inherit(block, nextSuper.getSuperBlock(), nextHistory);
            }
        }

        protected void inheritCommand(boolean cluster, CommandBlock block, CommandBlock nextSuper, Function<CommandBlock,List<List<CommandToken>>> lineGetter) {
            lineGetter.apply(nextSuper)
                    .forEach(cs -> inheritCommand(cluster,
                            lineGetter.apply(block),
                            findCommand(lineGetter.apply(block), cs),
                            cs));
        }

        protected void inheritCommand(boolean cluster, List<List<CommandToken>> lines, List<CommandToken> existing, List<CommandToken> cs) {
            if (existing != null) {
                lines.add(cs);
            }
        }

        private List<CommandToken> findCommand(List<List<CommandToken>> csLines, List<CommandToken> cs) {
            return csLines.stream()
                    .filter(ecs -> ecs.get(0).getData().equals(cs.get(0).getData()))
                    .findFirst()
                    .orElse(null);
        }
    }

    public static class CommandBlock {
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
                    if (match(tokens, CommandTokenType.Identifier, CommandTokenType.Colon, CommandTokenType.LineEnd) ||
                            match(tokens, CommandTokenType.String, CommandTokenType.Colon, CommandTokenType.LineEnd)) {
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
                    while (!tokens.isEmpty() && !(match(tokens, CommandTokenType.LineEnd))) {
                        CommandToken arg = tokens.removeFirst();
                        if (arg.getType().equals(CommandTokenType.Identifier) && arg.getData().equals("\\")) {
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
                clusterCommands = Arrays.stream(ClusterConfig.class.getFields())
                        .filter(ConfigBase::isConfigProperty)
                        .map(Field::getName)
                        .collect(Collectors.toList());
            }
            return data.equals("extends") ||
                    clusterCommands.contains(data);
        }

        public boolean match(LinkedList<CommandToken> tokens, CommandTokenType... types) {
            if (tokens.size() > types.length) {
                for (int i = 0; i < types.length; ++i) {
                    CommandToken t = tokens.get(i);
                    if (!t.getType().equals(types[i])) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        }

    }

    public static class CommandBlockLineContinue extends CommandBlock {
        protected CommandBlock base;
        protected List<CommandToken> commands;

        public CommandBlockLineContinue(CommandBlock base, List<CommandToken> commands) {
            this.base = base;
            this.commands = commands;
        }

        @Override
        public CommandBlock parse(CommandToken indent, LinkedList<CommandToken> tokens) {
            boolean end = false;
            while (!tokens.isEmpty() && !(match(tokens, CommandTokenType.LineEnd))) {
                CommandToken arg = tokens.removeFirst();
                if (arg.getType().equals(CommandTokenType.Identifier) && arg.getData().equals("\\")) {
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
            if (this.indentLevel > len) {
                return super.parse(indent, tokens);
            } else {
                tokens.addFirst(indent);
                return parent.parse(tokens);
            }
        }
    }
}
