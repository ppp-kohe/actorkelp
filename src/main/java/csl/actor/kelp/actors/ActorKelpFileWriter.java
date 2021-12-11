package csl.actor.kelp.actors;

import csl.actor.ActorBehaviorBuilder;
import csl.actor.ActorSystem;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.behavior.ActorBehaviorBuilderKelp;
import csl.actor.util.PathModifier;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * file writer
 * <pre>
 *     var w = new ActorKelpFileWriter(system).withFileName("out/%i.txt");
 *     var ws = prev.connects(w);
 *      ...
 *     ws.merge().getFilePaths(); //close and all paths of {"out/01.txt", "out/02.txt", ...}
 * </pre>
 */
public class ActorKelpFileWriter extends ActorKelp<ActorKelpFileWriter> {
    public @TransferredState
    String filePath;
    public @TransferredState(mergeType = MergerOpType.Add)
    List<String> filePaths = new ArrayList<>();

    protected volatile PrintWriter writer;

    public static String defaultFileName = "output-%i.txt";

    public ActorKelpFileWriter(ActorSystem system, String name, ConfigKelp config) {
        super(system, name, config);
        withFileName(defaultFileName);
    }

    public ActorKelpFileWriter(ActorSystem system, ConfigKelp config) {
        super(system, config);
        withFileName(defaultFileName);
    }

    public ActorKelpFileWriter(ActorSystem system) {
        super(system);
        withFileName(defaultFileName);
    }

    /**
     * the final name will be joined with {@link ConfigKelp#outputDir} (the default is "%a") and
     * expanded by {@link PathModifier}.
     * Also, as a special rule, "%i" will be replaced with the shuffle-index
     * (the index will have "0" padding followed by {@link ConfigKelp#shufflePartitions}).
     * The default value is "output-%i.txt", and so the default fileName will be "%a/output-%i.txt"
     *
     * @param fileName the file name part
     * @return this
     */
    public ActorKelpFileWriter withFileName(String fileName) {
        String p = getOutputDir();
        return withFilePath(p == null || p.isEmpty() ? "" : (p + "/") + fileName);
    }

    /**
     * @param fileName the file path, which can contain "%..." symbols
     * @return this
     */
    public ActorKelpFileWriter withFilePath(String fileName) {
        this.filePath = fileName;
        return this;
    }

    @Override
    public double memoryManageRatioTotalActor() {
        return 0.4; //no tree construction
    }

    @Override
    protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilderKelp builder) {
        return builder.matchAnyData(this::save);
    }

    public void save(Object data) {
        String line = toString(data);
        try {
            if (writer == null) {
                PathModifier pathModifier = PathModifier.getPathModifier(getSystem());
                String localPath = pathModifier.expandPath(expandExtra(filePath));
                Path path = pathModifier.get(localPath);
                Path parent = path.getParent();
                if (parent != null && !Files.exists(parent)) {
                    Files.createDirectories(parent);
                }
                if (!filePaths.contains(localPath)) {
                    filePaths.add(localPath);
                }
                writer = new PrintWriter(Files.newBufferedWriter(path));
                getLogger().log("start writing: %s", path);
            }
            writer.println(line);
            nextStageTell(data);
        } catch (Exception ex) {
            String errStr;
            if (line.length() < 100) {
                errStr = line.substring(0, Math.min(100, line.length())) + "...";
            } else {
                errStr = line;
            }
            getLogger().log("write error %s %s %s : %s", this, filePath, ex, errStr);
        }
    }

    protected String expandExtra(String path) {
        int w = Math.max(1, Integer.toString(getShufflePartitions()).length());
        return path.replaceAll(Pattern.quote("%i"), String.format("%" + (w <= 1 ? "" : ("0" + w)) + "d", getShuffleIndex()));
    }


    @Override
    public void flush() {
        if (writer != null) {
            writer.flush();
        }
        super.flush();
    }

    @Override
    public void close() {
        super.close();
        stageEnd();
    }

    @Override
    public void stageEnd() {
        flush();
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    protected String toString(Object data) {
        return Objects.toString(data);
    }

    public List<String> getFilePaths() {
        return filePaths;
    }


}
