package csl.actor.example.wordcount;

import csl.actor.ActorSystem;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.cluster.FileSplitter;
import csl.actor.keyaggregate.ActorPlacementKeyAggregation;
import csl.actor.keyaggregate.ClusterKeyAggregation;
import csl.actor.keyaggregate.Config;
import csl.actor.keyaggregate.FileMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class WordCountCluster {
    public static void main(String[] args) throws Exception {
        String inputFile;
        String confFile;
        if (args.length == 1) {
            String[] ns = createConf(args[0]);
            inputFile = ns[0];
            confFile = ns[1];
        } else {
            inputFile = args[0];
            confFile = args[1];
        }
        ClusterKeyAggregation c = ClusterKeyAggregation.create();
        ActorPlacementKeyAggregation place = c.deploy(confFile);

        Config conf = c.getMaster().getAppConfig();
        ActorSystem system = c.getSystem();

        FileMapper fileReader = new FileMapper(system, "fileReader", conf,
                FileSplitter.getWithSplitCount(10, ConfigDeployment.getPathModifier(system)));
        WordCount.WordCountMapper mapper = new WordCount.WordCountMapper(system, "mapper", conf);
        WordCount.WordCountReducer reducer = new WordCount.WordCountReducer(system, "reducer", conf, ".");

        place.connectStage(fileReader, mapper, reducer).get();

        fileReader.startReadFile(inputFile).get();

        c.shutdownAll();
    }

    static String[] createConf(String inputFile) throws Exception {
        Path inputPath = Paths.get(inputFile);
        Path dir = inputPath.getParent();
        String confData = "" +
                "node master:\n" +
                "   host \"localhost\"\n" +
                "   port 38888\n" +
                "   master true\n" +
                String.format("   baseDir \"%s\"\n", dir) +
                "   logColor 102\n" +
                "   logFile true\n" +
                "   lowerBoundThresholdFactor 0\n" +
                "\n" +
                "node follower1:\n" +
                "   host \"localhost\"\n" +
                "   port 38889\n" +
                "   java \"java -Xmx512m %s %s %s\"\n" +
                String.format("   baseDir \"%s\"\n", dir) +
                "   logColor 71\n" +
                "   logFile true\n" +
                "   persist true\n" +
                "   persistMailboxOnMemorySize 1_000\n" +
                "   lowerBoundThresholdFactor 0\n" +
                "\n" +
                "node follower2:\n" +
                "   host \"localhost\"\n" +
                "   port 38890\n" +
                "   java \"java -Xmx512m %s %s %s\"\n" +
                String.format("   baseDir \"%s\"\n", dir) +
                "   logColor 72\n" +
                "   logFile true\n" +
                "   persist true\n" +
                "   persistMailboxOnMemorySize 1_000\n" +
                "   lowerBoundThresholdFactor 0\n" +
                "\n";
        Path confPath = dir.resolve("config.txt");
        Files.writeString(confPath, confData);
        return new String[] { inputPath.getFileName().toString(), confPath.toString() };
    }
}
