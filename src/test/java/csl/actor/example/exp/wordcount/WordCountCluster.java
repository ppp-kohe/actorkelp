package csl.actor.example.exp.wordcount;

import com.esotericsoftware.kryo.Kryo;
import csl.actor.kelp_old.ActorPlacementKelp;
import csl.actor.kelp_old.ClusterKelp;
import csl.actor.kelp_old.FileMapper;
import csl.actor.remote.KryoBuilder;

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
        ClusterKelp c = ClusterKelp.create();
        ActorPlacementKelp place = c.deploy(confFile);

        FileMapper fileReader = place.fileMapperWithSplitCount(10);
        WordCount.WordCountMapper mapper = place.actor((system, conf) -> new WordCount.WordCountMapper(system, "mapper", conf));
        WordCount.WordCountReducer reducer = place.actor((system, conf) -> new WordCount.WordCountReducer(system, "reducer", conf, "."));

        place.connectStage(fileReader, mapper, reducer).get();

        fileReader.startReadFile(inputFile).get();

        c.shutdownAll();
    }

    public static class KryoBuilderWordCount extends KryoBuilder {
        @Override
        public Kryo build() {
            Kryo k = super.build();
            register(k, WordCount.Count.class);
            return k;
        }
    }

    static String[] createConf(String inputFile) throws Exception {
        Path inputPath = Paths.get(inputFile);
        Path dir = inputPath.getParent();
        String confData = "" +
                "node primary:\n" +
                "   host \"localhost\"\n" +
                "   port 38888\n" +
                "   primary true\n" +
                String.format("   baseDir \"%s\"\n", dir) +
                "   logColor 102\n" +
                "   logFile true\n" +
                "   lowerBoundThresholdFactor 0\n" +
                "   kryoBuilderType \"" + KryoBuilderWordCount.class.getName() + "\"\n" +
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
                "   kryoBuilderType \"" + KryoBuilderWordCount.class.getName() + "\"\n" +
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
                "   kryoBuilderType \"" + KryoBuilderWordCount.class.getName() + "\"\n" +
                "\n";
        Path confPath = dir.resolve("config.txt");
        Files.writeString(confPath, confData);
        return new String[] { inputPath.getFileName().toString(), confPath.toString() };
    }
}
