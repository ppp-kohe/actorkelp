package csl.actor.example.keyaggregate;

import csl.actor.keyaggregate.ActorPlacementKeyAggregation;
import csl.actor.keyaggregate.ClusterKeyAggregation;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class ExampleClusterRemoteDriver {
    public static void main(String[] args) throws Exception {
        String[] path = createConf("target/debug-remote-driver");
        ClusterKeyAggregation.run(path[1], TestMain.class.getName(), "hello", "world");
    }

    static String[] createConf(String inputFile) throws Exception {
        Path inputPath = Paths.get(inputFile);
        Path dir = inputPath.getParent();
        Files.createDirectories(dir);

        Path p0 = inputPath.resolve("node-0").toAbsolutePath();
        Path p1 = inputPath.resolve("node-1").toAbsolutePath();
        Path p2 = inputPath.resolve("node-2").toAbsolutePath();
        Files.createDirectories(p0);
        Files.createDirectories(p1);
        Files.createDirectories(p2);

        String confData = "" +
                "node master:\n" +
                "   host \"localhost\"\n" +
                "   port 38888\n" +
                "   master true\n" +
                String.format("   baseDir \"%s\"\n", p0) +
                "   logColor 102\n" +
                "   logFile true\n" +
                "   lowerBoundThresholdFactor 0\n" +
                "   sharedDeploy false\n" +
                "\n" +
                "node follower1:\n" +
                "   host \"localhost\"\n" +
                "   port 38889\n" +
                "   java \"java -Xmx512m %s %s %s\"\n" +
                String.format("   baseDir \"%s\"\n", p1) +
                "   logColor 71\n" +
                "   logFile true\n" +
                "   persist true\n" +
                "   persistMailboxOnMemorySize 1_000\n" +
                "   lowerBoundThresholdFactor 0\n" +
                "   sharedDeploy false\n" +
                "\n" +
                "node follower2:\n" +
                "   host \"localhost\"\n" +
                "   port 38890\n" +
                "   java \"java -Xmx512m %s %s %s\"\n" +
                String.format("   baseDir \"%s\"\n", p2) +
                "   logColor 72\n" +
                "   logFile true\n" +
                "   persist true\n" +
                "   persistMailboxOnMemorySize 1_000\n" +
                "   lowerBoundThresholdFactor 0\n" +
                "   sharedDeploy false\n" +
                "\n";
        Path confPath = dir.resolve("config.txt");
        Files.writeString(confPath, confData);
        return new String[] { inputPath.getFileName().toString(), confPath.toString() };
    }

    public static class TestMain {
        public static void main(String[] args) throws Exception {
            ClusterKeyAggregation cluster = ClusterKeyAggregation.create();
            ActorPlacementKeyAggregation place = cluster.deploy(args[0]);
            System.out.println(place + " : " + Arrays.toString(args));
            Thread.sleep(10_000);
            cluster.shutdownAll();
        }
    }
}
