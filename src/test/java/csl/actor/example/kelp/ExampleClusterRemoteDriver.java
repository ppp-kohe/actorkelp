package csl.actor.example.kelp;

import csl.actor.kelp.ActorPlacementKelp;
import csl.actor.kelp.ClusterKelp;
import csl.actor.kelp.ConfigKelp;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class ExampleClusterRemoteDriver {
    public static void main(String[] args) throws Exception {
        String[] path = createConf("target/debug-remote-driver");
        ClusterKelp.run(path[1], TestMain.class.getName(), "hello", "world");
    }
    public static class TestMain {
        public static void main(String[] args) throws Exception {
            try (ClusterKelp<ConfigKelp> cluster = ClusterKelp.createAndDeploy()) {
                System.out.println(cluster + " : " + Arrays.toString(args));
                Thread.sleep(10_000);
            }
        }
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
                "node primary:\n" +
                "   host \"localhost\"\n" +
                "   port 38888\n" +
                "   primary true\n" +
                String.format("   baseDir \"%s\"\n", p0) +
                "   logColor 102\n" +
                "   logFile true\n" +
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
                "   sharedDeploy false\n" +
                "\n";
        Path confPath = dir.resolve("config.txt");
        Files.writeString(confPath, confData);
        return new String[] { inputPath.getFileName().toString(), confPath.toString() };
    }

}
