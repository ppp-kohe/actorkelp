package csl.actor.example.wordcount;

import csl.actor.cluster.ClusterDeployment;
import csl.actor.keyaggregate.Config;

import java.nio.file.Paths;

public class LogFileWriterTest {
    public static void main(String[] args) throws Exception {
        try (ClusterDeployment.LogFileWriter w = new ClusterDeployment.LogFileWriter(System.err, Paths.get("target/debug-log.txt"), false)) {
            w.println("hello, world");
            w.println(Config.CONFIG_DEFAULT.toConsoleLine("color"));
            w.println("hello, world2");
        }

        try (ClusterDeployment.LogFileWriter w = new ClusterDeployment.LogFileWriter(System.err, Paths.get("target/debug-log2.txt"), true)) {
            w.println("hello, world");
            w.println(Config.CONFIG_DEFAULT.toConsoleLine("color"));
            w.println("hello, world2");
        }

    }
}
