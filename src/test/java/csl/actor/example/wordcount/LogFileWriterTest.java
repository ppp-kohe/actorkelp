package csl.actor.example.wordcount;

import csl.actor.ActorSystemDefault;
import csl.actor.cluster.LogFileWriter;

import java.nio.file.Paths;

public class LogFileWriterTest {
    public static void main(String[] args) throws Exception {
        try (LogFileWriter w = new LogFileWriter(System.err, Paths.get("target/debug-log.txt"), false)) {
            w.println("hello, world");
            w.println(new ActorSystemDefault.SystemLoggerErr().toColorLine(100, "color"));
            w.println("hello, world2");
        }

        try (LogFileWriter w = new LogFileWriter(System.err, Paths.get("target/debug-log2.txt"), true)) {
            w.println("hello, world");
            w.println(new ActorSystemDefault.SystemLoggerErr().toColorLine(100, "color"));
            w.println("hello, world2");
        }

    }
}
