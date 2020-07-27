package csl.actor.example.util;

import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.example.TestTool;
import csl.actor.util.PathModifier;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ExamplePathModifier {
    public static void main(String[] args) {
        new ExamplePathModifier().testDefaultPath();
        new ExamplePathModifier().testBasePath();
    }

    public void testDefaultPath() {
        ActorSystem system = new ActorSystemDefault();
        PathModifier pm = PathModifier.getPathModifier(system);

        Path path = pm.getExpanded("%h/%a/hello.txt");
        System.err.println(path);

        Path p0 = path.getFileName();
        Path p1 = path.getParent();
        Path p2 = p1.getParent();
        TestTool.assertEquals("getExpanded", Paths.get("hello.txt"), p0);
        TestTool.assertTrue("getExpanded a", p1.getFileName().toString().startsWith("app-"));
        TestTool.assertEquals("getExpanded h", Paths.get("local"), p2);

        String exPath = pm.expandPath("%h-%h-%h-%a");
        TestTool.assertTrue("expandPath", exPath.startsWith("local-local-local-app-"));
        system.close();
    }

    public void testBasePath() {
        ActorSystem system = new ActorSystemDefault();
        PathModifier.PathModifierHost host = PathModifier.setPathModifierWithBaseDir(system, "/tmp/hello");

        TestTool.assertEquals("getPathModifier", host, PathModifier.getPathModifier(system));
        host.setApp("APP");
        host.setHost("hst", -1);

        String path = host.expandPath("%h/%a/test.txt");
        TestTool.assertEquals("expandPath", "hst--1/APP/test.txt", path);

        Path p = host.get(path);
        TestTool.assertEquals("get", Paths.get("/tmp/hello/hst--1/APP/test.txt"), p);
        system.close();
    }


}
