package csl.example;

import csl.actor.remote.ActorSystemRemote;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestToolRemote {

    static String classpath;
    public static void setMvnClasspath() {
        try {
            System.err.println("PATH: " + System.getenv("PATH"));
            System.err.println("PWD: " + System.getenv("PWD"));
            ProcessBuilder pb = new ProcessBuilder();
            pb.environment().put("MAVEN_OPTS", "");
            pb.environment().put("JAVA_HOME", System.getProperty("java.home"));
            boolean win = System.getProperty("os.name", "").contains("Windows");
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            Process proc = pb.command(win ? "mvn.cmd" : "mvn", "dependency:build-classpath", "-DincludeScope=test")
                    .start();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                boolean start = false;
                String cp = null;
                while ((line = r.readLine()) != null) {
                    if (!start) {
                        if (line.contains("Dependencies classpath:")) {
                            start = true;
                        }
                    } else {
                        if (cp != null && line.startsWith("[INFO]")) {
                            break;
                        } else {
                            cp = line;
                        }
                    }
                }
                classpath = "target/classes" + File.pathSeparator + "target/test-classes" + File.pathSeparator + cp;
                System.err.println("classpath: " + classpath);
            }

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Process launchJava(String... args) {
        try {
            List<String> list = new ArrayList<>();
            File exe = new File(System.getProperty("java.home"), "bin/java");
            list.add(exe.getPath());
            if (classpath != null) {
                list.add("-cp");
                list.add(classpath);
            }
            list.add("-Dcsl.actor.debug=" + ActorSystemRemote.debugLog);
            list.addAll(Arrays.asList(args));
            System.err.println("cmd: " + list);
            ProcessBuilder builder = new ProcessBuilder().command(list);
            builder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .redirectError(ProcessBuilder.Redirect.INHERIT);
            return builder.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
