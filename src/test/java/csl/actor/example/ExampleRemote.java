package csl.actor.example;

import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExampleRemote {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            setMvnClasspath();

            int startPort = 30000;
            int endPort =   30010;
            for (int i = startPort; i < endPort; ++i) {
                launchJava(ExampleRemote.class.getName(), Integer.toString(i), Integer.toString(i + 1));
            }
            launchJava(ExampleRemote.class.getName(), Integer.toString(endPort), Integer.toString(startPort), "0").waitFor();
        } else {
            String host = "localhost";
            String port = args[0];
            String linkPort = args[1];
            System.err.println("started " + port + " " + linkPort + " [" + args.length + "]");
            int p = Integer.parseInt(port);
            int lp = Integer.parseInt(linkPort);
            ActorSystemRemote sys = new ActorSystemRemote();
            sys.startWithoutWait(p);

            new CustomCloser(sys);

            Example.MyActor a = new Example.MyActor(sys, ActorRefRemote.get(sys, host, lp, "example"));
            if (args.length >= 3) {
                String msgVal = args[2];
                System.err.println("init value " + msgVal);
                Thread.sleep(3000);
                System.err.println("start");
                a.tell(Integer.valueOf(msgVal), null);
            }
        }
    }

    static class CustomCloser extends ActorSystemRemote.ConnectionCloseActor {
        public CustomCloser(ActorSystemRemote system) {
            super(system);
        }

        @Override
        public void receive(ActorSystemRemote.ConnectionCloseNotice notice) {
            super.receive(notice);
            getSystem().close();
        }
    }

    static String classpath;
    public static void setMvnClasspath() {
        try {
            ProcessBuilder pb = new ProcessBuilder();
            pb.environment().put("MAVEN_OPTS", "");
            boolean win = System.getProperty("os.name", "").contains("Windows");
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
