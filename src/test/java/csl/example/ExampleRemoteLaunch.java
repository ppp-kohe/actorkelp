package csl.example;

import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;

public class ExampleRemoteLaunch {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            TestToolRemote.setMvnClasspath();

            int startPort = 30000;
            int endPort =   30010;
            for (int i = startPort; i < endPort; ++i) {
                TestToolRemote.launchJava(ExampleRemoteLaunch.class.getName(), Integer.toString(i), Integer.toString(i + 1));
            }
            TestToolRemote.launchJava(ExampleRemoteLaunch.class.getName(), Integer.toString(endPort), Integer.toString(startPort), "0").waitFor();
        } else {
            String host = "localhost";
            String port = args[0];
            String linkPort = args[1];
            System.err.println("started " + port + " " + linkPort + " [" + args.length + "]");
            int p = Integer.parseInt(port);
            int lp = Integer.parseInt(linkPort);
            ActorSystemRemote sys = new ActorSystemRemote();
            ActorSystemRemote.debugLog = true;
            sys.startWithoutWait(p);

            new CustomCloser(sys);

            ExampleActor.MyActor a = new ExampleActor.MyActor(sys, ActorRefRemote.get(sys, host, lp, "example"));
            if (args.length >= 3) {
                String msgVal = args[2];
                System.err.println("init value " + msgVal);
                Thread.sleep(3000);
                System.err.println("start");
                a.tell(Integer.valueOf(msgVal));
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

}
