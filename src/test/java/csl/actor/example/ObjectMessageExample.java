package csl.actor.example;

import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.ObjectMessageClient;
import csl.actor.remote.ObjectMessageServer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ObjectMessageExample {
    public static void main(String[] args) throws Exception {
        ActorSystemRemote.debugLog = true;
        ActorSystemRemote.debugLogMsg = true;

        new Thread() {public void run() {
            ObjectMessageServer server = new ObjectMessageServer();
            ObjectMessageExample.server = server;
            server.setPort(30000);
            server.setReceiver(ObjectMessageExample::receive);
            System.err.println("server start");
            server.start();
        }}.start();

        Thread.sleep(3000);

        ObjectMessageClient client = new ObjectMessageClient();
        client.setPort(30000);
        client.setHost("localhost");
        ObjectMessageClient.ObjectMessageConnection con = client.connect();
        con.write("hello");
        Thread.sleep(3000);
        con = client.connect();
        con.write("world");
        Thread.sleep(3000);
        con.write("!!!");
        Thread.sleep(1000);
        for (int i = 0; i < 100; ++i) {
            con.write(new ExampleData(i, new Random().doubles(100_000).toArray()));
        }
        System.out.println("FINISH received=" + received);

        con.write("close");
        con.close();
        client.close();
    }

    static List<Object> received = new ArrayList<>();
    static ObjectMessageServer server;

    static int receive(Object obj) {
        received.add(obj);
        System.out.println("RECEIVE " + obj);

        if (obj.equals("close")) {
            server.close();
        }
        return 200;
    }

    static class ExampleData implements Serializable {
        public long n;
        public double[] data;

        public ExampleData(long n, double[] data) {
            this.n = n;
            this.data = data;
        }

        @Override
        public String toString() {
            return "(" + n + ")";
        }
    }

}
