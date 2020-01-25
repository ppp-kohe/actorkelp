package csl.actor.example;

import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.ObjectMessageClient;
import csl.actor.remote.ObjectMessageServer;

import java.util.ArrayList;
import java.util.List;

public class ObjectMessageExample {
    public static void main(String[] args) throws Exception {
        ActorSystemRemote.debugLog = true;

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
        System.out.println("FINISH received=" + received);

        con.write("close");
        con.close();
        client.close();
    }

    static List<Object> received = new ArrayList<>();
    static ObjectMessageServer server;

    static void receive(Object obj) {
        received.add(obj);
        System.out.println("RECEIVE " + obj);

        if (obj.equals("close")) {
            server.close();
        }
    }


}
