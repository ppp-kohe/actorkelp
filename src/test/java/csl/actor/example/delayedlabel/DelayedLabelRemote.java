package csl.actor.example.delayedlabel;

import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.CallableMessage;
import csl.actor.example.Example;
import csl.actor.example.ExampleRemote;
import csl.actor.msgassoc.ActorAggregationReplicable;
import csl.actor.msgassoc.ActorPlacement;
import csl.actor.msgassoc.ResponsiveCalls;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DelayedLabelRemote extends DelayedLabelAggregationReplicable {
    public static void main(String[] args) {
        new DelayedLabelRemote().run(args);
    }

    @Override
    public void run(PrintWriter out, String src) {
        Iterator<Object> inputs = inputs(out, src);

        Instant startTime = Instant.now();

        ActorSystemRemote system = new ActorSystemRemote();
        ResponsiveCalls.initCallableTarget(system);
        int serverPort = 10000;
        system.startWithoutWait(serverPort);

        new ActorAggregationReplicable.PlacemenActorReplicable(system,
                new ActorPlacement.PlacementStrategyRoundRobin(0));

        List<Process> ps = new ArrayList<>();
        ExampleRemote.setMvnClasspath();
        //ps.add(ExampleRemote.launchJava("-Dcsl.actor.debug.color=106", Follower.class.getName(), "10001", Integer.toString(serverPort)));
        //ps.add(ExampleRemote.launchJava("-Dcsl.actor.debug.color=118", Follower.class.getName(), "10002", Integer.toString(serverPort)));
        try {
            Thread.sleep(10_000);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        ResultActor resultActor = resultActor(system, out, startTime);
        ActorRef learnerActor = learnerActor(system, out, resultActor);
        resultActor.setLearner(learnerActor);

        System.out.println(system.getLocalSystem().getNamedActorMap());

        while (inputs.hasNext()) {
            learnerActor.tell(inputs.next(), null);
        }
    }

    @Override
    public ActorRef learnerActor(ActorSystem system, PrintWriter out, ActorRef resultActor) {
        LernerActorAggregationReplicable r = (LernerActorAggregationReplicable) super.learnerActor(system, out, resultActor);
        ResponsiveCalls.sendCallable(system, r,
                CallableMessage.callableMessageConsumer((a, s) -> ((ActorAggregationReplicable) a).routerSplit(3)));
        return r;
    }

    public static class Follower {
        public static void main(String[] args) throws Exception {
            ActorSystemRemote system = new ActorSystemRemote();
            ResponsiveCalls.initCallableTarget(system);
            int port = Integer.parseInt(args[0]);
            int joinPort = Integer.parseInt(args[1]);
            system.startWithoutWait(port);

            ActorAggregationReplicable.PlacemenActorReplicable p = new ActorAggregationReplicable.PlacemenActorReplicable(system);

            Thread.sleep(8_000);
            p.join(ActorAddress.get("localhost", joinPort));
        }
    }
}
