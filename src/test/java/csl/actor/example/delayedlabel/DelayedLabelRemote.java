package csl.actor.example.delayedlabel;

import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.CallableMessage;
import csl.actor.example.ExampleRemote;
import csl.actor.keyaggregate.ActorKeyAggregation;
import csl.actor.keyaggregate.ActorPlacement;
import csl.actor.keyaggregate.Config;
import csl.actor.keyaggregate.ResponsiveCalls;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DelayedLabelRemote extends DelayedLabelAggregationReplicable {
    public static void main(String[] args) {
        new DelayedLabelRemote().run(args);
    }

    @Override
    public void run(PrintWriter out, String src) {
        Iterator<Object> inputs = inputs(out, src);
        config.lowerBoundThresholdFactor = -1; //no Few mailbox and no merge

        ActorSystemRemote system = new ActorSystemRemote();
        ResponsiveCalls.initCallableTarget(system);
        int serverPort = 10000;
        system.startWithoutWait(serverPort);

        new ActorKeyAggregation.PlacemenActorKeyAggregation(system,
                new ActorPlacement.PlacementStrategyRoundRobin(0));

        List<Process> ps = new ArrayList<>();
        ExampleRemote.setMvnClasspath();
        ps.add(ExampleRemote.launchJava("-Dcsl.actor.debug.color=106", Follower.class.getName(), "10001", Integer.toString(serverPort)));
        ps.add(ExampleRemote.launchJava("-Dcsl.actor.debug.color=118", Follower.class.getName(), "10002", Integer.toString(serverPort)));
        try {
            Thread.sleep(5000);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        Instant startTime = Instant.now();
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
        LearnerActorAggregationReplicable r = new LearnerRemote(system, out, resultActor, config);
        root = r;
        ResponsiveCalls.sendTask(system, r,
                CallableMessage.callableMessageConsumer((a, s) -> ((ActorKeyAggregation) a).routerSplit(3)));
        return r;
    }

    static class LearnerRemote extends LearnerActorAggregationReplicable {
        public LearnerRemote(ActorSystem system, String name, Config config, ActorRef result, State state) {
            super(system, name, config, result, state);
        }

        public LearnerRemote(ActorSystem system, String name, PrintWriter out, ActorRef resultActor, DelayedLabelConfig config, State state) {
            super(system, name, out, resultActor, config, state);
        }

        public LearnerRemote(ActorSystem system, PrintWriter out, ActorRef resultActor, DelayedLabelConfig config) {
            super(system, out, resultActor, config);
        }

        @Override
        public void finish(Finish f) {
            super.finish(f);
            ((ActorPlacement.PlacemenActor) getPlacement()).getCluster().stream()
                .map(ActorPlacement.AddressListEntry::getPlacementActor)
                .forEach(a -> ResponsiveCalls.sendTaskConsumer(system, a, (act,sen) -> {
                    System.out.println("#remote close: " + act);
                    act.getSystem().getScheduledExecutor().schedule(() -> act.getSystem().close(), 1, TimeUnit.SECONDS);
                }));
            try {
                Thread.sleep(3000);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            getSystem().close();
        }
    }

    public static class Follower {
        public static void main(String[] args) throws Exception {
            ActorSystemRemote system = new ActorSystemRemote();
            ResponsiveCalls.initCallableTarget(system);
            int port = Integer.parseInt(args[0]);
            int joinPort = Integer.parseInt(args[1]);
            system.startWithoutWait(port);

            ActorKeyAggregation.PlacemenActorKeyAggregation p = new ActorKeyAggregation.PlacemenActorKeyAggregation(system,
                new ActorPlacement.PlacementStrategyUndertaker());

            p.join(ActorAddress.get("localhost", joinPort));
        }
    }
}
