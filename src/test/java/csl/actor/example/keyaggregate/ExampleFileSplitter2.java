package csl.actor.example.keyaggregate;

import csl.actor.ActorSystemDefault;
import csl.actor.cluster.FileSplitter;
import csl.actor.cluster.PhaseShift;
import csl.actor.example.ExpLock;
import csl.actor.keyaggregate.Config;
import csl.actor.keyaggregate.FileMapper;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExampleFileSplitter2 {
    final static AtomicBoolean error = new AtomicBoolean();
    public static void main(String[] args) throws Exception {
        String src = args[0];
        for (int n = 0; n < 20; ++n) {
            if (error.get()) {
                System.err.println("error");
                break;
            }
            System.err.println("loop: "+ n);
            ActorSystemDefault system = new ActorSystemDefault();

            long max = Long.MAX_VALUE;

            FileMapper mapper = new FileMapper(system, "mapper", Config.CONFIG_DEFAULT, FileSplitter.getWithSplitCount(10));

            ExpLock.TreeActor t = new ExpLock.TreeActor(system, "root1", ExpLock.MapActor::new);
            ExpLock.TreeActor t2 = new ExpLock.TreeActor(system, "root2", ExpLock.LeafActor::new);
            ExpLock.EndActor e = new ExpLock.EndActor(system, "end", max);

            mapper.setNextStage(t).get();
            t.set(new ExpLock.Setting(10, t2));
            t2.set(new ExpLock.Setting(10, e));
            Thread.sleep(100);

            mapper.tell(new FileSplitter.FileSplit(src));
            Thread.sleep(2000);
            mapper.tell(new PhaseShift(src, e));

            system.getExecutorService().awaitTermination(1, TimeUnit.HOURS);
        }
    }

}
