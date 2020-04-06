package csl.actor.example.kelp;

import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.cluster.ClusterCommands;
import csl.actor.cluster.ClusterDeployment;
import csl.actor.cluster.ClusterHttp;
import csl.actor.example.exp.wordcount.WordCount;
import csl.actor.kelp.ClusterKelp;
import csl.actor.kelp.Config;

public class ExampleClusterHttpToJson {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        ClusterHttp c = new ClusterHttp(new ClusterDeployment<>(
                Config.class, ClusterDeployment.ActorPlacementForCluster.class));
        Object o = c.jsonConverter(Object.class).apply(
                new ClusterCommands.ClusterUnit<>()
                        .edit(u -> u.setAppConfig(new Config())));

        System.err.println(o);
        ActorSystem s = new ActorSystemDefault();
        WordCount.WordCountMapper m = new WordCount.WordCountMapper(s, "mapper", new Config());
        m.routerSplit(2);
        Thread.sleep(1000);
        o = c.jsonConverter(Object.class).apply(ClusterKelp.toStatTree(m));

        System.err.println(o);

        s.close();
    }
}
