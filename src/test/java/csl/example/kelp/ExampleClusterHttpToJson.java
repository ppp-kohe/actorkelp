package csl.example.kelp;

import csl.actor.cluster.ClusterCommands;
import csl.actor.cluster.ClusterDeployment;
import csl.actor.cluster.ClusterHttp;
import csl.actor.kelp.ConfigKelp;

public class ExampleClusterHttpToJson {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        ClusterHttp c = new ClusterHttp(
                new ClusterDeployment<>(
                ConfigKelp.class, ClusterDeployment.ActorPlacementForCluster.class));
        Object o = c.jsonConverter(Object.class).apply(
                new ClusterCommands.ClusterUnit<>()
                        .edit(u -> u.setAppConfig(new ConfigKelp())));

        System.err.println(o);
    }
}
