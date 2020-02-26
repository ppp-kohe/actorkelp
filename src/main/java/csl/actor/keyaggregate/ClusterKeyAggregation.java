package csl.actor.keyaggregate;

import csl.actor.cluster.ClusterDeployment;

public class ClusterKeyAggregation extends ClusterDeployment<Config, ActorPlacementKeyAggregation> {
    public static ClusterKeyAggregation create() {
        return new ClusterKeyAggregation();
    }

    public ClusterKeyAggregation() {
        super(Config.class, ActorPlacementKeyAggregation.class);
    }

    public ClusterKeyAggregation(Class<Config> defaultConfType, Class<ActorPlacementKeyAggregation> placeType) {
        super(defaultConfType, placeType);
    }


}
