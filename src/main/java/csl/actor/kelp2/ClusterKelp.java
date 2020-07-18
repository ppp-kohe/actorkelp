package csl.actor.kelp2;

import csl.actor.cluster.ClusterDeployment;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;
import csl.actor.util.ConfigBase;

public class ClusterKelp extends ClusterDeployment<ConfigKelp, ActorPlacementKelp> {
    public ClusterKelp(Class<ConfigKelp> defaultConfType, Class<ActorPlacementKelp> placeType) {
        super(defaultConfType, placeType);
    }

    public ClusterKelp() {
        this(ConfigKelp.class, ActorPlacementKelp.class);
    }

    @Override

    protected void deployPrimaryInitSystem() {
        primary.log("primary %s: create system with serializer %s", primary.getDeploymentConfig().getAddress(), primary.getDeploymentConfig().kryoBuilderType);
        system = createSystemKelp(primary.getDeploymentConfig().kryoBuilderType, primary.getDeploymentConfig());
        system.getLocalSystem().setLogger(new ConfigBase.SystemLoggerHeader(system.getLogger(), primary.getAppConfig()));
    }

    public static ActorSystemKelp createSystemKelp(String buildType, ConfigDeployment configDeployment) {
        try {
            Class<?> cls = Class.forName(buildType);
            if (KryoBuilder.class.isAssignableFrom(cls)) {
                return new ActorSystemKelp(configDeployment);
            } else {
                throw new RuntimeException("not a KryoBuilder: " + cls);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Class<?> getNodeMainType() {
        return NodeMainKelp.class;
    }

    public static class NodeMainKelp extends NodeMain {
        public static void main(String[] args) throws Exception {
            new NodeMainKelp().run(args);
        }

        @Override
        protected ActorSystemRemote initSystem() {
            ActorSystemRemote system = createSystemKelp(kryoBuilderType, this.configDeployment);
            system.getLocalSystem().setLogger(new ConfigBase.SystemLoggerHeader(system.getLogger(), configDeployment));
            return system;
        }
    }
}
