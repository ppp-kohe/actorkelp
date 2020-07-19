package csl.actor.kelp2;

import csl.actor.cluster.ClusterDeployment;
import csl.actor.cluster.ConfigDeployment;
import csl.actor.remote.ActorSystemRemote;
import csl.actor.remote.KryoBuilder;
import csl.actor.util.ConfigBase;

public class ClusterKelp<ConfigType extends ConfigKelp> extends ClusterDeployment<ConfigType, ActorPlacementKelp<ConfigType>> {
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ClusterKelp(Class<ConfigType> defaultConfType, Class<ActorPlacementKelp> placeType) {
        super(defaultConfType, (Class<ActorPlacementKelp<ConfigType>>) (Class) placeType);
    }

    public static ClusterKelp<ConfigKelp> create() {
        return new ClusterKelp<>(ConfigKelp.class, ActorPlacementKelp.class);
    }

    @Override
    protected void deployPrimaryInitSystem() {
        primary.log("primary %s: create system with serializer %s", primary.getDeploymentConfig().getAddress(), primary.getDeploymentConfig().kryoBuilderType);
        system = createSystemKelpPrimary(primary.getDeploymentConfig().kryoBuilderType, primary.getDeploymentConfig());
        system.getLocalSystem().setLogger(new ConfigBase.SystemLoggerHeader(system.getLogger(), primary.getAppConfig()));
    }

    public ActorSystemKelp createSystemKelpPrimary(String buildType, ConfigDeployment configDeployment) {
        return createSystemKelp(buildType, configDeployment);
    }

    @SuppressWarnings("unchecked")
    public static ActorSystemKelp createSystemKelp(String buildType, ConfigDeployment configDeployment) {
        try {
            Class<?> cls = Class.forName(buildType);
            if (KryoBuilder.class.isAssignableFrom(cls)) {
                return ActorSystemKelp.createWithKryoBuilderType((Class<? extends KryoBuilder>) cls, configDeployment);
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
            ActorSystemRemote system = createSystemKelpNode(kryoBuilderType, this.configDeployment);
            system.getLocalSystem().setLogger(new ConfigBase.SystemLoggerHeader(system.getLogger(), configDeployment));
            return system;
        }

        public ActorSystemKelp createSystemKelpNode(String buildType, ConfigDeployment configDeployment) {
            return createSystemKelp(buildType, configDeployment);
        }
    }
}
