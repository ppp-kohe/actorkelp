package csl.actor.example.keyaggregate;

import csl.actor.keyaggregate.ClusterKeyAggregation;

import java.nio.file.Paths;

public class ExampleCluster {
    public static void main(String[] args) {
        String dir = Paths.get("").toAbsolutePath().toString() + "/target/debug";

        ClusterKeyAggregation d = new ClusterKeyAggregation();
        d.deploy(d.master()
                    .edit(c -> c.getDeploymentConfig().baseDir = dir),
                d.node("localhost", 30001)
                    .edit(c -> c.getDeploymentConfig().baseDir = dir)
                    .edit(c -> c.getDeploymentConfig().java = "java -Dcsl.actor.debugKryo=true %s %s %s"),
                d.node("localhost", 30002)
                    .edit(c -> c.getDeploymentConfig().baseDir = dir)
                    .edit(c -> c.getDeploymentConfig().java = "java -Dcsl.actor.debugKryo=true %s %s %s"));

    }
}
