package csl.actor.example.keyaggregate;

import csl.actor.keyaggregate.KeyAggregationCluster;

import java.nio.file.Paths;

public class ExampleCluster {
    public static void main(String[] args) {
        String dir = Paths.get("").toAbsolutePath().toString() + "/target/debug";

        KeyAggregationCluster d = new KeyAggregationCluster();
        d.deploy(KeyAggregationCluster.master()
                        .edit(c -> c.getDeploymentConfig().baseDir = dir),
                KeyAggregationCluster.node("localhost", 30001)
                        .edit(c -> c.getDeploymentConfig().baseDir = dir)
                        .edit(c -> c.getDeploymentConfig().java = "java -Dcsl.actor.debugKryo=true %s %s %s"),
                KeyAggregationCluster.node("localhost", 30002)
                        .edit(c -> c.getDeploymentConfig().baseDir = dir)
                        .edit(c -> c.getDeploymentConfig().java = "java -Dcsl.actor.debugKryo=true %s %s %s"));

    }
}
