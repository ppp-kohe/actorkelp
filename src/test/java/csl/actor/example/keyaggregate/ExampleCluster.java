package csl.actor.example.keyaggregate;

import java.nio.file.Paths;

public class ExampleCluster {
    public static void main(String[] args) {
        String dir = Paths.get("").toAbsolutePath().toString() + "/target/debug";

        ClusterDeployment d = new ClusterDeployment();
        d.deploy(ClusterDeployment.master()
                        .edit(c -> c.getClusterConfig().baseDir = dir),
                ClusterDeployment.node("localhost", 30001)
                        .edit(c -> c.getClusterConfig().baseDir = dir)
                        .edit(c -> c.getClusterConfig().java = "java -Dcsl.actor.debugKryo=true %s %s %s"),
                ClusterDeployment.node("localhost", 30002)
                        .edit(c -> c.getClusterConfig().baseDir = dir)
                        .edit(c -> c.getClusterConfig().java = "java -Dcsl.actor.debugKryo=true %s %s %s"));

    }
}
