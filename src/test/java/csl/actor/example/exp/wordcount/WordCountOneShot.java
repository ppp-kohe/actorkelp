package csl.actor.example.exp.wordcount;

import csl.actor.keyaggregate.ActorPlacementKeyAggregation;
import csl.actor.keyaggregate.ClusterKeyAggregation;
import csl.actor.keyaggregate.FileMapper;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Function;

public class WordCountOneShot {
    public static void main(String[] args) throws Exception {
        ClusterKeyAggregation cluster = ClusterKeyAggregation.create();
        ActorPlacementKeyAggregation place = cluster.deploy(args[0]);
        FileMapper fileMapper = place.fileMapperWithSplitCount(10);

        ActorPlacementKeyAggregation.ActorKeyAggregationOneShot reducer;
        place.connectStage(fileMapper,
                place.actor("mapper", (self, builder) ->
                        builder.match(String.class, line -> Arrays.stream(line.split("\\W+"))
                            .forEach(w -> self.nextStage().tell(w)))),
                reducer = place.actor("reducer", (self, builder) ->
                        builder.matchKey(String.class, Function.identity(), w -> new Tuple(w, 1))
                            .fold((w, ts) -> ts.stream().reduce(new Tuple(w, 0), Tuple::add))
                            .eventually()
                        .forEach(t -> self.writer().println(t))));
        //reducer.routerSplit(3);
        fileMapper.startReadFile(args[1]).get();

        cluster.shutdownAll();
    }

    public static class Tuple {
        public String word;
        public long count;

        public Tuple(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public Tuple add(Tuple a) {
            return new Tuple(word, count + a.count);
        }

        @Override
        public String toString() {
            return "(" + word + "," + count + ")";
        }
    }

    public static class Driver {
        public static void main(String[] args) throws Exception {
            Path inFile = Paths.get(args[0]).toAbsolutePath();
            String dir = inFile.getParent().toString();
            String debugFlag = "false";

            ClusterKeyAggregation d = ClusterKeyAggregation.create();
            ClusterKeyAggregation.run(Arrays.asList(d.master()
                            .edit(c -> c.getDeploymentConfig().baseDir = dir)
                            .edit(c -> c.getAppConfig().routerAutoMerge = false)
                            .edit(c -> c.getDeploymentConfig().httpHost = "0.0.0.0"),
                    d.node("localhost", 30001)
                            .edit(c -> c.getDeploymentConfig().baseDir = dir)
                            .edit(c -> c.getDeploymentConfig().java = "java -Dcsl.actor.debug=" + debugFlag + " -Dcsl.actor.debugMsg=" + debugFlag + " %s %s %s"),
                    d.node("localhost", 30002)
                            .edit(c -> c.getDeploymentConfig().baseDir = dir)
                            .edit(c -> c.getDeploymentConfig().java = "java -Dcsl.actor.debug=" + debugFlag + " -Dcsl.actor.debugMsg=" + debugFlag + " %s %s %s")),
                    WordCountOneShot.class.getName(),
                    inFile.getFileName().toString());
        }
    }
}
