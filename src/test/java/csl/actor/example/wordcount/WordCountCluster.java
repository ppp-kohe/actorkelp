package csl.actor.example.wordcount;

import csl.actor.ActorSystem;
import csl.actor.cluster.FileSplitter;
import csl.actor.keyaggregate.ActorPlacementKeyAggregation;
import csl.actor.keyaggregate.ClusterKeyAggregation;
import csl.actor.keyaggregate.Config;
import csl.actor.keyaggregate.FileMapper;

public class WordCountCluster {
    public static void main(String[] args) throws Exception {
        String confFile = args[0];
        String inputFile = args[1];
        ClusterKeyAggregation c = ClusterKeyAggregation.create();
        ActorPlacementKeyAggregation place = c.deploy(confFile);

        Config conf = c.getMaster().getAppConfig();
        ActorSystem system = c.getSystem();

        FileMapper fileReader = new FileMapper(system, "fileReader", conf, FileSplitter.getWithSplitCount(10));
        WordCount.WordCountMapper mapper = new WordCount.WordCountMapper(system, "mapper", conf);
        WordCount.WordCountReducer reducer = new WordCount.WordCountReducer(system, "reducer", conf, ".");

        place.connectAndSplitStage(fileReader, mapper, reducer).get();

        fileReader.startReadFile(inputFile).get();

        c.shutdownAll();
    }
}
