package csl.actor.example.wordcount;

import csl.actor.ActorSystem;
import csl.actor.example.keyaggregate.ClusterDeployment;
import csl.actor.keyaggregate.ActorPlacementKeyAggregation;
import csl.actor.keyaggregate.Config;

public class WordCountCluster {
    public static void main(String[] args) throws Exception {
        String confFile = args[0];
        String inputFile = args[1];
        ClusterDeployment c = new ClusterDeployment();
        ActorPlacementKeyAggregation place = c.deploy(confFile);

        Config conf = c.getMaster().getAppConfig();
        ActorSystem system = c.getSystem();

        WordCount.FileMapper fileReader = new WordCount.FileMapper(system, "fileReader", conf, FileSplitter.getWithSplitCount(10));
        WordCount.WordCountMapper mapper = new WordCount.WordCountMapper(system, "mapper", conf);
        WordCount.WordCountReducer reducer = new WordCount.WordCountReducer(system, "reducer", conf, ".");

        place.connectAndSplitStage(fileReader, mapper, reducer).get();

        fileReader.startReadFile(inputFile).get();

        //TODO shutdown
    }
}
