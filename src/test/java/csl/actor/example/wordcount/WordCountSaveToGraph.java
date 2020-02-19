package csl.actor.example.wordcount;

import com.esotericsoftware.kryo.io.Input;
import csl.actor.ActorSystem;
import csl.actor.ActorSystemDefault;
import csl.actor.example.delayedlabel.ActorToGraph;
import csl.actor.msgassoc.KeyHistograms;
import csl.actor.remote.KryoBuilder;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class WordCountSaveToGraph {
    public static void main(String[] args) throws Exception {
        String dir = "target/debug-split";
        if (args.length > 1) {
            dir = args[0];
        }

        WordCountSaveToGraph w = new WordCountSaveToGraph();
        try (Stream<Path> ps = Files.list(Paths.get(dir))) {
            ps.filter(Files::isRegularFile)
                    .filter(f -> f.getFileName().toString().endsWith(".obj"))
                    .forEach(w::process);
        }

    }

    ActorSystemDefault system;
    KryoBuilder.SerializerPoolDefault pool;
    {
        system = new ActorSystemDefault();
        pool = new KryoBuilder.SerializerPoolDefault(system);
    }

    ActorToGraph g;

    void process(Path p) {
        Path outPath = p.getParent().resolve(p.getFileName() + ".dot");

        System.err.println(String.format("save %s -> %s", p, outPath.getFileName()));

        g = new ActorToGraph(system, outPath.toFile(), null);

        try (Input in = new Input(new FileInputStream(p.toFile()))) {
            Object o = pool.read(in);
            processObj(o);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        g.finish();
    }

    void processObj(Object o) {
        if (o instanceof Object[]) {
            for (Object c : ((Object[]) o)) {
                processObj(c);
            }
        } else if (o instanceof KeyHistograms.HistogramTree) {
            g.save(null, (KeyHistograms.HistogramTree) o, 0);
        } else {
            System.err.println("??? " + o);
        }
    }
}
