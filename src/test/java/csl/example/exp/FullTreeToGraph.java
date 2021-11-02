package csl.example.exp;

import csl.actor.kelp.ActorSystemKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.behavior.HistogramTree;
import csl.actor.kelp.behavior.HistogramTreeNodeTable;
import csl.actor.kelp.behavior.KeyHistograms;
import csl.actor.kelp.persist.KeyHistogramsPersistable;
import csl.actor.persist.PersistentFileManager;
import csl.example.kelp.TestToolActorToGraph;

import java.nio.file.Paths;

public class FullTreeToGraph {
    public static void main(String[] args) throws Exception {
        try (ActorSystemKelp.ActorSystemDefaultForKelp k = ActorSystemKelp.createLocal()) {
            var p = new KeyHistogramsPersistable(
                    new KeyHistogramsPersistable.HistogramTreePersistableConfigKelp(new ConfigKelp()),
                    PersistentFileManager.getPersistentFile(k, "."));
            HistogramTree tree = p.loadFullTree(p.getPersistent().createReaderSourceForPathExpanded(args[0]));
            trav(tree, tree.getRoot());
            TestToolActorToGraph.save(k, tree, Paths.get(args[1]).toFile());
        }
    }

    public static void trav(HistogramTree tree, KeyHistograms.HistogramTreeNode node) {
        if (node instanceof HistogramTreeNodeTable) {
            for (KeyHistograms.HistogramTreeNode c : ((HistogramTreeNodeTable) node).getChildren(tree)) {
                trav(tree, c);
            }
        } else {
            KeyHistograms.HistogramPutContextMap c = new KeyHistograms.HistogramPutContextMap();
            c.putTree = tree;
            node.load(c);
        }
    }
}
