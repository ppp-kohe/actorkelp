package csl.actor.example.keyaggregate;

import csl.actor.ActorSystemDefault;
import csl.actor.example.delayedlabel.ActorToGraph;
import csl.actor.keyaggregate.KeyHistograms;

import java.io.File;
import java.util.*;

public class KeyHistogramsTest {
    public static void main(String[] args) {
        new KeyHistogramsTest().testPut();
        new KeyHistogramsTest().testSplit();
        new KeyHistogramsTest().testMerge();
    }

    public void testPut() {
        KeyHistograms.HistogramTree tree = new KeyHistograms.HistogramTree(this::compare, 3);

        KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
        ctx.putValue = "<aaa>";
        ctx.putPosition = 0;
        ctx.putRequiredSize = 2;
        tree.put("aaa", ctx);
        {
            assertEquals(1L, tree.getLeafSize());
            assertEquals(1L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof KeyHistograms.HistogramNodeLeafMap);
            assertEquals("aaa", tree.getRoot().keyStart());
            assertEquals("<aaa>", ((KeyHistograms.HistogramNodeLeafMap) tree.getRoot()).getValues().get(0)
                    .iterator().next());
            assertEquals(1L, tree.getRoot().size());
            assertEquals(0, tree.getRoot().height());
        }

        ctx.putValue = "<bbb>";
        tree.put("bbb", ctx);
        {
            assertEquals(2L, tree.getLeafSize());
            assertEquals(2L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof KeyHistograms.HistogramNodeTree);
            KeyHistograms.HistogramNodeTree root = (KeyHistograms.HistogramNodeTree) tree.getRoot();
            assertEquals(2L, root.size());
            assertEquals(1, root.height());
            assertEquals(2, root.getChildren().size());
            assertEquals("aaa", root.keyStart());
            assertEquals("bbb", root.keyEnd());

            KeyHistograms.HistogramNode child0 = root.getChildren().get(0);
            KeyHistograms.HistogramNode child1 = root.getChildren().get(1);
            assertTrue(child0 instanceof KeyHistograms.HistogramNodeLeafMap);
            assertTrue(child1 instanceof KeyHistograms.HistogramNodeLeafMap);
            assertEquals("aaa", child0.keyStart());
            assertEquals("<aaa>", ((KeyHistograms.HistogramNodeLeafMap) child0).getValues().get(0)
                    .iterator().next());
            assertEquals(1L, child0.size());
            assertEquals(0, child0.height());

            assertEquals("bbb", child1.keyStart());
            assertEquals("<bbb>", ((KeyHistograms.HistogramNodeLeafMap) child1).getValues().get(0)
                    .iterator().next());
            assertEquals(1L, child1.size());
            assertEquals(0, child1.height());
        }

        ctx.putValue = "<___>";
        tree.put("___", ctx);
        {
            assertEquals(3L, tree.getLeafSize());
            assertEquals(3L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof KeyHistograms.HistogramNodeTree);
            KeyHistograms.HistogramNodeTree root = (KeyHistograms.HistogramNodeTree) tree.getRoot();
            assertEquals(3L, root.size());
            assertEquals(1, root.height());
            assertEquals(3, root.getChildren().size());
            assertEquals("___", root.keyStart());
            assertEquals("bbb", root.keyEnd());

            KeyHistograms.HistogramNode child0 = root.getChildren().get(0);
            KeyHistograms.HistogramNode child1 = root.getChildren().get(1);
            KeyHistograms.HistogramNode child2 = root.getChildren().get(2);
            assertTrue(child0 instanceof KeyHistograms.HistogramNodeLeafMap);
            assertTrue(child1 instanceof KeyHistograms.HistogramNodeLeafMap);
            assertTrue(child2 instanceof KeyHistograms.HistogramNodeLeafMap);
            assertEquals("___", child0.keyStart());
            assertEquals("<___>", ((KeyHistograms.HistogramNodeLeafMap) child0).getValues().get(0)
                    .iterator().next());
            assertEquals(1L, child0.size());
            assertEquals(0, child0.height());

            assertEquals("aaa", child1.keyStart());
            assertEquals("<aaa>", ((KeyHistograms.HistogramNodeLeafMap) child1).getValues().get(0)
                    .iterator().next());
            assertEquals(1L, child1.size());
            assertEquals(0, child1.height());

            assertEquals("bbb", child2.keyStart());
            assertEquals("<bbb>", ((KeyHistograms.HistogramNodeLeafMap) child2).getValues().get(0)
                    .iterator().next());
            assertEquals(1L, child2.size());
            assertEquals(0, child2.height());
        }

        ctx.putValue = "<ab>";
        tree.put("ab", ctx);
        {
            assertEquals(4L, tree.getLeafSize());
            assertEquals(4L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof KeyHistograms.HistogramNodeTree);
            KeyHistograms.HistogramNodeTree root = (KeyHistograms.HistogramNodeTree) tree.getRoot();
            assertEquals(4L, root.size());
            assertEquals(2, root.height());
            assertEquals(2, root.getChildren().size());
            assertEquals("___", root.keyStart());
            assertEquals("bbb", root.keyEnd());

            KeyHistograms.HistogramNode child0 = root.getChildren().get(0);
            KeyHistograms.HistogramNode child1 = root.getChildren().get(1);
            assertTrue(child0 instanceof KeyHistograms.HistogramNodeTree);
            assertTrue(child1 instanceof KeyHistograms.HistogramNodeTree);
            KeyHistograms.HistogramNodeTree  cn0 = (KeyHistograms.HistogramNodeTree) child0;
            assertEquals(2L, cn0.size());
            assertEquals(1, cn0.height());
            assertEquals(2, cn0.getChildren().size());
            assertEquals("___", cn0.keyStart());
            assertEquals("aaa", cn0.keyEnd());

            KeyHistograms.HistogramNodeTree  cn1 = (KeyHistograms.HistogramNodeTree) child1;
            assertEquals(2L, cn1.size());
            assertEquals(1, cn1.height());
            assertEquals(2, cn1.getChildren().size());
            assertEquals("ab", cn1.keyStart());
            assertEquals("bbb", cn1.keyEnd());


            KeyHistograms.HistogramNode child2 = cn0.getChildren().get(0);
            assertTrue(child2 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child3 = cn0.getChildren().get(1);
            assertTrue(child3 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("___", child2.keyStart());
            assertEquals("<___>", ((KeyHistograms.HistogramNodeLeafMap) child2).getValues().get(0)
                    .iterator().next());
            assertEquals(1L, child2.size());
            assertEquals(0, child2.height());

            assertEquals("aaa", child3.keyStart());
            assertEquals("<aaa>", ((KeyHistograms.HistogramNodeLeafMap) child3).getValues().get(0)
                    .iterator().next());
            assertEquals(1L, child3.size());
            assertEquals(0, child3.height());

            KeyHistograms.HistogramNode child4 = cn1.getChildren().get(0);
            assertTrue(child4 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child5 = cn1.getChildren().get(1);
            assertTrue(child5 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("ab", child4.keyStart());
            assertEquals("<ab>", ((KeyHistograms.HistogramNodeLeafMap) child4).getValues().get(0)
                    .iterator().next());
            assertEquals(1L, child4.size());
            assertEquals(0, child4.height());

            assertEquals("bbb", child5.keyStart());
            assertEquals("<bbb>", ((KeyHistograms.HistogramNodeLeafMap) child5).getValues().get(0)
                    .iterator().next());
            assertEquals(1L, child5.size());
            assertEquals(0, child5.height());
        }

        ctx.putValue = "<ab2>";
        tree.put("ab", ctx);
        {
            assertEquals(4L, tree.getLeafSize());
            assertEquals(4L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof KeyHistograms.HistogramNodeTree);
            KeyHistograms.HistogramNodeTree root = (KeyHistograms.HistogramNodeTree) tree.getRoot();
            assertEquals(5L, root.size());
            assertEquals(2, root.height());
            assertEquals(2, root.getChildren().size());
            assertEquals("___", root.keyStart());
            assertEquals("bbb", root.keyEnd());

            KeyHistograms.HistogramNode child0 = root.getChildren().get(0);
            KeyHistograms.HistogramNode child1 = root.getChildren().get(1);
            assertTrue(child0 instanceof KeyHistograms.HistogramNodeTree);
            assertTrue(child1 instanceof KeyHistograms.HistogramNodeTree);
            KeyHistograms.HistogramNodeTree  cn0 = (KeyHistograms.HistogramNodeTree) child0;
            assertEquals(2L, cn0.size());
            assertEquals(1, cn0.height());
            assertEquals(2, cn0.getChildren().size());
            assertEquals("___", cn0.keyStart());
            assertEquals("aaa", cn0.keyEnd());

            KeyHistograms.HistogramNodeTree  cn1 = (KeyHistograms.HistogramNodeTree) child1;
            assertEquals(3L, cn1.size());
            assertEquals(1, cn1.height());
            assertEquals(2, cn1.getChildren().size());
            assertEquals("ab", cn1.keyStart());
            assertEquals("bbb", cn1.keyEnd());

            KeyHistograms.HistogramNode child2 = cn0.getChildren().get(0);
            assertTrue(child2 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child3 = cn0.getChildren().get(1);
            assertTrue(child3 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("___", child2.keyStart());
            assertEquals(Collections.singletonList("<___>"), values(child2, 0));
            assertEquals(1L, child2.size());
            assertEquals(0, child2.height());

            assertEquals("aaa", child3.keyStart());
            assertEquals(Collections.singletonList("<aaa>"), values(child3, 0));
            assertEquals(1L, child3.size());
            assertEquals(0, child3.height());

            KeyHistograms.HistogramNode child4 = cn1.getChildren().get(0);
            assertTrue(child4 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child5 = cn1.getChildren().get(1);
            assertTrue(child5 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("ab", child4.keyStart());
            assertEquals(Arrays.asList("<ab>", "<ab2>"), values(child4, 0));
            assertEquals(2L, child4.size());
            assertEquals(0, child4.height());

            assertEquals("bbb", child5.keyStart());
            assertEquals(Collections.singletonList("<bbb>"), values(child5, 0));
            assertEquals(1L, child5.size());
            assertEquals(0, child5.height());
        }

        ctx.putValue = "<ccc>";
        tree.put("ccc", ctx);
        {
            assertEquals(5L, tree.getLeafSize());
            assertEquals(5L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof KeyHistograms.HistogramNodeTree);
            KeyHistograms.HistogramNodeTree root = (KeyHistograms.HistogramNodeTree) tree.getRoot();
            assertEquals(6L, root.size());
            assertEquals(2, root.height());
            assertEquals(2, root.getChildren().size());
            assertEquals("___", root.keyStart());
            assertEquals("ccc", root.keyEnd());

            KeyHistograms.HistogramNode child0 = root.getChildren().get(0);
            KeyHistograms.HistogramNode child1 = root.getChildren().get(1);
            assertTrue(child0 instanceof KeyHistograms.HistogramNodeTree);
            assertTrue(child1 instanceof KeyHistograms.HistogramNodeTree);
            KeyHistograms.HistogramNodeTree  cn0 = (KeyHistograms.HistogramNodeTree) child0;
            assertEquals(2L, cn0.size());
            assertEquals(1, cn0.height());
            assertEquals(2, cn0.getChildren().size());
            assertEquals("___", cn0.keyStart());
            assertEquals("aaa", cn0.keyEnd());

            KeyHistograms.HistogramNodeTree  cn1 = (KeyHistograms.HistogramNodeTree) child1;
            assertEquals(4L, cn1.size());
            assertEquals(1, cn1.height());
            assertEquals(3, cn1.getChildren().size());
            assertEquals("ab", cn1.keyStart());
            assertEquals("ccc", cn1.keyEnd());

            KeyHistograms.HistogramNode child2 = cn0.getChildren().get(0);
            assertTrue(child2 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child3 = cn0.getChildren().get(1);
            assertTrue(child3 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("___", child2.keyStart());
            assertEquals(Collections.singletonList("<___>"), values(child2, 0));
            assertEquals(1L, child2.size());
            assertEquals(0, child2.height());

            assertEquals("aaa", child3.keyStart());
            assertEquals(Collections.singletonList("<aaa>"), values(child3, 0));
            assertEquals(1L, child3.size());
            assertEquals(0, child3.height());

            KeyHistograms.HistogramNode child4 = cn1.getChildren().get(0);
            assertTrue(child4 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child5 = cn1.getChildren().get(1);
            assertTrue(child5 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child6 = cn1.getChildren().get(2);
            assertTrue(child6 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("ab", child4.keyStart());
            assertEquals(Arrays.asList("<ab>", "<ab2>"), values(child4, 0));
            assertEquals(2L, child4.size());
            assertEquals(0, child4.height());

            assertEquals("bbb", child5.keyStart());
            assertEquals(Collections.singletonList("<bbb>"), values(child5, 0));
            assertEquals(1L, child5.size());
            assertEquals(0, child5.height());

            assertEquals("ccc", child6.keyStart());
            assertEquals(Collections.singletonList("<ccc>"), values(child6, 0));
            assertEquals(1L, child6.size());
            assertEquals(0, child6.height());
        }

        ctx.putValue = "<ddd>";
        tree.put("ddd", ctx);
        {
            assertEquals(6L, tree.getLeafSize());
            assertEquals(6L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof KeyHistograms.HistogramNodeTree);
            KeyHistograms.HistogramNodeTree root = (KeyHistograms.HistogramNodeTree) tree.getRoot();
            assertEquals(7L, root.size());
            assertEquals(2, root.height());
            assertEquals(3, root.getChildren().size());
            assertEquals("___", root.keyStart());
            assertEquals("ddd", root.keyEnd());

            KeyHistograms.HistogramNode child0 = root.getChildren().get(0);
            KeyHistograms.HistogramNode child1 = root.getChildren().get(1);
            KeyHistograms.HistogramNode child2 = root.getChildren().get(2);
            assertTrue(child0 instanceof KeyHistograms.HistogramNodeTree);
            assertTrue(child1 instanceof KeyHistograms.HistogramNodeTree);
            assertTrue(child2 instanceof KeyHistograms.HistogramNodeTree);
            KeyHistograms.HistogramNodeTree  cn0 = (KeyHistograms.HistogramNodeTree) child0;
            assertEquals(2L, cn0.size());
            assertEquals(1, cn0.height());
            assertEquals(2, cn0.getChildren().size());
            assertEquals("___", cn0.keyStart());
            assertEquals("aaa", cn0.keyEnd());

            KeyHistograms.HistogramNodeTree  cn1 = (KeyHistograms.HistogramNodeTree) child1;
            assertEquals(3L, cn1.size());
            assertEquals(1, cn1.height());
            assertEquals(2, cn1.getChildren().size());
            assertEquals("ab", cn1.keyStart());
            assertEquals("bbb", cn1.keyEnd());

            KeyHistograms.HistogramNodeTree  cn2 = (KeyHistograms.HistogramNodeTree) child2;
            assertEquals(2L, cn2.size());
            assertEquals(1, cn2.height());
            assertEquals(2, cn2.getChildren().size());
            assertEquals("ccc", cn2.keyStart());
            assertEquals("ddd", cn2.keyEnd());

            KeyHistograms.HistogramNode child3 = cn0.getChildren().get(0);
            assertTrue(child3 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child4 = cn0.getChildren().get(1);
            assertTrue(child4 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("___", child3.keyStart());
            assertEquals(Collections.singletonList("<___>"), values(child3, 0));
            assertEquals(1L, child3.size());
            assertEquals(0, child3.height());

            assertEquals("aaa", child4.keyStart());
            assertEquals(Collections.singletonList("<aaa>"), values(child4, 0));
            assertEquals(1L, child4.size());
            assertEquals(0, child4.height());

            KeyHistograms.HistogramNode child5 = cn1.getChildren().get(0);
            assertTrue(child5 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child6 = cn1.getChildren().get(1);
            assertTrue(child6 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child7 = cn2.getChildren().get(0);
            assertTrue(child7 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child8 = cn2.getChildren().get(1);
            assertTrue(child8 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("ab", child5.keyStart());
            assertEquals(Arrays.asList("<ab>", "<ab2>"), values(child5, 0));
            assertEquals(2L, child5.size());
            assertEquals(0, child5.height());

            assertEquals("bbb", child6.keyStart());
            assertEquals(Collections.singletonList("<bbb>"), values(child6, 0));
            assertEquals(1L, child6.size());
            assertEquals(0, child6.height());

            assertEquals("ccc", child7.keyStart());
            assertEquals(Collections.singletonList("<ccc>"), values(child7, 0));
            assertEquals(1L, child7.size());
            assertEquals(0, child7.height());

            assertEquals("ddd", child8.keyStart());
            assertEquals(Collections.singletonList("<ddd>"), values(child8, 0));
            assertEquals(1L, child8.size());
            assertEquals(0, child8.height());
        }


        ctx.putValue = "<ddd2>";
        ctx.putPosition = 1;
        tree.put("ddd", ctx);
        KeyHistograms.HistogramNodeLeafMap l = (KeyHistograms.HistogramNodeLeafMap) tree.takeCompleted();
        Object[] vs = l.take(2, tree);
        {
            assertEquals(6L, tree.getLeafSize());
            assertEquals(5L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof KeyHistograms.HistogramNodeTree);
            KeyHistograms.HistogramNodeTree root = (KeyHistograms.HistogramNodeTree) tree.getRoot();
            assertEquals(6L, root.size());
            assertEquals(2, root.height());
            assertEquals(3, root.getChildren().size());
            assertEquals("___", root.keyStart());
            assertEquals("ddd", root.keyEnd());

            KeyHistograms.HistogramNode child0 = root.getChildren().get(0);
            KeyHistograms.HistogramNode child1 = root.getChildren().get(1);
            KeyHistograms.HistogramNode child2 = root.getChildren().get(2);
            assertTrue(child0 instanceof KeyHistograms.HistogramNodeTree);
            assertTrue(child1 instanceof KeyHistograms.HistogramNodeTree);
            assertTrue(child2 instanceof KeyHistograms.HistogramNodeTree);
            KeyHistograms.HistogramNodeTree  cn0 = (KeyHistograms.HistogramNodeTree) child0;
            assertEquals(2L, cn0.size());
            assertEquals(1, cn0.height());
            assertEquals(2, cn0.getChildren().size());
            assertEquals("___", cn0.keyStart());
            assertEquals("aaa", cn0.keyEnd());

            KeyHistograms.HistogramNodeTree  cn1 = (KeyHistograms.HistogramNodeTree) child1;
            assertEquals(3L, cn1.size());
            assertEquals(1, cn1.height());
            assertEquals(2, cn1.getChildren().size());
            assertEquals("ab", cn1.keyStart());
            assertEquals("bbb", cn1.keyEnd());

            KeyHistograms.HistogramNodeTree  cn2 = (KeyHistograms.HistogramNodeTree) child2;
            assertEquals(1L, cn2.size());
            assertEquals(1, cn2.height());
            assertEquals(2, cn2.getChildren().size());
            assertEquals("ccc", cn2.keyStart());
            assertEquals("ddd", cn2.keyEnd());

            KeyHistograms.HistogramNode child3 = cn0.getChildren().get(0);
            assertTrue(child3 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child4 = cn0.getChildren().get(1);
            assertTrue(child4 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("___", child3.keyStart());
            assertEquals(Collections.singletonList("<___>"), values(child3, 0));
            assertEquals(1L, child3.size());
            assertEquals(0, child3.height());

            assertEquals("aaa", child4.keyStart());
            assertEquals(Collections.singletonList("<aaa>"), values(child4, 0));
            assertEquals(1L, child4.size());
            assertEquals(0, child4.height());

            KeyHistograms.HistogramNode child5 = cn1.getChildren().get(0);
            assertTrue(child5 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child6 = cn1.getChildren().get(1);
            assertTrue(child6 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child7 = cn2.getChildren().get(0);
            assertTrue(child7 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramNode child8 = cn2.getChildren().get(1);
            assertTrue(child8 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("ab", child5.keyStart());
            assertEquals(Arrays.asList("<ab>", "<ab2>"), values(child5, 0));
            assertEquals(2L, child5.size());
            assertEquals(0, child5.height());

            assertEquals("bbb", child6.keyStart());
            assertEquals(Collections.singletonList("<bbb>"), values(child6, 0));
            assertEquals(1L, child6.size());
            assertEquals(0, child6.height());

            assertEquals("ccc", child7.keyStart());
            assertEquals(Collections.singletonList("<ccc>"), values(child7, 0));
            assertEquals(1L, child7.size());
            assertEquals(0, child7.height());

            assertEquals("ddd", child8.keyStart());
            assertEquals(Collections.emptyList(), values(child8, 0));
            assertEquals(0L, child8.size());
            assertEquals(0, child8.height());


            assertEquals(Arrays.asList("<ddd>", "<ddd2>"), Arrays.asList(vs));
        }

        try (ActorSystemDefault sys = new ActorSystemDefault()) {
            ActorToGraph save = new ActorToGraph(sys, new File("target/msgassoc-debug/put.dot"), null);
            save.save(null, tree, 0);
            save.finish();
        }
    }

    public void testSplit() {

        KeyHistograms.HistogramTree tree = new KeyHistograms.HistogramTree(this::compare, 3);

        KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
        ctx.putValue = "<aaa>";
        ctx.putPosition = 0;
        ctx.putRequiredSize = 2;
        tree.put("aaa", ctx);

        ctx.putValue = "<bbb>";
        tree.put("bbb", ctx);

        ctx.putValue = "<___>";
        tree.put("___", ctx);

        ctx.putValue = "<ab>";
        tree.put("ab", ctx);

        ctx.putValue = "<ab2>";
        tree.put("ab", ctx);

        ctx.putValue = "<ccc>";
        tree.put("ccc", ctx);

        ctx.putValue = "<ddd>";
        tree.put("ddd", ctx);

        ctx.putValue = "<ddd2>";
        ctx.putPosition = 1;
        tree.put("ddd", ctx);

        ctx.putValue = "<aaa2>";
        ctx.putPosition = 1;
        tree.put("aaa", ctx);

        ctx.putValue = "<ccc2>";
        ctx.putPosition = 0;
        tree.put("ccc", ctx);
        ctx.putValue = "<ccc3>";
        ctx.putPosition = 0;
        tree.put("ccc", ctx);
        ctx.putValue = "<ccc4>";
        ctx.putPosition = 0;
        tree.put("ccc", ctx);

        try (ActorSystemDefault sys = new ActorSystemDefault()) {
            ActorToGraph save = new ActorToGraph(sys, new File("target/msgassoc-debug/split-before.dot"), null);
            save.save(null, tree, 0);
            save.finish();
        }


        KeyHistograms.HistogramTree tree2 = tree.split();
        {
            {
                assertEquals(3L, tree2.getLeafSize());
                assertEquals(3L, tree2.getLeafSizeNonZero());

                assertTrue(tree2.getRoot() instanceof KeyHistograms.HistogramNodeTree);
                KeyHistograms.HistogramNodeTree root = (KeyHistograms.HistogramNodeTree) tree2.getRoot();
                assertEquals(5L, root.size());
                assertEquals(2, root.height());
                assertEquals(2, root.getChildren().size());
                assertEquals("___", root.keyStart());
                assertEquals("ab", root.keyEnd());


                KeyHistograms.HistogramNode child0 = root.getChildren().get(0);
                KeyHistograms.HistogramNode child1 = root.getChildren().get(1);
                assertTrue(child0 instanceof KeyHistograms.HistogramNodeTree);
                assertTrue(child1 instanceof KeyHistograms.HistogramNodeTree);
                KeyHistograms.HistogramNodeTree cn0 = (KeyHistograms.HistogramNodeTree) child0;
                assertEquals(3L, cn0.size());
                assertEquals(1, cn0.height());
                assertEquals(2, cn0.getChildren().size());
                assertEquals("___", cn0.keyStart());
                assertEquals("aaa", cn0.keyEnd());

                KeyHistograms.HistogramNodeTree cn1 = (KeyHistograms.HistogramNodeTree) child1;
                assertEquals(2L, cn1.size());
                assertEquals(1, cn1.height());
                assertEquals(1, cn1.getChildren().size());
                assertEquals("ab", cn1.keyStart());
                assertEquals("ab", cn1.keyEnd());

                assertEquals(Arrays.asList(cn0.getChildren().get(1)), tree2.getCompleted());


            }
            {
                assertEquals(3L, tree2.getLeafSize());
                assertEquals(3L, tree2.getLeafSizeNonZero());
                assertTrue(tree.getRoot() instanceof KeyHistograms.HistogramNodeTree);
                KeyHistograms.HistogramNodeTree root = (KeyHistograms.HistogramNodeTree) tree.getRoot();
                assertEquals(7L, root.size());
                assertEquals(2, root.height());
                assertEquals(2, root.getChildren().size());
                assertEquals("bbb", root.keyStart());
                assertEquals("ddd", root.keyEnd());


                KeyHistograms.HistogramNode child0 = root.getChildren().get(0);
                KeyHistograms.HistogramNode child1 = root.getChildren().get(1);
                assertTrue(child0 instanceof KeyHistograms.HistogramNodeTree);
                assertTrue(child1 instanceof KeyHistograms.HistogramNodeTree);
                KeyHistograms.HistogramNodeTree cn0 = (KeyHistograms.HistogramNodeTree) child0;
                assertEquals(1L, cn0.size());
                assertEquals(1, cn0.height());
                assertEquals(1, cn0.getChildren().size());
                assertEquals("bbb", cn0.keyStart());
                assertEquals("bbb", cn0.keyEnd());

                KeyHistograms.HistogramNodeTree cn1 = (KeyHistograms.HistogramNodeTree) child1;
                assertEquals(6L, cn1.size());
                assertEquals(1, cn1.height());
                assertEquals(2, cn1.getChildren().size());
                assertEquals("ccc", cn1.keyStart());
                assertEquals("ddd", cn1.keyEnd());

                assertEquals(Arrays.asList(cn1.getChildren().get(1)), tree.getCompleted());
            }
        }


        try (ActorSystemDefault sys = new ActorSystemDefault()) {
            ActorToGraph save = new ActorToGraph(sys, new File("target/msgassoc-debug/split.dot"), null);
            save.save(null, tree2, 0);
            save.save(null, tree, 1);
            save.finish();
        }
    }

    public void testMerge() {
        KeyHistograms.HistogramTree tree = new KeyHistograms.HistogramTree(this::compare, 3);

        KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
        ctx.putValue = "<aaa>";
        ctx.putPosition = 0;
        ctx.putRequiredSize = 2;
        tree.put("aaa", ctx);

        ctx.putValue = "<bbb>";
        tree.put("bbb", ctx);

        ctx.putValue = "<___>";
        tree.put("___", ctx);

        ctx.putValue = "<ab>";
        tree.put("ab", ctx);

        ctx.putValue = "<ab2>";
        tree.put("ab", ctx);

        ctx.putValue = "<ccc>";
        tree.put("ccc", ctx);

        ctx.putValue = "<ddd>";
        tree.put("ddd", ctx);

        ctx.putValue = "<ddd2>";
        ctx.putPosition = 1;
        tree.put("ddd", ctx);

        ctx.putValue = "<aaa2>";
        ctx.putPosition = 1;
        tree.put("aaa", ctx);

        ctx.putValue = "<ccc2>";
        ctx.putPosition = 0;
        tree.put("ccc", ctx);
        ctx.putValue = "<ccc3>";
        ctx.putPosition = 0;
        tree.put("ccc", ctx);
        ctx.putValue = "<ccc4>";
        ctx.putPosition = 0;
        tree.put("ccc", ctx);

        KeyHistograms.HistogramTree tree2 = tree.split();
        tree.merge(tree2);

        KeyHistograms.HistogramTree tree3 = new KeyHistograms.HistogramTree(this::compare, 3);
        ctx.putValue = "<sss>";
        tree3.put("sss", ctx);
        ctx.putValue = "<ttt>";
        tree3.put("ttt", ctx);
        ctx.putValue = "<xxx>";
        tree3.put("xxx", ctx);
        ctx.putValue = "<yyy>";
        tree3.put("yyy", ctx);
        ctx.putValue = "<xxx>";
        tree3.put("zzz", ctx);

        tree3.merge(tree);



        KeyHistograms.HistogramTree tree4 = new KeyHistograms.HistogramTree(this::compare, 3);
        ctx.putValue = "<ooo>";
        tree4.put("ooo", ctx);
        ctx.putValue = "<ppp>";
        tree4.put("ppp", ctx);
        ctx.putValue = "<qqq>";
        tree4.put("qqq", ctx);
        ctx.putValue = "<rrr>";
        tree4.put("rrr", ctx);

        tree3.merge(tree4);

        try (ActorSystemDefault sys = new ActorSystemDefault()) {
            ActorToGraph save = new ActorToGraph(sys, new File("target/msgassoc-debug/merge.dot"), null)
                    .setSaveLeafNode(true);
            save.save(null, tree3, 0);
            save.finish();
        }
    }

    protected List<Object> values(KeyHistograms.HistogramNode node, Object pos) {
        List<Object> vs = new ArrayList<>();
        KeyHistograms.HistogramLeafList l = ((KeyHistograms.HistogramNodeLeafMap) node).getValues().get(pos);
        if (l != null) {
            l.iterator().forEachRemaining(vs::add);
        }
        return vs;
    }

    public int compare(Object a, Object b) {
        return ((String)a).compareTo((String) b);
    }

    public void assertEquals(Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            throw new RuntimeException("not-equals: " + expected + " : " + actual);
        }
    }

    public void assertTrue(boolean b) {
        if (!b) {
            throw new RuntimeException("expected true: " + b);
        }
    }
}
