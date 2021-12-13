package csl.example.kelp;

import csl.actor.ActorSystemDefault;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.kelp.behavior.HistogramTreeNodeTable;
import csl.actor.kelp.behavior.HistogramTree;
import csl.actor.kelp.behavior.KeyHistograms;
import csl.example.TestTool;

import java.io.File;
import java.util.*;

public class ExampleKeyHistogramsTest {
    public static void main(String[] args) {
        new ExampleKeyHistogramsTest().testPut();
        new ExampleKeyHistogramsTest().testSplit();
        new ExampleKeyHistogramsTest().testMerge();
    }
    public void testPut() {
        System.err.println("testPut ----------------------");
        HistogramTree tree = new HistogramTree(this::compare, 3);

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
                    .iterator(tree, (KeyHistograms.HistogramNodeLeafMap) tree.getRoot()).next());
            assertEquals(1L, tree.getRoot().size());
            assertEquals(0, tree.getRoot().height());
        }

        ctx.putValue = "<bbb>";
        tree.put("bbb", ctx);
        {
            assertEquals(2L, tree.getLeafSize());
            assertEquals(2L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof HistogramTreeNodeTable);
            HistogramTreeNodeTable root = (HistogramTreeNodeTable) tree.getRoot();
            assertEquals(2L, root.size());
            assertEquals(1, root.height());
            assertEquals(2, root.getChildren(tree).size());
            assertEquals("aaa", root.keyStart());
            assertEquals("bbb", root.keyEnd());

            KeyHistograms.HistogramTreeNode child0 = root.getChildren(tree).get(0);
            KeyHistograms.HistogramTreeNode child1 = root.getChildren(tree).get(1);
            assertTrue(child0 instanceof KeyHistograms.HistogramNodeLeafMap);
            assertTrue(child1 instanceof KeyHistograms.HistogramNodeLeafMap);
            assertEquals("aaa", child0.keyStart());
            assertEquals("<aaa>", ((KeyHistograms.HistogramNodeLeafMap) child0).getValues().get(0)
                    .iterator(tree, (KeyHistograms.HistogramNodeLeafMap) child0).next());
            assertEquals(1L, child0.size());
            assertEquals(0, child0.height());

            assertEquals("bbb", child1.keyStart());
            assertEquals("<bbb>", ((KeyHistograms.HistogramNodeLeafMap) child1).getValues().get(0)
                    .iterator(tree, (KeyHistograms.HistogramNodeLeafMap) child1).next());
            assertEquals(1L, child1.size());
            assertEquals(0, child1.height());
        }

        ctx.putValue = "<___>";
        tree.put("___", ctx);
        {
            assertEquals(3L, tree.getLeafSize());
            assertEquals(3L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof HistogramTreeNodeTable);
            HistogramTreeNodeTable root = (HistogramTreeNodeTable) tree.getRoot();
            assertEquals(3L, root.size());
            assertEquals(1, root.height());
            assertEquals(3, root.getChildren(tree).size());
            assertEquals("___", root.keyStart());
            assertEquals("bbb", root.keyEnd());

            KeyHistograms.HistogramTreeNode child0 = root.getChildren(tree).get(0);
            KeyHistograms.HistogramTreeNode child1 = root.getChildren(tree).get(1);
            KeyHistograms.HistogramTreeNode child2 = root.getChildren(tree).get(2);
            assertTrue(child0 instanceof KeyHistograms.HistogramNodeLeafMap);
            assertTrue(child1 instanceof KeyHistograms.HistogramNodeLeafMap);
            assertTrue(child2 instanceof KeyHistograms.HistogramNodeLeafMap);
            assertEquals("___", child0.keyStart());
            assertEquals("<___>", ((KeyHistograms.HistogramNodeLeafMap) child0).getValues().get(0)
                    .iterator(tree, (KeyHistograms.HistogramNodeLeafMap) child0).next());
            assertEquals(1L, child0.size());
            assertEquals(0, child0.height());

            assertEquals("aaa", child1.keyStart());
            assertEquals("<aaa>", ((KeyHistograms.HistogramNodeLeafMap) child1).getValues().get(0)
                    .iterator(tree, (KeyHistograms.HistogramNodeLeafMap) child1).next());
            assertEquals(1L, child1.size());
            assertEquals(0, child1.height());

            assertEquals("bbb", child2.keyStart());
            assertEquals("<bbb>", ((KeyHistograms.HistogramNodeLeafMap) child2).getValues().get(0)
                    .iterator(tree, (KeyHistograms.HistogramNodeLeafMap) child2).next());
            assertEquals(1L, child2.size());
            assertEquals(0, child2.height());
        }

        ctx.putValue = "<ab>";
        tree.put("ab", ctx);
        {
            assertEquals(4L, tree.getLeafSize());
            assertEquals(4L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof HistogramTreeNodeTable);
            HistogramTreeNodeTable root = (HistogramTreeNodeTable) tree.getRoot();
            assertEquals(4L, root.size());
            assertEquals(2, root.height());
            assertEquals(2, root.getChildren(tree).size());
            assertEquals("___", root.keyStart());
            assertEquals("bbb", root.keyEnd());

            KeyHistograms.HistogramTreeNode child0 = root.getChildren(tree).get(0);
            KeyHistograms.HistogramTreeNode child1 = root.getChildren(tree).get(1);
            assertTrue(child0 instanceof HistogramTreeNodeTable);
            assertTrue(child1 instanceof HistogramTreeNodeTable);
            HistogramTreeNodeTable cn0 = (HistogramTreeNodeTable) child0;
            assertEquals(2L, cn0.size());
            assertEquals(1, cn0.height());
            assertEquals(2, cn0.getChildren(tree).size());
            assertEquals("___", cn0.keyStart());
            assertEquals("aaa", cn0.keyEnd());

            HistogramTreeNodeTable cn1 = (HistogramTreeNodeTable) child1;
            assertEquals(2L, cn1.size());
            assertEquals(1, cn1.height());
            assertEquals(2, cn1.getChildren(tree).size());
            assertEquals("ab", cn1.keyStart());
            assertEquals("bbb", cn1.keyEnd());


            KeyHistograms.HistogramTreeNode child2 = cn0.getChildren(tree).get(0);
            assertTrue(child2 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child3 = cn0.getChildren(tree).get(1);
            assertTrue(child3 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("___", child2.keyStart());
            assertEquals("<___>", ((KeyHistograms.HistogramNodeLeafMap) child2).getValues().get(0)
                    .iterator(tree, (KeyHistograms.HistogramNodeLeafMap) child2).next());
            assertEquals(1L, child2.size());
            assertEquals(0, child2.height());

            assertEquals("aaa", child3.keyStart());
            assertEquals("<aaa>", ((KeyHistograms.HistogramNodeLeafMap) child3).getValues().get(0)
                    .iterator(tree, (KeyHistograms.HistogramNodeLeafMap) child3).next());
            assertEquals(1L, child3.size());
            assertEquals(0, child3.height());

            KeyHistograms.HistogramTreeNode child4 = cn1.getChildren(tree).get(0);
            assertTrue(child4 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child5 = cn1.getChildren(tree).get(1);
            assertTrue(child5 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("ab", child4.keyStart());
            assertEquals("<ab>", ((KeyHistograms.HistogramNodeLeafMap) child4).getValues().get(0)
                    .iterator(tree, (KeyHistograms.HistogramNodeLeafMap) child4).next());
            assertEquals(1L, child4.size());
            assertEquals(0, child4.height());

            assertEquals("bbb", child5.keyStart());
            assertEquals("<bbb>", ((KeyHistograms.HistogramNodeLeafMap) child5).getValues().get(0)
                    .iterator(tree, (KeyHistograms.HistogramNodeLeafMap) child5).next());
            assertEquals(1L, child5.size());
            assertEquals(0, child5.height());
        }

        ctx.putValue = "<ab2>";
        tree.put("ab", ctx);
        {
            assertEquals(4L, tree.getLeafSize());
            assertEquals(4L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof HistogramTreeNodeTable);
            HistogramTreeNodeTable root = (HistogramTreeNodeTable) tree.getRoot();
            assertEquals(5L, root.size());
            assertEquals(2, root.height());
            assertEquals(2, root.getChildren(tree).size());
            assertEquals("___", root.keyStart());
            assertEquals("bbb", root.keyEnd());

            KeyHistograms.HistogramTreeNode child0 = root.getChildren(tree).get(0);
            KeyHistograms.HistogramTreeNode child1 = root.getChildren(tree).get(1);
            assertTrue(child0 instanceof HistogramTreeNodeTable);
            assertTrue(child1 instanceof HistogramTreeNodeTable);
            HistogramTreeNodeTable cn0 = (HistogramTreeNodeTable) child0;
            assertEquals(2L, cn0.size());
            assertEquals(1, cn0.height());
            assertEquals(2, cn0.getChildren(tree).size());
            assertEquals("___", cn0.keyStart());
            assertEquals("aaa", cn0.keyEnd());

            HistogramTreeNodeTable cn1 = (HistogramTreeNodeTable) child1;
            assertEquals(3L, cn1.size());
            assertEquals(1, cn1.height());
            assertEquals(2, cn1.getChildren(tree).size());
            assertEquals("ab", cn1.keyStart());
            assertEquals("bbb", cn1.keyEnd());

            KeyHistograms.HistogramTreeNode child2 = cn0.getChildren(tree).get(0);
            assertTrue(child2 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child3 = cn0.getChildren(tree).get(1);
            assertTrue(child3 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("___", child2.keyStart());
            assertEquals(Collections.singletonList("<___>"), values(tree, child2, 0));
            assertEquals(1L, child2.size());
            assertEquals(0, child2.height());

            assertEquals("aaa", child3.keyStart());
            assertEquals(Collections.singletonList("<aaa>"), values(tree, child3, 0));
            assertEquals(1L, child3.size());
            assertEquals(0, child3.height());

            KeyHistograms.HistogramTreeNode child4 = cn1.getChildren(tree).get(0);
            assertTrue(child4 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child5 = cn1.getChildren(tree).get(1);
            assertTrue(child5 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("ab", child4.keyStart());
            assertEquals(Arrays.asList("<ab>", "<ab2>"), values(tree, child4, 0));
            assertEquals(2L, child4.size());
            assertEquals(0, child4.height());

            assertEquals("bbb", child5.keyStart());
            assertEquals(Collections.singletonList("<bbb>"), values(tree, child5, 0));
            assertEquals(1L, child5.size());
            assertEquals(0, child5.height());
        }

        ctx.putValue = "<ccc>";
        tree.put("ccc", ctx);
        {
            assertEquals(5L, tree.getLeafSize());
            assertEquals(5L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof HistogramTreeNodeTable);
            HistogramTreeNodeTable root = (HistogramTreeNodeTable) tree.getRoot();
            assertEquals(6L, root.size());
            assertEquals(2, root.height());
            assertEquals(2, root.getChildren(tree).size());
            assertEquals("___", root.keyStart());
            assertEquals("ccc", root.keyEnd());

            KeyHistograms.HistogramTreeNode child0 = root.getChildren(tree).get(0);
            KeyHistograms.HistogramTreeNode child1 = root.getChildren(tree).get(1);
            assertTrue(child0 instanceof HistogramTreeNodeTable);
            assertTrue(child1 instanceof HistogramTreeNodeTable);
            HistogramTreeNodeTable cn0 = (HistogramTreeNodeTable) child0;
            assertEquals(2L, cn0.size());
            assertEquals(1, cn0.height());
            assertEquals(2, cn0.getChildren(tree).size());
            assertEquals("___", cn0.keyStart());
            assertEquals("aaa", cn0.keyEnd());

            HistogramTreeNodeTable cn1 = (HistogramTreeNodeTable) child1;
            assertEquals(4L, cn1.size());
            assertEquals(1, cn1.height());
            assertEquals(3, cn1.getChildren(tree).size());
            assertEquals("ab", cn1.keyStart());
            assertEquals("ccc", cn1.keyEnd());

            KeyHistograms.HistogramTreeNode child2 = cn0.getChildren(tree).get(0);
            assertTrue(child2 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child3 = cn0.getChildren(tree).get(1);
            assertTrue(child3 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("___", child2.keyStart());
            assertEquals(Collections.singletonList("<___>"), values(tree, child2, 0));
            assertEquals(1L, child2.size());
            assertEquals(0, child2.height());

            assertEquals("aaa", child3.keyStart());
            assertEquals(Collections.singletonList("<aaa>"), values(tree, child3, 0));
            assertEquals(1L, child3.size());
            assertEquals(0, child3.height());

            KeyHistograms.HistogramTreeNode child4 = cn1.getChildren(tree).get(0);
            assertTrue(child4 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child5 = cn1.getChildren(tree).get(1);
            assertTrue(child5 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child6 = cn1.getChildren(tree).get(2);
            assertTrue(child6 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("ab", child4.keyStart());
            assertEquals(Arrays.asList("<ab>", "<ab2>"), values(tree, child4, 0));
            assertEquals(2L, child4.size());
            assertEquals(0, child4.height());

            assertEquals("bbb", child5.keyStart());
            assertEquals(Collections.singletonList("<bbb>"), values(tree, child5, 0));
            assertEquals(1L, child5.size());
            assertEquals(0, child5.height());

            assertEquals("ccc", child6.keyStart());
            assertEquals(Collections.singletonList("<ccc>"), values(tree, child6, 0));
            assertEquals(1L, child6.size());
            assertEquals(0, child6.height());
        }

        ctx.putValue = "<ddd>";
        tree.put("ddd", ctx);
        {
            assertEquals(6L, tree.getLeafSize());
            assertEquals(6L, tree.getLeafSizeNonZero());

            assertTrue(tree.getRoot() instanceof HistogramTreeNodeTable);
            HistogramTreeNodeTable root = (HistogramTreeNodeTable) tree.getRoot();
            assertEquals(7L, root.size());
            assertEquals(2, root.height());
            assertEquals(3, root.getChildren(tree).size());
            assertEquals("___", root.keyStart());
            assertEquals("ddd", root.keyEnd());

            KeyHistograms.HistogramTreeNode child0 = root.getChildren(tree).get(0);
            KeyHistograms.HistogramTreeNode child1 = root.getChildren(tree).get(1);
            KeyHistograms.HistogramTreeNode child2 = root.getChildren(tree).get(2);
            assertTrue(child0 instanceof HistogramTreeNodeTable);
            assertTrue(child1 instanceof HistogramTreeNodeTable);
            assertTrue(child2 instanceof HistogramTreeNodeTable);
            HistogramTreeNodeTable cn0 = (HistogramTreeNodeTable) child0;
            assertEquals(2L, cn0.size());
            assertEquals(1, cn0.height());
            assertEquals(2, cn0.getChildren(tree).size());
            assertEquals("___", cn0.keyStart());
            assertEquals("aaa", cn0.keyEnd());

            HistogramTreeNodeTable cn1 = (HistogramTreeNodeTable) child1;
            assertEquals(3L, cn1.size());
            assertEquals(1, cn1.height());
            assertEquals(2, cn1.getChildren(tree).size());
            assertEquals("ab", cn1.keyStart());
            assertEquals("bbb", cn1.keyEnd());

            HistogramTreeNodeTable cn2 = (HistogramTreeNodeTable) child2;
            assertEquals(2L, cn2.size());
            assertEquals(1, cn2.height());
            assertEquals(2, cn2.getChildren(tree).size());
            assertEquals("ccc", cn2.keyStart());
            assertEquals("ddd", cn2.keyEnd());

            KeyHistograms.HistogramTreeNode child3 = cn0.getChildren(tree).get(0);
            assertTrue(child3 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child4 = cn0.getChildren(tree).get(1);
            assertTrue(child4 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("___", child3.keyStart());
            assertEquals(Collections.singletonList("<___>"), values(tree, child3, 0));
            assertEquals(1L, child3.size());
            assertEquals(0, child3.height());

            assertEquals("aaa", child4.keyStart());
            assertEquals(Collections.singletonList("<aaa>"), values(tree, child4, 0));
            assertEquals(1L, child4.size());
            assertEquals(0, child4.height());

            KeyHistograms.HistogramTreeNode child5 = cn1.getChildren(tree).get(0);
            assertTrue(child5 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child6 = cn1.getChildren(tree).get(1);
            assertTrue(child6 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child7 = cn2.getChildren(tree).get(0);
            assertTrue(child7 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child8 = cn2.getChildren(tree).get(1);
            assertTrue(child8 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("ab", child5.keyStart());
            assertEquals(Arrays.asList("<ab>", "<ab2>"), values(tree, child5, 0));
            assertEquals(2L, child5.size());
            assertEquals(0, child5.height());

            assertEquals("bbb", child6.keyStart());
            assertEquals(Collections.singletonList("<bbb>"), values(tree, child6, 0));
            assertEquals(1L, child6.size());
            assertEquals(0, child6.height());

            assertEquals("ccc", child7.keyStart());
            assertEquals(Collections.singletonList("<ccc>"), values(tree, child7, 0));
            assertEquals(1L, child7.size());
            assertEquals(0, child7.height());

            assertEquals("ddd", child8.keyStart());
            assertEquals(Collections.singletonList("<ddd>"), values(tree, child8, 0));
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

            assertTrue(tree.getRoot() instanceof HistogramTreeNodeTable);
            HistogramTreeNodeTable root = (HistogramTreeNodeTable) tree.getRoot();
            assertEquals(6L, root.size());
            assertEquals(2, root.height());
            assertEquals(3, root.getChildren(tree).size());
            assertEquals("___", root.keyStart());
            assertEquals("ddd", root.keyEnd());

            KeyHistograms.HistogramTreeNode child0 = root.getChildren(tree).get(0);
            KeyHistograms.HistogramTreeNode child1 = root.getChildren(tree).get(1);
            KeyHistograms.HistogramTreeNode child2 = root.getChildren(tree).get(2);
            assertTrue(child0 instanceof HistogramTreeNodeTable);
            assertTrue(child1 instanceof HistogramTreeNodeTable);
            assertTrue(child2 instanceof HistogramTreeNodeTable);
            HistogramTreeNodeTable cn0 = (HistogramTreeNodeTable) child0;
            assertEquals(2L, cn0.size());
            assertEquals(1, cn0.height());
            assertEquals(2, cn0.getChildren(tree).size());
            assertEquals("___", cn0.keyStart());
            assertEquals("aaa", cn0.keyEnd());

            HistogramTreeNodeTable cn1 = (HistogramTreeNodeTable) child1;
            assertEquals(3L, cn1.size());
            assertEquals(1, cn1.height());
            assertEquals(2, cn1.getChildren(tree).size());
            assertEquals("ab", cn1.keyStart());
            assertEquals("bbb", cn1.keyEnd());

            HistogramTreeNodeTable cn2 = (HistogramTreeNodeTable) child2;
            assertEquals(1L, cn2.size());
            assertEquals(1, cn2.height());
            assertEquals(2, cn2.getChildren(tree).size());
            assertEquals("ccc", cn2.keyStart());
            assertEquals("ddd", cn2.keyEnd());

            KeyHistograms.HistogramTreeNode child3 = cn0.getChildren(tree).get(0);
            assertTrue(child3 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child4 = cn0.getChildren(tree).get(1);
            assertTrue(child4 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("___", child3.keyStart());
            assertEquals(Collections.singletonList("<___>"), values(tree, child3, 0));
            assertEquals(1L, child3.size());
            assertEquals(0, child3.height());

            assertEquals("aaa", child4.keyStart());
            assertEquals(Collections.singletonList("<aaa>"), values(tree, child4, 0));
            assertEquals(1L, child4.size());
            assertEquals(0, child4.height());

            KeyHistograms.HistogramTreeNode child5 = cn1.getChildren(tree).get(0);
            assertTrue(child5 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child6 = cn1.getChildren(tree).get(1);
            assertTrue(child6 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child7 = cn2.getChildren(tree).get(0);
            assertTrue(child7 instanceof KeyHistograms.HistogramNodeLeafMap);
            KeyHistograms.HistogramTreeNode child8 = cn2.getChildren(tree).get(1);
            assertTrue(child8 instanceof KeyHistograms.HistogramNodeLeafMap);

            assertEquals("ab", child5.keyStart());
            assertEquals(Arrays.asList("<ab>", "<ab2>"), values(tree, child5, 0));
            assertEquals(2L, child5.size());
            assertEquals(0, child5.height());

            assertEquals("bbb", child6.keyStart());
            assertEquals(Collections.singletonList("<bbb>"), values(tree, child6, 0));
            assertEquals(1L, child6.size());
            assertEquals(0, child6.height());

            assertEquals("ccc", child7.keyStart());
            assertEquals(Collections.singletonList("<ccc>"), values(tree, child7, 0));
            assertEquals(1L, child7.size());
            assertEquals(0, child7.height());

            assertEquals("ddd", child8.keyStart());
            assertEquals(Collections.emptyList(), values(tree, child8, 0));
            assertEquals(0L, child8.size());
            assertEquals(0, child8.height());


            assertEquals(Arrays.asList("<ddd>", "<ddd2>"), Arrays.asList(vs));
        }

        save("target/msgassoc-debug/put.dot", tree);
        tree.restructure(true);
        save("target/msgassoc-debug/put-reconst.dot", tree);
    }

    public void save(String fileName, HistogramTree tree) {
        try (ActorSystemDefault sys = new ActorSystemKelp.ActorSystemDefaultForKelp()) {
            TestToolActorToGraph.save(sys, tree, new File(fileName));
        }
    }

    public void testSplit() {
        System.err.println("testSplit ----------------------");
        HistogramTree tree = new HistogramTree(this::compare, 3);

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

        try (ActorSystemDefault sys = new ActorSystemKelp.ActorSystemDefaultForKelp()) {
            TestToolActorToGraph.save(sys, tree, new File("target/msgassoc-debug/split-before.dot"));
        }


        HistogramTree tree2 = tree.split();
        {
            {
                assertEquals(3L, tree2.getLeafSize());
                assertEquals(3L, tree2.getLeafSizeNonZero());

                assertTrue(tree2.getRoot() instanceof HistogramTreeNodeTable);
                HistogramTreeNodeTable root = (HistogramTreeNodeTable) tree2.getRoot();
                assertEquals(5L, root.size());
                assertEquals(2, root.height());
                assertEquals(2, root.getChildren(tree2).size());
                assertEquals("___", root.keyStart());
                assertEquals("ab", root.keyEnd());


                KeyHistograms.HistogramTreeNode child0 = root.getChildren(tree2).get(0);
                KeyHistograms.HistogramTreeNode child1 = root.getChildren(tree2).get(1);
                assertTrue(child0 instanceof HistogramTreeNodeTable);
                assertTrue(child1 instanceof HistogramTreeNodeTable);
                HistogramTreeNodeTable cn0 = (HistogramTreeNodeTable) child0;
                assertEquals(3L, cn0.size());
                assertEquals(1, cn0.height());
                assertEquals(2, cn0.getChildren(tree2).size());
                assertEquals("___", cn0.keyStart());
                assertEquals("aaa", cn0.keyEnd());

                HistogramTreeNodeTable cn1 = (HistogramTreeNodeTable) child1;
                assertEquals(2L, cn1.size());
                assertEquals(1, cn1.height());
                assertEquals(1, cn1.getChildren(tree2).size());
                assertEquals("ab", cn1.keyStart());
                assertEquals("ab", cn1.keyEnd());

                assertEquals(Arrays.asList(cn0.getChildren(tree2).get(1)), tree2.getCompleted());


            }
            {
                assertEquals(3L, tree2.getLeafSize());
                assertEquals(3L, tree2.getLeafSizeNonZero());
                assertTrue(tree.getRoot() instanceof HistogramTreeNodeTable);
                HistogramTreeNodeTable root = (HistogramTreeNodeTable) tree.getRoot();
                assertEquals(7L, root.size());
                assertEquals(2, root.height());
                assertEquals(2, root.getChildren(tree).size());
                assertEquals("bbb", root.keyStart());
                assertEquals("ddd", root.keyEnd());


                KeyHistograms.HistogramTreeNode child0 = root.getChildren(tree).get(0);
                KeyHistograms.HistogramTreeNode child1 = root.getChildren(tree).get(1);
                assertTrue(child0 instanceof HistogramTreeNodeTable);
                assertTrue(child1 instanceof HistogramTreeNodeTable);
                HistogramTreeNodeTable cn0 = (HistogramTreeNodeTable) child0;
                assertEquals(1L, cn0.size());
                assertEquals(1, cn0.height());
                assertEquals(1, cn0.getChildren(tree).size());
                assertEquals("bbb", cn0.keyStart());
                assertEquals("bbb", cn0.keyEnd());

                HistogramTreeNodeTable cn1 = (HistogramTreeNodeTable) child1;
                assertEquals(6L, cn1.size());
                assertEquals(1, cn1.height());
                assertEquals(2, cn1.getChildren(tree).size());
                assertEquals("ccc", cn1.keyStart());
                assertEquals("ddd", cn1.keyEnd());

                assertEquals(Arrays.asList(cn1.getChildren(tree).get(1)), tree.getCompleted());
            }
        }


        save("target/msgassoc-debug/split2.dot", tree2);
        save("target/msgassoc-debug/split1.dot", tree);
    }

    public void testMerge() {
        System.err.println("testMerge -------------");
        HistogramTree tree = new HistogramTree(this::compare, 3);

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

        HistogramTree tree2 = tree.split();
        tree.merge(tree2);

        HistogramTree tree3 = new HistogramTree(this::compare, 3);
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



        HistogramTree tree4 = new HistogramTree(this::compare, 3);
        ctx.putValue = "<ooo>";
        tree4.put("ooo", ctx);
        ctx.putValue = "<ppp>";
        tree4.put("ppp", ctx);
        ctx.putValue = "<qqq>";
        tree4.put("qqq", ctx);
        ctx.putValue = "<rrr>";
        tree4.put("rrr", ctx);

        tree3.merge(tree4);

        save("target/msgassoc-debug/merge.dot", tree3);
    }

    protected List<Object> values(HistogramTree tree, KeyHistograms.HistogramTreeNode node, Object pos) {
        List<Object> vs = new ArrayList<>();
        KeyHistograms.HistogramNodeLeafMap leaf = (KeyHistograms.HistogramNodeLeafMap) node;
        KeyHistograms.HistogramLeafList l = leaf.getValues().get(pos);
        if (l != null) {
            l.iterator(tree, leaf).forEachRemaining(vs::add);
        }
        return vs;
    }

    public int compare(Object a, Object b) {
        return ((String)a).compareTo((String) b);
    }

    public void assertEquals(Object expected, Object actual) {
        TestTool.assertEquals("", expected, actual);
        if (!Objects.equals(expected, actual)) {
            new RuntimeException("not-equals: " + expected + " : " + actual).printStackTrace();
        }
    }

    public void assertTrue(boolean b) {
        TestTool.assertTrue("", b);
        if (!b) {
            new RuntimeException("not true").printStackTrace();
        }
    }
}
