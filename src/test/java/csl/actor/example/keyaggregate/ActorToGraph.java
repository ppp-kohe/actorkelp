package csl.actor.example.keyaggregate;

import csl.actor.*;
import csl.actor.cluster.PhaseShift;
import csl.actor.cluster.ResponsiveCalls;
import csl.actor.keyaggregate.*;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ActorToGraph extends ActorDefault {
    protected List<GraphNode> nodes = new ArrayList<>();
    protected Map<String,GraphNode> nodeMap = new HashMap<>();
    protected Map<String,List<GraphEdge>> edgeMap = new HashMap<>();
    protected boolean saveLeafNode;

    public ActorToGraph(ActorSystem system) {
        super(system, ActorToGraph.class.getName());
    }

    public static void save(ActorSystem system, Object a, File file) {
        ActorToGraph ag = new ActorToGraph(system);
        ag.tell(a);
        try {
            PhaseShift.start(system, ag).get(100, TimeUnit.SECONDS);
            ResponsiveCalls.sendTaskConsumer(ag, self -> self.save(file)).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected ActorBehavior initBehavior() {
        return behaviorBuilder()
                .match(GraphGen.class, this::add)
                .match(File.class, this::save)
                .match(ActorRef.class, this::receive)
                .match(KeyHistograms.HistogramTree.class, this::receive)
                .build();
    }

    public void addNode(GraphNode node) {
        int n = nodes.size();
        nodes.add(node);
        GraphNode en = nodeMap.get(node.key);
        if (en == null) {
            nodeMap.put(node.key, node);
            node.id = n;
        } else {
            if (!Objects.equals(en.label, node.label)) {
                if (en.label == null) {
                    en.label = node.label;
                } else {
                    en.label += " " + node.label;
                }
            }
            if (node.tableLabel != null) {
                if (en.tableLabel == null) {
                    en.tableLabel = node.tableLabel;
                } else {
                    en.tableLabel.addAll(node.tableLabel);
                }
            }
        }
    }

    public GraphNode getOrAdd(GraphNode node) {
        int n = nodes.size();
        GraphNode en = nodeMap.get(node.key);
        if (en == null) {
            nodeMap.put(node.key, node);
            node.id = n;
            return node;
        } else {
            return en;
        }
    }

    public void addEdge(GraphEdge e) {
        List<GraphEdge> es = edgeMap.computeIfAbsent(e.from.key + "->" + e.to.key, (k) -> new ArrayList<>());
        if (es.stream().noneMatch(ee -> Objects.equals(ee.label, e.label))) {
            es.add(new GraphEdge(getOrAdd(e.from), getOrAdd(e.to), e.label));
        }
    }

    public void add(GraphGen g) {
        g.nodes.forEach(this::addNode);
        g.edges.forEach(this::addEdge);
    }

    public void save(File file) {
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        try (PrintWriter pw = new PrintWriter(new FileOutputStream(file))) {
            write(pw);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        getSystem().getLogger().log("#saved: %s", file);
    }

    public void receive(ActorRef ref) {
        if (ref instanceof ActorRefRemote) {
            try {
                boolean sln = saveLeafNode;
                add(ResponsiveCalls.sendTask(getSystem(), ref, a ->
                    new GraphGen(a, sln).build(a)).get());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } else {
            add(new GraphGen(this, saveLeafNode).build(ref.asLocal()));
        }
    }

    public void receive(KeyHistograms.HistogramTree tree) {
        GraphGen g = new GraphGen(this, saveLeafNode);
        GraphNode n = g.createNode("?:" + Instant.now());
        n.tableLabel = new ArrayList<>();
        g.createNodeHistTree(n, tree, 0);
        add(g);
    }

    public static String idStr(Object o) {
        return Integer.toHexString(System.identityHashCode(o));
    }

    public static class GraphGen implements Serializable {
        protected ActorRef actorToGraph;
        protected boolean saveLeafNode;
        public List<GraphNode> nodes = new ArrayList<>();
        public List<GraphEdge> edges = new ArrayList<>();

        public GraphGen(ActorRef actorToGraph, boolean saveLeafNode) {
            this.actorToGraph = actorToGraph;
            this.saveLeafNode = saveLeafNode;
        }

        public GraphGen build(Actor a) {
            createNode(a);
            return this;
        }

        public String getId(ActorSystem system) {
            if (system instanceof ActorSystemRemote) {
                return Objects.toString(((ActorSystemRemote) system).getServerAddress());
            } else {
                return "@local:" + idStr(system);
            }
        }

        public String getId(ActorRef ref) {
            if (ref instanceof Actor) {
                String name = ((Actor) ref).getName();
                if (name == null) {
                    name = "#" + idStr(name);
                }
                return "actor:" + getId(((Actor) ref).getSystem()) + "/" + name;
            } else if (ref instanceof ActorRefLocalNamed) {
                return getId(ref.asLocal());
            } else if (ref instanceof ActorRefRemote) {
                return "actor:" + ((ActorRefRemote) ref).getAddress();
            } else {
                return "actor:" + ref.toString();
            }
        }

        public GraphEdge createEdge(GraphNode from, GraphNode to) {
            GraphEdge e = new GraphEdge(from, to);
            edges.add(e);
            return e;
        }

        public GraphNode createNode(String key) {
            GraphNode n = new GraphNode(key);
            nodes.add(n);
            return n;
        }

        public GraphNode createNode(Actor a) {
            GraphNode n = createNode(getId(a));
            List<List<String>> table = new ArrayList<>();
            table.add(Arrays.asList("actor", a.getClass().getSimpleName()));
            table.add(Arrays.asList("name", Objects.toString(a.getName())));
            table.add(Arrays.asList("address", getId(a)));
            table.add(Arrays.asList("idhash", idStr(a)));
            createNodeMailbox(table, a.getMailbox());
            n.tableLabel = table;

            if (a instanceof ActorKeyAggregation) {
                createNodeKeyAgg(n, (ActorKeyAggregation) a);
            }
            return n;
        }

        public void createNodeMailbox(List<List<String>> table, Mailbox mailbox) {
            if (mailbox instanceof MailboxDefault) {
                table.add(Arrays.asList("queue", String.format("%,d", ((MailboxDefault) mailbox).getQueue().size())));
            } else if (mailbox instanceof MailboxKeyAggregation) {
                createNodeMailbox(table, ((MailboxKeyAggregation) mailbox).getMailbox());
            }
        }

        public void createNodeKeyAgg(GraphNode n, ActorKeyAggregation ag) {
            List<List<String>> table = n.tableLabel;
            for (int i = 0, size = ag.getMailboxAsKeyAggregation().getEntrySize(); i < size; ++i) {
                MailboxKeyAggregation.HistogramEntry e = ag.getMailboxAsKeyAggregation().getEntries().get(i);
                table.add(Arrays.asList("t" + i + ".processor", idStr(e.getProcessor())));

                createNodeHistTree(n, e.getTree(), i);
            }

            ActorKeyAggregation.State s = ag.getState();
            n.tableLabel.add(Arrays.asList("state", s.getClass().getSimpleName()));
            n.tableLabel.add(Arrays.asList("state.procs", String.format("%,d", s.getProcessCount())));
            if (s instanceof KeyAggregationStateRouter) {
                KeyAggregationStateRouter sr = (KeyAggregationStateRouter) s;
                n.tableLabel.add(Arrays.asList("height", String.format("%,d", sr.getHeight())));
                n.tableLabel.add(Arrays.asList("maxHeight", String.format("%,d", sr.getMaxHeight())));
                createNodeSplit(n, sr.getSplit(), "");
            } else if (s instanceof ActorKeyAggregation.StateUnit) {
                n.tableLabel.add(Arrays.asList("router", actorStr(((ActorKeyAggregation.StateUnit) s).getRouter())));
            } else if (s instanceof ActorKeyAggregation.StateCanceled) {
                n.tableLabel.add(Arrays.asList("router", actorStr(((ActorKeyAggregation.StateCanceled) s).getRouter())));
            }
        }

        public String actorStr(ActorRef ref) {
            if (ref instanceof Actor) {
                return idStr(ref);
            } else if (ref instanceof ActorRefRemote) {
                return Objects.toString(((ActorRefRemote) ref).getAddress());
            } else if (ref instanceof ActorRefLocalNamed) {
                return ((ActorRefLocalNamed) ref).getName();
            } else if (ref == null) {
                return "null";
            } else {
                return ref.toString();
            }
        }

        public void createNodeHistTree(GraphNode n, KeyHistograms.HistogramTree tree, int i) {
            List<List<String>> table = n.tableLabel;
            table.add(Arrays.asList("t" + i + ".leafSize", String.format("%,d", tree.getLeafSize())));
            table.add(Arrays.asList("t" + i + ".leafSizeNZ", String.format("%,d", tree.getLeafSizeNonZero())));
            table.add(Arrays.asList("t" + i + ".leafSizeNZR", String.format("%1.2f", tree.getLeafSizeNonZeroRate())));
            table.add(Arrays.asList("t" + i + ".completed", idStr(tree.getCompleted()) + " (" + tree.getCompleted().size() + ")"));

            GraphEdge e = createNodeHistNode(n, tree.getRoot());
            if (e != null) {
                e.label = "root";
            }
        }


        public GraphEdge createNodeHistNode(GraphNode from, KeyHistograms.HistogramNode node) {
            if (node == null) {
                GraphNode n = createNode("null:" + Instant.now());
                n.label = "tree null";
                return createEdge(from, n);
            } else if (node instanceof KeyHistograms.HistogramNodeTree) {
                return createNodeHistTree(from, (KeyHistograms.HistogramNodeTree) node);
            } else if (node instanceof KeyHistograms.HistogramNodeLeaf) {
                if (node.height() > 0 || (saveLeafNode)) {
                    return createNodeHistLeaf(from, (KeyHistograms.HistogramNodeLeaf) node);
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }

        public GraphEdge createNodeHistTree(GraphNode from, KeyHistograms.HistogramNodeTree t) {
            List<List<String>> table = new ArrayList<>();
            table.add(Arrays.asList("height=" + t.height(), "start", "end", "size", "sizet"));

            String cIdx;
            if (t.getParent() != null) {
                cIdx = String.format("%,d", t.getParent().getChildren().indexOf(t));
            } else {
                cIdx = "null";
            }
            table.add(Arrays.asList("cIdx=" + cIdx, Objects.toString(t.keyStart()), Objects.toString(t.keyEnd()), String.format("%,d", t.size()), ""));
            int i = 0;
            long subTotal = 0;
            for (KeyHistograms.HistogramNode n : t.getChildren()) {
                subTotal += n.size();
                table.add(Arrays.asList("child" + i, Objects.toString(n.keyStart()), Objects.toString(n.keyEnd()),
                        String.format("%,d", n.size()),
                        String.format("%,d", subTotal)));
                ++i;
            }

            GraphNode n = createNode("histTree:" + Instant.now());
            n.tableLabel = table;
            GraphEdge e = createEdge(from, n);
            if (t.getChildren() != null) {
                t.getChildren().forEach(c -> createNodeHistNode(n, c));
            }
            return e;
        }

        public GraphEdge createNodeHistLeaf(GraphNode from, KeyHistograms.HistogramNodeLeaf l) {
            List<List<String>> table = new ArrayList<>();
            int vi = 0;
            table.add(Arrays.asList("height", String.format("%,d", l.height())));
            if (l.getParent() != null) {
                table.add(Arrays.asList("childIdx", String.format("%,d", l.getParent().getChildren().indexOf(l))));
            } else {
                table.add(Arrays.asList("childIdx", "null"));
            }
            table.add(Arrays.asList("key", Objects.toString(l.getKey())));
            table.add(Arrays.asList("size", String.format("%,d", l.size())));
            if (l instanceof ActorBehaviorKeyAggregation.HistogramNodeLeafN) {
                for (KeyHistograms.HistogramLeafList list : ((ActorBehaviorKeyAggregation.HistogramNodeLeafN) l).getStructList()) {
                    long n = list.count();
                    table.add(Arrays.asList("v" + vi + ".count", String.format("%,d", n)));
                    ++vi;
                }
            }

            GraphNode n = new GraphNode("histLeaf:" + Instant.now());
            n.tableLabel = table;
            return createEdge(from, n);
        }

        public void createNodeSplit(GraphNode from, KeyAggregationRoutingSplit s, String edgeLabel) {
            if (s == null) {
                GraphNode n = createNode("null:" + Instant.now());
                n.label = "null";
                createEdge(from, n);
            } else if (s instanceof KeyAggregationRoutingSplit.RoutingSplitNode) {
                KeyAggregationRoutingSplit.RoutingSplitNode st = (KeyAggregationRoutingSplit.RoutingSplitNode) s;
                GraphNode n = createNode("splitNode:" + Instant.now());
                n.tableLabel = new ArrayList<>();
                n.tableLabel.add(Arrays.asList("split.procs", String.format("%,d", st.getProcessCount())));
                n.tableLabel.add(Arrays.asList("path", st.getPath().toString()));
                n.tableLabel.add(Arrays.asList("split", limitString(Objects.toString(st.getSplitPoints()))));
                n.tableLabel.add(Arrays.asList("depth", Integer.toString(s.getDepth())));
                n.tableLabel.add(Arrays.asList("ratioAll", String.format("%4.2f", st.getHistory().ratioAll())));

                int hi = 0;
                for (KeyAggregationRoutingSplit.RoutingHistory h : st.getHistory().toList()) {
                    n.tableLabel.add(Arrays.asList("hist" + hi, String.format("%4.2f", h.ratio()), String.format("%,d", h.left.get()), String.format("%,d", h.right.get())));
                    ++hi;
                }

                createNodeSplit(n, st.getLeft(), "left");
                createNodeSplit(n, st.getRight(), "right");

                createEdge(from, n).label = edgeLabel;
            } else if (s instanceof KeyAggregationRoutingSplit.RoutingSplitLeaf) {
                ActorRef ref = ((KeyAggregationRoutingSplit.RoutingSplitLeaf) s).getActor();
                GraphNode n = createNode(getId(ref));
                createEdge(from, n).label = edgeLabel + " dep:" + s.getDepth();
                actorToGraph.tell(ref);
            }
        }

        protected String limitString(String s) {
            if (s != null && s.length() > 20) {
                return s.substring(0, 20) + "...";
            } else {
                return s;
            }
        }

    }


    public static class GraphNode implements Serializable {
        public int id;
        public String key;
        public String label;
        public List<List<String>> tableLabel;

        public GraphNode(String key) {
            this.key = key;
        }
    }

    public static class GraphEdge implements Serializable {
        public GraphNode from;
        public GraphNode to;
        public String label = "";

        public GraphEdge(GraphNode from, GraphNode to) {
            this.from = from;
            this.to = to;
        }

        public GraphEdge(GraphNode from, GraphNode to, String label) {
            this.from = from;
            this.to = to;
            this.label = label;
        }
    }

    public void write(PrintWriter out) {
        out.println("digraph {");

        for (GraphNode n : nodes) {
            out.print("n" + n.id);
            if (n.label != null) {
                out.print("[label=\"" + escape(n.label) + "\" shape=box]");
            } else if (n.tableLabel != null) {
                out.print("[label=<" + tableHtml(n.tableLabel) + "> shape=none]");
            }
            out.println(";");
        }

        for (GraphEdge e : edgeMap.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList())) {
            out.print("n" + e.from.id + " -> n" + e.to.id);
            if (e.label != null) {
                out.print("[label=\"" + escape(e.label) + "\"]");
            }
            out.println(";");
        }

        out.println("}");
    }

    public String escape(String s) {
        return s; //TODO
    }

    public String tableHtml(List<List<String>> table) {
        StringBuilder buf = new StringBuilder();
        buf.append("<table border=\"0\" cellborder=\"1\" cellspacing=\"0\">");
        buf.append(table.stream().map(row ->
            row.stream()
                .map(this::escapeHtml)
                .collect(Collectors.joining("</td><td>", "<td>", "</td>")))
            .collect(Collectors.joining("</tr><tr>", "<tr>", "</tr>")));
        buf.append("</table>");
        return buf.toString();
    }

    public String escapeHtml(String s) {
        return s;
    }
}
