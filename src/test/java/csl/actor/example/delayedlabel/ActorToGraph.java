package csl.actor.example.delayedlabel;

import csl.actor.*;
import csl.actor.msgassoc.*;
import csl.actor.remote.ActorRefRemote;
import csl.actor.remote.ActorSystemRemote;

import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

public class ActorToGraph {
    protected List<GraphNode> nodes = new ArrayList<>();
    protected Map<Object,GraphNode> nodeMap = new WeakHashMap<>();

    public void save(ActorSystem sys) {
        if (sys instanceof ActorSystemDefault) {

            ActorSystemDefault sd = (ActorSystemDefault) sys;
            sd.getNamedActorMap().values()
                    .forEach(this::save);

        } else if (sys instanceof ActorSystemRemote) {
            save(((ActorSystemRemote) sys).getLocalSystem());
        }
    }

    public GraphNode save(Actor a) {
        GraphNode n = createNode();
        nodeMap.put(a, n);

        List<List<String>> table = new ArrayList<>();
        table.add(Arrays.asList("actor", a.getClass().getSimpleName()));
        table.add(Arrays.asList("name", Objects.toString(a.getName())));
        if (a.getMailbox() instanceof MailboxDefault) {
            table.add(Arrays.asList("queue", String.format("%,d", ((MailboxDefault) a.getMailbox()).getQueue().size())));
        }
        n.tableLabel = table;
        if (a instanceof ActorAggregation) {
            ActorAggregation ag = (ActorAggregation) a;
            for (int i = 0, size = ag.getMailboxAsAggregation().getTableSize(); i < size; ++i) {
                KeyHistograms.HistogramTree tree = ag.getMailboxAsAggregation().getTable(i);
                GraphEdge e = save(n, tree.getRoot());
                if (e != null) {
                    e.label = "table" + i;
                }
            }
            if (a instanceof ActorAggregationReplicable) {
                save(n, (ActorAggregationReplicable) a);
            }
        }
        return n;
    }

    protected GraphEdge link(GraphNode from, ActorRef ref) {
        GraphNode ex = nodeMap.get(from);
        if (ex != null) {
            return ex.fromEdge(from);
        } else {
            if (ref instanceof ActorRefLocalNamed) {
                String name = ((ActorRefLocalNamed) ref).getName();
                GraphNode n = createNode();
                n.label = "refLocal:" + name;
                nodeMap.put(ref, n);
                return n.fromEdge(from);
            } else if (ref instanceof ActorRefRemote) {
                GraphNode n = createNode();
                n.label = "refRemote:" + ((ActorRefRemote) ref).getAddress();
                nodeMap.put(ref, n);
                return n.fromEdge(from);
            } else if (ref instanceof ActorAggregation) {
                return save((ActorAggregation) ref).fromEdge(from);
            } else if (ref == null) {
                GraphNode n = createNode();
                n.label = "ref null";
                return n.fromEdge(from);
            } else if (ref instanceof Actor) {
                return save((Actor) ref).fromEdge(from);
            } else {
                GraphNode n = createNode();
                n.label = limitString("" + ref);
                nodeMap.put(ref, n);
                return n.fromEdge(from);
            }
        }
    }

    protected String limitString(String s) {
        if (s != null && s.length() > 20) {
            return s.substring(0, 20) + "...";
        } else {
            return s;
        }
    }

    protected void save(GraphNode n, ActorAggregationReplicable a) {
        ActorAggregationReplicable.State s = a.getState();
        n.tableLabel.add(Arrays.asList("state", s.getClass().getSimpleName()));
        if (s instanceof ActorAggregationReplicable.StateDefault) {
            //
        } else if (s instanceof ActorAggregationReplicable.StateReplica) {
            link(n, ((ActorAggregationReplicable.StateReplica) s).getRouter()).label = "router";
        } else if (s instanceof ActorAggregationReplicable.StateRouterTemporary) {
            ActorAggregationReplicable.StateRouterTemporary tmp = (ActorAggregationReplicable.StateRouterTemporary) s;
            link(n, tmp.getRouter()).label = "router";
            link(n, tmp.getLeft()).label = "newLeft";
            link(n, tmp.getRight()).label = "newRight";
            tmp.getSplits().forEach(c -> saveSplitTree(n, c).label = "split");
        } else if (s instanceof ActorAggregationReplicable.StateRouter) {
            ((ActorAggregationReplicable.StateRouter) s).getSplits().forEach(c -> saveSplitTree(n, c).label = "split");
        }
    }

    protected GraphEdge save(GraphNode from, KeyHistograms.HistogramNode node) {
        if (node == null) {
            GraphNode n = createNode();
            n.label = "tree null";
            return n.fromEdge(from);
        } else if (node instanceof KeyHistograms.HistogramNodeTree) {
            return saveTree(from, (KeyHistograms.HistogramNodeTree) node);
        } else if (node instanceof KeyHistograms.HistogramNodeLeaf) {
            return saveLeafN(from, (KeyHistograms.HistogramNodeLeaf) node);
        } else {
            return null;
        }
    }

    protected GraphEdge saveTree(GraphNode from, KeyHistograms.HistogramNodeTree t) {
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

        GraphNode n = createNode();
        n.tableLabel = table;
        GraphEdge e = n.fromEdge(from);
        if (t.getChildren() != null) {
            t.getChildren().forEach(c -> save(n, c));
        }
        return e;
    }

    protected GraphEdge saveLeafN(GraphNode from, KeyHistograms.HistogramNodeLeaf l) {
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
        if (l instanceof ActorBehaviorBuilderKeyValue.HistogramNodeLeafN) {
            for (KeyHistograms.HistogramLeafList list : ((ActorBehaviorBuilderKeyValue.HistogramNodeLeafN) l).getValueList()) {
                long n = list.count();
                table.add(Arrays.asList("v" + vi + ".count", String.format("%,d", n)));
                ++vi;
            }
        }

        GraphNode n = createNode();
        n.tableLabel = table;
        return n.fromEdge(from);
    }

    protected GraphEdge saveSplitTree(GraphNode n, MailboxAggregationReplicable.SplitTreeRoot root) {
        MailboxAggregationReplicable.Split s = root.getSplit();
        return saveSplit(n, s);
    }

    protected GraphEdge saveSplit(GraphNode from, MailboxAggregationReplicable.Split s) {
        if (s instanceof MailboxAggregationReplicable.SplitTree) {
            MailboxAggregationReplicable.SplitTree st = (MailboxAggregationReplicable.SplitTree) s;
            GraphNode n = createNode();
            n.label = "split:" + limitString(Objects.toString(st.getPoint()));

            saveSplit(n, st.getLeft()).label = "left";
            saveSplit(n, st.getRight()).label = "right";

            return n.fromEdge(from);
        } else if (s instanceof MailboxAggregationReplicable.SplitActor) {
            return link(from, ((MailboxAggregationReplicable.SplitActor) s).getActorRef());
        } else {
            return null;
        }
    }

    protected GraphNode createNode() {
        GraphNode n = new GraphNode();
        n.id = nodes.size();
        nodes.add(n);
        return n;
    }

    static class GraphNode {
        public int id;
        public String label;
        public List<List<String>> tableLabel;
        public List<GraphEdge> outgoings = new ArrayList<>();

        public GraphEdge fromEdge(GraphNode n) {
            if (n != null) {
                GraphEdge e = new GraphEdge(n, this);
                n.outgoings.add(e);
                return e;
            } else {
                return null;
            }
        }
    }

    static class GraphEdge {
        public GraphNode from;
        public GraphNode to;
        public String label = "";

        public GraphEdge(GraphNode from, GraphNode to) {
            this.from = from;
            this.to = to;
        }
    }

    public void write(PrintWriter out) {
        Set<GraphEdge> es = new HashSet<>();
        nodes.forEach(n -> es.addAll(n.outgoings));

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

        for (GraphEdge e : es) {
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
