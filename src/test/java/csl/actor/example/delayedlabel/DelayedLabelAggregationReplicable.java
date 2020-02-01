package csl.actor.example.delayedlabel;

import csl.actor.ActorBehavior;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.msgassoc.ActorAggregationReplicable;
import csl.actor.msgassoc.ConfigAggregationReplicable;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DelayedLabelAggregationReplicable extends DelayedLabelManual {
    public static void main(String[] args) {
        new DelayedLabelAggregationReplicable().run(args);
    }

    @Override
    public ActorRef learnerActor(ActorSystem system, PrintWriter out, ActorRef resultActor, int numInstances) {
        root = new LernerActorAggregationReplicable(system, out, resultActor, numInstances);
        root.log(root.getConfig().toString());
        return root;
    }

    @Override
    public ResultActor resultActor(ActorSystem system, PrintWriter out, Instant startTime, int numInstances) {
        return new ResultActorAggregationReplicable(system, out, startTime, numInstances);
    }

    static LernerActorAggregationReplicable root;

    static List<LernerActorAggregationReplicable> processing = new ArrayList<>();

    static class ResultActorAggregationReplicable extends ResultActor {
        public ResultActorAggregationReplicable(ActorSystem system, PrintWriter out, Instant startTime, int numInstances) {
            super(system, out, startTime, numInstances);
        }

        @Override
        public void receive(int next, ActorRef sender) {
            super.receive(next, sender);
            if (learner != null) {
                root.support.process();
            }
        }
    }

    static class LernerActorAggregationReplicable extends ActorAggregationReplicable {
        DelayedLabelAggregation.LearnerAggregationSupport support;

        public LernerActorAggregationReplicable(ActorSystem system, String name, PrintWriter out, ActorRef resultActor, int numInstances) {
            super(system, name, ConfigAggregationReplicable.readConfig("", System.getProperties()));
            support = new DelayedLabelAggregation.LearnerAggregationSupport(this, out, resultActor, numInstances);
        }

        public LernerActorAggregationReplicable(ActorSystem system, PrintWriter out, ActorRef resultActor, int numInstances) {
            this(system, null, out, resultActor, numInstances);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .matchKey(FeatureInstance.class, FeatureInstance::getId)
                    .or(LabelInstance.class, LabelInstance::getId)
                    .collect()
                    .forEachPair(this::train)
                    .match(Finish.class, this::finish)
                    .build();
        }

        public void train(FeatureInstance f, LabelInstance l) {
            support.train(f, l);
        }

        public void finish(Finish f) {
            try {
                Thread.sleep(1000);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            support.finish(f);
        }

        AtomicInteger rec = new AtomicInteger();

        @Override
        protected void initClone(ActorAggregationReplicable original) {
            log("clone");
            rec = new AtomicInteger();
            support = support.createClone(this);
        }

        @Override
        protected void initMerged(ActorAggregationReplicable m) {
            log("merge");
        }

        /*        @Override
                public boolean processMessageNext() {
                    return super.processMessageNext();
                }

                private void logActor(Throwable ex) {
                    synchronized (DelayedLabelAggregationReplicable.class) {
                        log("#error: actor: " + toStr(this));
                        log("                        actor: " + toStrProc(self -> toStr(self)));
                        log("                      mailbox: " + toStrProc(self -> toStr(self.getMailboxAsReplicable())));
                        log("                mailbox.entry: " + toStrProc(self -> toStr(self.getMailboxAsReplicable().getTableEntries().get(0))));
                        log("           mailbox.entry.proc: " + toStrProc(self -> toStr(self.getMailboxAsReplicable().getTableEntries().get(0).getProcessor())));
                        log("           mailbox.entry.tree: " + toStrProc(self -> toStr(self.getMailboxAsReplicable().getTableEntries().get(0).getTree())));
                        log("      mailbox.entry.tree.root: " + toStrProc(self -> toStr(self.getMailboxAsReplicable().getTable(0).getRoot())));
                        log(" mailbox.entry.tree.completed: " + toStrProc(self -> toStr(self.getMailboxAsReplicable().getTable(0).getCompleted())));
                        log("                        state: " + toStrProc(self -> toStr(self.getState())));
                        log("                     behavior: " + toStrProc(self -> behaviorStr(self.getBehavior())));
                        processing.forEach(self ->
                                log(" tree:\n" + String.join("\n", listObjectsToStr(0, self.getMailboxAsAggregation().getTable(0).getRoot()))));

                        List<List<Object>> oss = new ArrayList<>();
                        for (ActorAggregationReplicable self : processing) {
                            List<Object> os = new ArrayList<>();
                            os.addAll(Arrays.asList(self,
                                    self.getMailboxAsReplicable(),
                                    self.getMailboxAsReplicable().getTableEntries().get(0),
                                    self.getMailboxAsReplicable().getTableEntries().get(0).getProcessor(),
                                    self.getMailboxAsReplicable().getTable(0),
                                    self.getMailboxAsReplicable().getTable(0).getRoot(),
                                    self.getMailboxAsReplicable().getTable(0).getCompleted(),
                                    self.getState()));
                            os.addAll(listObjects(self.getMailboxAsReplicable().getTableEntries().get(0).getTree().getRoot()));
                            os.addAll(listBehavior(self.getBehavior()));
                            os.removeIf(Objects::isNull);
                            oss.add(os);
                        }
                        int i = 0;
                        for (List<Object> os : oss) {
                            int j = 0;
                            for (List<Object> os2: oss) {
                                if (os != os2) {
                                    Set<Object> sh = new HashSet<>(os);
                                    sh.removeIf(s -> os2.stream().noneMatch(o -> s == o));
                                    if (!sh.isEmpty()) {
                                        log("  !!!! shared instances: " + toStr(processing.get(i)) + " vs "  + toStr(processing.get(j)) +  " : " +
                                                sh.stream().map(this::toStr).collect(Collectors.joining(", ", "[", "]")));
                                    }
                                }
                                ++j;
                            }
                            ++i;
                        }

                        ex.printStackTrace();
                    }
                }

                private List<Object> listObjects(KeyHistograms.HistogramNode n) {
                    List<Object> ls = new ArrayList<>();
                    if (n instanceof KeyHistograms.HistogramNodeTree) {
                        ls.add(n);
                        for (KeyHistograms.HistogramNode c : ((KeyHistograms.HistogramNodeTree) n).getChildren()) {
                            listObjects(c);
                        }
                    } else if (n instanceof ActorBehaviorBuilderKeyValue.HistogramNodeLeafN) {
                        ls.add(n);
                        for (KeyHistograms.HistogramLeafList v : ((ActorBehaviorBuilderKeyValue.HistogramNodeLeafN) n).getValueList()) {
                            ls.add(v);
                        }
                    }
                    return ls;
                }

                private List<Object> listBehavior(ActorBehavior b) {
                    List<Object> ls = new ArrayList<>();
                    if (b instanceof ActorBehaviorBuilder.ActorBehaviorOr) {
                        ActorBehaviorBuilder.ActorBehaviorOr or = (ActorBehaviorBuilder.ActorBehaviorOr) b;
                        ls.addAll(listBehavior(or.getLeft()));
                        ls.addAll(listBehavior(or.getRight()));
                    } else if (b instanceof ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey) {
                        ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey<?> mk = (ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey<?>) b;
                        ls.add(mk);
                        ls.add(mk.putTree);
                        ls.add(mk.getKeyComparator());
                        ls.add(mk.getHandler());
                    }
                    return ls;
                }

                private List<String> listObjectsToStr(int dep, KeyHistograms.HistogramNode n) {
                    List<String> ls = new ArrayList<>();
                    String indent = IntStream.range(0, dep).mapToObj(i -> "  ").collect(Collectors.joining());
                    if (n instanceof KeyHistograms.HistogramNodeTree) {
                        ls.add(indent + toStr(n) + " parent: " + toStr(((KeyHistograms.HistogramNodeTree) n).getParent()));
                        for (KeyHistograms.HistogramNode c : ((KeyHistograms.HistogramNodeTree) n).getChildren()) {
                            ls.addAll(listObjectsToStr(dep + 1, c));
                        }
                    } else if (n instanceof ActorBehaviorBuilderKeyValue.HistogramNodeLeafN) {
                        ls.add(indent + toStr(n) + " parent: " + toStr(((KeyHistograms.HistogramNodeLeaf) n).getParent()));
                        for (KeyHistograms.HistogramLeafList v : ((ActorBehaviorBuilderKeyValue.HistogramNodeLeafN) n).getValueList()) {
                            ls.add(indent + "  " + toStr(v));
                        }
                    }
                    return ls;
                }

                private String toStrProc(Function<ActorAggregationReplicable,String> f) {
                    return processing.stream().map(f).collect(Collectors.joining(", ", "[", "]"));
                }

                private String behaviorStr(ActorBehavior b) {
                    if (b instanceof ActorBehaviorBuilder.ActorBehaviorOr) {
                        ActorBehaviorBuilder.ActorBehaviorOr or = (ActorBehaviorBuilder.ActorBehaviorOr) b;
                        return behaviorStr(or.getLeft()) + " " + behaviorStr(or.getRight());
                    } else if (b instanceof ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey) {
                        ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey<?> mk = (ActorBehaviorBuilderKeyValue.ActorBehaviorMatchKey<?>) b;
                        return toStr(b) + " (" + toStr(mk.putTree) + ", " + toStr(mk.getKeyComparator()) + ", " + toStr(mk.getHandler()) + ")";
                    } else {
                        return "";
                    }
                }

                private String toStr(Object o) {
                    return o == null ? "null": (o.getClass().getSimpleName() + "@" + ActorToGraph.idStr(o));
                }

                @Override
                public ActorAggregationReplicable createClone() {
                    ActorAggregationReplicable r = super.createClone();
                    System.err.println("#clone: " + r + " <- " + this);
                    System.err.println("   completed  " +
                            Integer.toHexString(System.identityHashCode(r.getMailboxAsReplicable().getTable(0).getCompleted())) + " <- " +
                            Integer.toHexString(System.identityHashCode(this.getMailboxAsReplicable().getTable(0).getCompleted())));

                    return r;
                }
        */
        @Override
        protected void processMessage(Message<?> message) {
            if (root == this) {
                support.processMessageBefore(message);
            } else {
                DelayedLabelAggregation.LearnerAggregationSupport.pruneCount.addAndGet(getMailboxAsReplicable().prune(32, 0.5));
            }
            super.processMessage(message);
        }
    }


}
