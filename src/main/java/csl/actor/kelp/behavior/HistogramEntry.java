package csl.actor.kelp.behavior;

import csl.actor.Actor;
import csl.actor.CallableMessage;
import csl.actor.MessageBundle;
import csl.actor.kelp.ActorKelp;

import java.util.concurrent.atomic.AtomicBoolean;

public class HistogramEntry {
    protected int entryId;
    protected HistogramProcessor processor;
    protected HistogramTree tree;
    protected AtomicBoolean traversalReserved = new AtomicBoolean();

    public HistogramEntry(int entryId, HistogramProcessor p, HistogramTree tree) {
        this.entryId = entryId;
        this.processor = p;
        this.tree = tree;
    }

    public HistogramEntry create() {
        return new HistogramEntry(entryId, processor, tree.createTree(null));
    }

    public int getEntryId() {
        return entryId;
    }

    public HistogramTree getTree() {
        return tree;
    }

    public void setTree(HistogramTree tree) {
        this.tree = tree;
    }

    public void reserveTraversal(Actor self) {
        if (self != null && traversalReserved.compareAndSet(false, true)) {
            self.tellMessage(new MessageBundle.MessageAccepted<>(self, new TraversalProcess(entryId).withSender(null)));
        }
    }

    public void processTraversalReserved(Actor self, MailboxKelp.ReducedSize reducedSize) {
        if (traversalReserved.compareAndSet(true, false)) {
            processTraversal(self, reducedSize, true);
        }
    }

    public void processTraversal(Actor self, MailboxKelp.ReducedSize reducedSize, boolean reserved) {
        if (processor.needToProcessTraversal(self, tree, reducedSize, reserved)) {
            processTraversal(self, reducedSize, tree, tree.getRoot());
        }
    }

    public void processTraversal(Actor self, MailboxKelp.ReducedSize reducedSize, HistogramTree tree, KeyHistograms.HistogramTreeNode node) {
        if (node != null && node.size() > 0) {
            if (node instanceof HistogramTreeNodeTable) {
                for (KeyHistograms.HistogramTreeNode ch : ((HistogramTreeNodeTable) node).getChildren(tree)) { //TODO loading all persisted sub-nodes
                    processTraversal(self, reducedSize, tree, ch);
                }
            } else if (node instanceof HistogramTreeNodeLeaf) {
                processor.processTraversal(self, reducedSize, (HistogramTreeNodeLeaf) node);
            }
        }
    }

    public HistogramProcessor getProcessor() {
        return processor;
    }

    public static class TraversalProcess implements CallableMessage.CallableMessageConsumer<ActorKelp<?>> {
        public static final long serialVersionUID = 1L;
        public int entryId;

        public TraversalProcess() {}

        public TraversalProcess(int entryId) {
            this.entryId = entryId;
        }

        @Override
        public void accept(ActorKelp<?> self) {
            self.getMailboxAsKelp().processTraversalReserved(self, self.getReducedSize(), entryId);
        }
    }

    public synchronized boolean hasRemainingProcesses() {
        HistogramTree tree = this.tree;
        return (tree != null && tree.hasCompleted());
    }

    public synchronized void terminateRemainingProcess() {
        HistogramTree tree = this.tree;
        if (tree != null) {
            tree.close();
        }
    }

    public void processStageEnd(Actor self, Object stageKey, MailboxKelp.ReducedSize reducedSize) {
        if (processor.needToProcessStageEnd(self, reducedSize, stageKey, tree)) {
            synchronized (this) {
                processStageEnd(self, stageKey, reducedSize, tree, tree.getRoot());
            }
        }
    }

    public void processStageEnd(Actor self, Object stageKey, MailboxKelp.ReducedSize reducedSize, HistogramTree tree, KeyHistograms.HistogramTreeNode node) {
        if (node != null && node.size() > 0) {
            if (node instanceof HistogramTreeNodeTable) {
                for (KeyHistograms.HistogramTreeNode ch : ((HistogramTreeNodeTable) node).getChildren(tree)) {
                    processStageEnd(self, stageKey, reducedSize, tree, ch);
                }
            } else if (node instanceof HistogramTreeNodeLeaf) {
                processor.processStageEnd(self, stageKey, reducedSize, (HistogramTreeNodeLeaf) node);
            }
        }
    }

    public void processPersistableTraversalBeforePut(Actor self, MailboxKelp.ReducedSize reducedSize) {
        processTraversal(self, reducedSize, false);
    }
}
