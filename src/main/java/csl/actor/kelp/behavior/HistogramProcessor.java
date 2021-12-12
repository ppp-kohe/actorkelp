package csl.actor.kelp.behavior;

import csl.actor.Actor;
import csl.actor.ActorBehavior;
import csl.actor.kelp.ActorKelpFunctions;

public interface HistogramProcessor extends ActorBehavior {
    ActorKelpFunctions.KeyComparator<?> getKeyComparator();

    Object selectFromValue(Object value);
    Object extractKeyFromValue(Object value, Object position);

    default boolean needToProcessTraversal(Actor self, HistogramTree tree, MailboxKelp.ReducedSize reducedSize, boolean reserved) {
        return false;
    }

    default boolean needToProcessStageEnd(Actor self, MailboxKelp.ReducedSize reducedSize, Object stageKey, HistogramTree tree) {
        return false;
    }

    default void processTraversal(Actor self, MailboxKelp.ReducedSize reducedSize, HistogramTreeNodeLeaf leaf) {}

    default void processStageEnd(Actor self, Object stageKey, MailboxKelp.ReducedSize reducedSize, HistogramTreeNodeLeaf leaf) {}
}
