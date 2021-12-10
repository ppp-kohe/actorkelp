package csl.actor.kelp.behavior;

import csl.actor.*;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.persist.PersistentConditionActor;
import csl.actor.kelp.persist.TreeMerger;
import csl.actor.kelp.shuffle.ActorRefShuffle;
import csl.actor.persist.MailboxManageable;
import csl.actor.util.FileSplitter;
import csl.actor.util.Staging;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class ActorKelpStats implements Cloneable, Serializable {
    public static abstract class ActorKelpProcessingStats implements Cloneable, Serializable {

        public ActorKelpProcessingStats prev;

        public String toStringContents() {
            return toStringContents(null);
        }

        public String toStringContents(ActorKelpProcessingStats past) {
            if (past == null) {
                return toStringContentsPrev(null) + toStringContentsInChain(null);
            } else if (past.getClass().equals(getClass())) {
                return toStringContentsPrev(past) + toStringContentsInChain(past);
            } else {
                return toStringContentsPrev(past) + toStringContentsInChain(null) + "*";
            }
        }

        public abstract String toStringContentsInChain(ActorKelpProcessingStats past); //past is always same class

        protected String toStringContentsPrev(ActorKelpProcessingStats past) {
            if (past == null) {
                if (prev == null) {
                    return "";
                } else {
                    return prev.toStringContents(null) + "*, ";
                }
            } else {
                if (prev == null) {
                    if (past.prev == null) {
                        return "";
                    } else {
                        return "*, ";
                    } 
                } else {
                    return prev.toStringContentsPrev(past.prev);
                }
            }
        }

        public ActorKelpProcessingStats copy() {
            try {
                ActorKelpProcessingStats s = (ActorKelpProcessingStats) super.clone();
                s.prev = (prev == null ? null : prev.copy());
                return s;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class ActorKelpMessageHandledStats implements Cloneable, Serializable {
        public static final long serialVersionUID = 1;
        public volatile long count;
        public void increment() {
            count++;
        }
        public String toStringContents() {
            return String.format("msgHandled=%,d", count);
        }

        public String toStringContents(ActorKelpMessageHandledStats prev) {
            if (prev == null) {
                return toStringContents();
            } else {
                return String.format("msgHandled=%,d [%+,d]", count, count - prev.count);
            }
        }

        public ActorKelpMessageHandledStats copy() {
            try {
                return (ActorKelpMessageHandledStats) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class ActorKelpMailboxTreeStats implements Cloneable, Serializable {
        public static final long serialVersionUID = 1;

        public long onMemorySize;
        public long totalSize;
        public long leafSize;
        public long leafSizeNonZero;

        public ActorKelpMailboxTreeStats() {}

        public ActorKelpMailboxTreeStats(ActorKelp<?> k) {
            MailboxKelp mb = k.getMailboxAsKelp();
            for (int i = 0; i < mb.getEntrySize(); ++i) {
                HistogramTree tree = mb.getHistogram(i);
                this.totalSize += tree.getTreeSize();
                this.onMemorySize += tree.getTreeSizeOnMemory();
                this.leafSize += tree.getLeafSize();
                this.leafSizeNonZero += tree.getLeafSizeNonZero();
            }
        }

        public ActorKelpMailboxTreeStats copy() {
            try {
                return (ActorKelpMailboxTreeStats) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

        public String toStringContents() {
            return String.format("tree=(onMem:%,d /total:%,d, nonZeroLf:%,d /leaf:%,d)",
                    onMemorySize, totalSize, leafSizeNonZero, leafSize);
        }

        public String toStringContents(ActorKelpMailboxTreeStats prev) {
            if (prev == null) {
                return toStringContents();
            } else {
                return String.format("tree=(onMem:%,d [%+,d] /total:%,d [%+,d], nonZeroLf:%,d [%+,d] /leaf:%,d [%+,d])",
                        onMemorySize, onMemorySize - prev.onMemorySize,
                        totalSize, totalSize - prev.totalSize,
                        leafSizeNonZero, leafSizeNonZero - prev.leafSizeNonZero,
                        leafSize, leafSize - prev.leafSize);
            }
        }
    }

    public static class ActorKelpProcessingStatsFileSplit extends ActorKelpProcessingStats {
        public static final long serialVersionUID = 1;
        public FileSplitter.FileSplit split;

        public volatile double rate;
        public volatile Duration totalWait;
        public volatile Duration totalTime;

        public ActorKelpProcessingStatsFileSplit() {}

        public ActorKelpProcessingStatsFileSplit(ActorKelpProcessingStats prev, FileSplitter.FileSplit split) {
            this.prev = prev;
            this.split = split;
            totalWait = Duration.ZERO;
        }

        public void updateRate(Duration totalTime, Duration totalWait, Iterator<String> iterator) {
            double d = (iterator instanceof FileSplitter.FileSplitLineIterator ? ((FileSplitter.FileSplitLineIterator) iterator).getPositionRate() : 1.0);
            updateRate(totalTime, totalWait, d);
        }

        public void updateRate(Duration totalTime, Duration totalWait, double d) {
            rate = d;
            this.totalTime = totalTime;
            this.totalWait = totalWait;
        }

        @Override
        public String toStringContentsInChain(ActorKelpProcessingStats past) {
            if (past instanceof ActorKelpProcessingStatsFileSplit) {
                ActorKelpProcessingStatsFileSplit pastFile = (ActorKelpProcessingStatsFileSplit) past;
                String str = "";
                if (pastFile.split.equals(split)) {
                    double diff = rate - pastFile.rate;
                    if (diff == 0) {
                        str = " [+No%]";
                    } else {
                        str = String.format(" [%+1.6f%%]", (diff * 100.0));
                    }
                } else {
                    str = "*";
                }
                return String.format("split(%s, %2.2f%%%s, totalWait=%s, totalTime=%s) ", split.toStringShort(),
                        rate * 100.0, str, totalWait, totalTime);
            } else {
                return toStringContentsInChain();
            }
        }

        protected String toStringContentsInChain() {
            return String.format("split(%s, %2.2f%%, totalWait=%s, totalTime=%s) ", split.toStringShort(),
                    rate * 100.0, totalWait, totalTime);
        }
    }

    public static class ActorKelpProcessingStatsMessageBundle extends ActorKelpProcessingStats {
        public static final long serialVersionUID = 1;
        public int code;
        public int size;
        public volatile int current;

        public ActorKelpProcessingStatsMessageBundle() {}

        public ActorKelpProcessingStatsMessageBundle(ActorKelpProcessingStats prev, int size, int code) {
            this.code = code;
            this.prev = prev;
            this.size = size;
        }

        public void update(int index) {
            this.current = index;
        }

        public double getRate() {
            if (size == 0) {
                return 1.0;
            } else {
                return current / (double) size;
            }
        }

        @Override
        public String toStringContentsInChain(ActorKelpProcessingStats past) {
            String str = "";
            if (past instanceof ActorKelpProcessingStatsMessageBundle) {
                ActorKelpProcessingStatsMessageBundle pastBundle = (ActorKelpProcessingStatsMessageBundle) past;
                if (pastBundle.code != code) {
                    str = "*";
                } else {
                    str = String.format(" [+%,d]", current - pastBundle.current);
                }
            }
            return String.format("msgBundle(%,d/%,d, %2.2f%%)%s", current, size, getRate() * 100.0, str);
        }
    }

    public static class ActorKelpStageEndStats implements Serializable, Cloneable {
        public static final long serialVersionUID = -1;
        public int mergeLoop;
        public int mergingLoaders;
        public long mergingReadKeys;
        public long mergingReadValues;
        public long mergingMergedValues;
        public long mergedBytes;

        public ActorKelpStageEndStats() {}

        public ActorKelpStageEndStats(TreeMerger m) {
            mergeLoop = m.getLoadLoopCount();
            mergingLoaders = m.getTotalMergedLoaders();
            mergingReadKeys = m.getTotalKeys();
            mergingReadValues = m.getTotalValues();
            mergingMergedValues = m.getTotalMergedValues();
            mergedBytes = m.getTotalWriteBytes();
        }

        public ActorKelpStageEndStats copy(long keys, long values) {
            try {
                ActorKelpStageEndStats s = (ActorKelpStageEndStats) super.clone();
                s.mergingReadKeys = keys;
                s.mergingReadValues = values;
                return s;
            } catch (CloneNotSupportedException ex) {
                throw new RuntimeException(ex);
            }
        }

        public String toStringContents(ActorKelpStageEndStats past) {
            if (past == null) {
                return toStringContents();
            } else {
                return String.format("stageEndMerge=(loop:%,d [%+,d] loadedSrcs:%,d [%+,d] keys:%,d [%+,d] vals:%,d [%+,d] writeVals:%,d [%+,d] write:%s)",
                        mergeLoop, mergeLoop - past.mergeLoop,
                        mergingLoaders, mergingLoaders - past.mergingLoaders,
                        mergingReadKeys, mergingReadKeys - past.mergingReadKeys,
                        mergingReadValues, mergingReadValues - past.mergingReadValues,
                        mergingMergedValues, mergingMergedValues - past.mergingMergedValues,
                        PersistentConditionActor.bytesString(mergedBytes));
            }
        }

        public String toStringContents() {
            return String.format("stageEndMerge=(loop:%,d loadedSrcs:%,d keys:%,d vals:%,d writeVals:%,d write:%s)",
                mergeLoop, mergingLoaders, mergingReadKeys, mergingReadValues, mergingMergedValues,
                    PersistentConditionActor.bytesString(mergedBytes));
        }
    }

    public static class ActorStats implements Serializable {
        public static final long serialVersionUID = -1;
        public String head;
        public long mailboxSize = -1L;
        public long mailboxSizeOnMemory = -1L;
        public long nextPending;
        public ActorKelpMessageHandledStats handledStats;
        public ActorKelpProcessingStats processingStats;
        public ActorKelpMailboxTreeStats mailboxTreeStats;
        public ActorKelpStageEndStats stageEndStats;
        public Instant time = Instant.EPOCH;

        public ActorStats() {}

        public void set(Actor self) {
            if (self != null) {
                setMailbox(self.getMailbox());
            }
            if (self instanceof ActorKelp<?>) {
                ActorKelp<?> k = (ActorKelp<?>) self;
                handledStats = k.getHandledStats().copy();
                ActorKelpProcessingStats ps = k.getProcessingStats();
                processingStats = (ps == null ? null : ps.copy());
                mailboxTreeStats = k.getMailboxTreeStats();
                stageEndStats = k.getStageEndStats();

                head =((ActorKelp<?>) self).toStringContentsName();
            } else {
                head = Objects.toString(self);
            }
            if (self instanceof Staging.StagingSupported) {
                nextPending = bufferingCount(((Staging.StagingSupported) self).nextStageActor());
            }
        }

        protected void setMailbox(Mailbox mbox) {
            if (mbox instanceof MailboxManageable) {
                MailboxManageable m = (MailboxManageable) mbox;
                mailboxSize = m.getSize();
                mailboxSizeOnMemory = m.getSizeOnMemory();
            } else if (mbox instanceof MailboxKelp) {
                setMailbox(((MailboxKelp) mbox).getMailbox());
            }
        }

        protected long bufferingCount(ActorRef next) {
            if (next instanceof ActorRefShuffle) {
                return ((ActorRefShuffle) next).getBufferingCount();
            } else if (next instanceof Staging.ActorRefCombined) {
                return ((Staging.ActorRefCombined) next).getMemberActors().stream()
                        .mapToLong(this::bufferingCount)
                        .sum();
            } else {
                return 0;
            }
        }
        public String toString(ActorStats last) {
            List<String> strs = new ArrayList<>();
            strs.add(head);
            if (mailboxSize != -1L) {
                if (last == null) {
                    strs.add(String.format("mbox=(onMem:%,d /total:%,d)", mailboxSizeOnMemory, mailboxSize));
                } else {
                    strs.add(String.format("mbox=(onMem:%,d [%+,d] /total:%,d [%+,d])",
                            mailboxSizeOnMemory, mailboxSizeOnMemory - last.mailboxSizeOnMemory,
                            mailboxSize, mailboxSize - last.mailboxSize));
                }
            }
            if (handledStats != null) {
                strs.add(handledStats.toStringContents(last == null ? null : last.handledStats));
            }
            if (processingStats != null) {
                strs.add(processingStats.toStringContents(last == null ? null : last.processingStats));
            }
            if (mailboxTreeStats != null) {
                strs.add(mailboxTreeStats.toStringContents(last == null ? null : last.mailboxTreeStats));
            }
            if (stageEndStats != null) {
                strs.add(stageEndStats.toStringContents(last == null ? null : last.stageEndStats));
            }
            if (last == null && nextPending > 0) {
                strs.add(String.format("nextPending=%,d", nextPending));
            } else if (last != null && (last.nextPending != 0 || nextPending != 0)) {
                strs.add(String.format("nextPending=%,d [%+,d]", nextPending, nextPending - last.nextPending));
            }
            strs.removeIf(String::isEmpty);
            return String.join(" ", strs);
        }
    }

    public static boolean logDebug = System.getProperty("csl.actor.stats.debug", "false").equals("true");
    public static int logColor = ActorSystem.systemPropertyColor("csl.actor.stats.debugColor", 9);

    public static class GetActorStatTask implements CallableMessage<Actor, ActorStats>, Serializable ,
            Message.MessageDataSpecial {
        public static final long serialVersionUID = -1;

        public ActorStats stats = new ActorStats();
        public Instant time = Instant.now();

        public GetActorStatTask() { }

        public GetActorStatTask(Instant time) {
            if (time != null) {
                this.time = time;
            }
        }

        @Override
        public ActorStats call(Actor self) {
            ActorStats s = this.stats;
            s.time = Instant.now();
            s.set(self);
            if (logDebug) {
                self.getSystem().getLogger().log(true, logColor,
                        "GetActorStatTask %s actor=%s : from=(%s %s)",
                        Thread.currentThread(), self.getName(),
                        time, Duration.between(time, Instant.now()));
            }
            return s;
        }
    }
}
