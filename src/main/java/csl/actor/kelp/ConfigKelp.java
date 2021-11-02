package csl.actor.kelp;

import csl.actor.util.ConfigBase;

public class ConfigKelp extends ConfigBase {
    public static final long serialVersionUID = 1L;
    public static ConfigKelp CONFIG_DEFAULT = new ConfigKelp();
    public String mailboxPath = "%l/%a/mbox";
    public String outputDir = "%a";
    public int mailboxOnMemorySize = 1_000;
    public int logColor = 33;
    public boolean logSplit = true;
    public String logHeader = "";

    public boolean persist = true;
    public boolean persistRuntimeCondition = true;
    public double totalActorMemoryRate = 0.7;
    public double memoryCheckedThreshold = 0.7;
    public double memoryPersistThreshold = 0.3;

    public long systemHostUpdateMs = 3_000;
    public int systemMaxBundle = 30;
    public long systemPendingMessageSize = 10_000;
    public double systemWaitMsFactor = 0.3; //10_000 messages -> 3000ms
    public long systemPendingMessageLimit = 150_000;

    public long fileLineWaitMs = 10; //wait per fileLineWaitLines
    public long fileLineWaitLines = 1_000;

    public int shuffleBufferSize = 512;
    public int shuffleBufferSizeFile = 0;
    public int shufflePartitions = 30;
    public long splitLength = 32 * 1024L * 1024L;

    public long traverseDelayTimeMs = 300;
    public int reduceRuntimeCheckingThreshold = 100_000;
    public double reduceRuntimeRemainingBytesToSizeRatio = (1.0 / 300.0) / 100.0; //1/messages /100 trees : e.g. 4GB available, 12M/100 = 120K messages/tree
    public float pruneGreaterThanLeafThresholdFactor = 2f;
    public float pruneLessThanNonZeroLeafRate = 0.2f;

    public int mailboxThreshold = 100_000;
    public int mailboxTreeSize = 100;

    public long systemMailboxPendingMessageSize = 300_000;
    public double systemMailboxWaitMsFactor = 0.006;

    public int histogramPersistHistoryEntrySize = 10;
    public int histogramPersistHistoryEntryLimit = 100;
    public long histogramPersistSizeLimit = 100_000;
    public long histogramPersistOnMemorySize = 100;
    public double histogramPersistSizeRatioThreshold = 0.00001;
    public long histogramPersistRandomSeed = 0;

    public long dispatcherRandomSeed = 10101;

    @Override
    public int getLogColorDefault() {
        return logColor;
    }

    @Override
    public FormatAndArgs logMessageHeader() {
        return super.logMessageHeader().append(logMessageHeaderCustom());
    }

    public FormatAndArgs logMessageHeaderCustom() {
        if (logHeader.isEmpty()) {
            return new FormatAndArgs("");
        } else {
            return new FormatAndArgs("%s ", logHeader);
        }
    }
}
