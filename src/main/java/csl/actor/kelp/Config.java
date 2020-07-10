package csl.actor.kelp;

import csl.actor.util.ConfigBase;

public class Config extends ConfigBase {
    public static final long serialVersionUID = 1L;
    public static final Config CONFIG_DEFAULT = new Config();

    public boolean routerAutoSplit = true;
    public boolean routerAutoMerge = true;
    public int mailboxThreshold = 1000;
    public int mailboxTreeSize = 32;
    public float lowerBoundThresholdFactor = 0.001f;
    public int minSizeOfEachMailboxSplit = 10;
    public float maxParallelRoutingThresholdFactor = 5.0f;
    public int historyEntrySize = 10;
    public float historyEntryLimitThresholdFactor = 0.1f;
    public float historyExceededLimitThresholdFactor = 0.3f;
    public float mergeRatioThreshold = 0.2f;
    public float pruneGreaterThanLeafThresholdFactor = 2f;
    public float pruneLessThanNonZeroLeafRate = 0.2f;
    public long toLocalWaitMs = 20_000;
    public boolean logSplit = true;
    public int logColor = 17;
    public int logColorPhase = 27;
    public long traverseDelayTimeMs = 300;
    public boolean persist = false;
    public String persistMailboxPath = "%a/persist-%h";
    public boolean persistRuntimeCondition = true;
    public long persistMailboxSizeLimit = Integer.MAX_VALUE / 512;
    public long persistMailboxOnMemorySize = 100_000L;
    public int reduceRuntimeCheckingThreshold = 100_000;
    public double reduceRuntimeRemainingBytesToSizeRatio = 0.003;

    public int histogramPersistHistoryEntrySize = 10;
    public int histogramPersistHistoryEntryLimit = 100;
    public long histogramPersistSizeLimit = 100_000;
    public long histogramPersistOnMemorySize = 100;
    public double histogramPersistSizeRatioThreshold = 0.00001;
    public long histogramPersistRandomSeed = 0;

    public boolean fileMapperSplitByCount = false;
    public long fileMapperSplitLength = 100_000;
    public long fileMapperSplitCount = 10;

    public String logHeader = "";

    @Override
    public int getLogColorDefault() {
        return logColor;
    }

    @Override
    protected FormatAndArgs logMessageHeader() {
        if (logHeader.isEmpty()) {
            return super.logMessageHeader();
        } else {
            return super.logMessageHeader().append(new FormatAndArgs("%s ", logHeader));
        }
    }
}
