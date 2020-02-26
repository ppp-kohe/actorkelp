package csl.actor.keyaggregate;

import csl.actor.cluster.ConfigBase;

public class Config extends ConfigBase {
    public static final Config CONFIG_DEFAULT = new Config();

    public int mailboxThreshold = 1000;
    public int mailboxTreeSize = 32;
    public float lowerBoundThresholdFactor = 0.1f;
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
    public int logColor = 33;
    public int logColorPhase = 27;
    public long traverseDelayTimeMs = 300;
    public String persistMailboxPath = "";
    public long persistMailboxSizeLimit = Integer.MAX_VALUE / 64;
    public long persistMailboxOnMemorySize = 100_000L;
    public int reduceRuntimeCheckingThreshold = 100_000;
    public double reduceRuntimeRemainingBytesToSizeRatio = 0.003;

    public int histogramPersistHistoryEntrySize = 10;
    public int histogramPersistHistoryEntryLimit = 100;
    public long histogramPersistSizeLimit = 1000;
    public long histogramPersistOnMemorySize = 100;
    public double histogramPersistSizeRatioThreshold = 0.00001;
    public long histogramPersistRandomSeed = 0;

    public boolean fileMapperSplitByCount = false;
    public long fileMapperSplitLength = 100_000;
    public long fileMapperSplitCount = 10;

    @Override
    protected int getLogColorDefault() {
        return logColor;
    }
}
