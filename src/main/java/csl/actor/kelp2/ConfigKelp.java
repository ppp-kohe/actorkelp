package csl.actor.kelp2;

import csl.actor.util.ConfigBase;

public class ConfigKelp extends ConfigBase {
    public static final long serialVersionUID = 1L;
    public static ConfigKelp CONFIG_DEFAULT = new ConfigKelp();
    public String mailboxPath = "%a/mbox";
    public boolean persist = true;
    public int mailboxOnMemorySize = 1_000;
    public int logColor = 17;
    public boolean logSplit = true;
    public String logHeader = "";

    public long systemHostUpdateMs = 3_000;
    public int systemMaxBundle = 30;
    public long systemPendingMessageSize = 10_000;
    public double systemWaitMsFactor = 0.3; //10_000 messages -> 3000ms
    public long systemPendingMessageLimit = 150_000;

    public int shuffleBufferSize = 512;
    public int shuffleBufferSizeFile = 0;
    public int shufflePartitions = 30;
    public boolean shuffleHostIncludePort = false;
    public long splitLength = 32 * 1024L * 1024L;


    public long traverseDelayTimeMs = 300;
    public int reduceRuntimeCheckingThreshold = 100_000;
    public double reduceRuntimeRemainingBytesToSizeRatio = 0.003;
    public float pruneGreaterThanLeafThresholdFactor = 2f;
    public float pruneLessThanNonZeroLeafRate = 0.2f;
    public int mailboxThreshold = 1000;


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
