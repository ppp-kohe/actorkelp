package csl.actor.msgassoc;

import csl.actor.*;

public abstract class ActorAggregation extends ActorDefault
        implements KeyHistogramsPersistable.HistogramTreePersistableConfig {
    protected Config config = CONFIG_DEFAULT;
    public static final Config CONFIG_DEFAULT = new Config();

    public ActorAggregation(ActorSystem system, String name, MailboxAggregation mailbox, ActorBehavior behavior) {
        super(system, name, mailbox, behavior);
    }

    public ActorAggregation(ActorSystem system, String name, ActorBehavior behavior) {
        super(system, name, behavior);
    }

    public ActorAggregation(ActorSystem system, ActorBehavior behavior) {
        super(system, behavior);
    }

    public ActorAggregation(ActorSystem system, String name) {
        this(system, name, CONFIG_DEFAULT);
    }

    public ActorAggregation(ActorSystem system) {
        this(system, CONFIG_DEFAULT);
    }

    public ActorAggregation(ActorSystem system, String name, Config config) {
        super(system, name, null, null);
        this.config = config;
        initMailbox();
        behavior = initBehavior();
    }

    public ActorAggregation(ActorSystem system, Config config) {
        this(system, null, config);
    }


    @Override
    protected void initMailbox() {
        MailboxPersistable.PersistentFileManager m = getPersistentFile();
        this.mailbox = new MailboxAggregation(mailboxTreeSize(), initMailboxDefault(m), initTreeFactory(m));
    }

    protected KeyHistograms initTreeFactory(MailboxPersistable.PersistentFileManager m) {
        if (m != null) {
            return new KeyHistogramsPersistable(this, m);
        } else {
            return KeyHistograms.DEFAULT;
        }
    }

    protected MailboxDefault initMailboxDefault(MailboxPersistable.PersistentFileManager m) {
        if (m != null) {
             return new MailboxPersistable(
                            new MailboxPersistable.MessagePersistentFile(m),
                            persistMailboxSizeLimit(), persistMailboxOnMemorySize());
        } else {
            return new MailboxDefault();
        }
    }

    protected MailboxPersistable.PersistentFileManager getPersistentFile() {
        String path = persistMailboxPath();
        if (!path.isEmpty()) {
            return MailboxPersistable.getPersistentFile(system, ()->path);
        } else {
            return null;
        }
    }

    ///// config

    public int mailboxTreeSize() {
        return config.mailboxTreeSize;
    }

    public long traverseDelayTimeMs() {
        return config.traverseDelayTimeMs;
    }

    protected long pruneGreaterThanLeaf() {
        return (long) config.pruneGreaterThanLeafThresholdFactor * config.mailboxThreshold;
    }

    protected double pruneLessThanNonZeroLeafRate() {
        return config.pruneLessThanNonZeroLeafRate;
    }

    protected String persistMailboxPath() {
        return config.persistMailboxPath;
    }

    protected long persistMailboxSizeLimit() {
        return config.persistMailboxSizeLimit;
    }

    protected long persistMailboxOnMemorySize() {
        return config.persistMailboxOnMemorySize;
    }

    protected int reduceRuntimeCheckingThreshold() {
        return config.reduceRuntimeCheckingThreshold;
    }

    protected double reduceRuntimeRemainingBytesToSizeRatio() {
        return config.reduceRuntimeRemainingBytesToSizeRatio;
    }

    @Override
    public int histogramPersistHistoryEntrySize() { return config.histogramPersistHistoryEntrySize; }
    @Override
    public int histogramPersistHistoryEntryLimit() { return config.histogramPersistHistoryEntryLimit; }
    @Override
    public long histogramPersistSizeLimit() { return config.histogramPersistSizeLimit; }
    @Override
    public long histogramPersistOnMemorySize() { return config.histogramPersistOnMemorySize; }
    @Override
    public double histogramPersistSizeRatioThreshold() { return config.histogramPersistSizeRatioThreshold; }
    @Override
    public long histogramPersistRandomSeed() { return config.histogramPersistRandomSeed; }

    /////////////

    @Override
    protected ActorBehaviorBuilderKeyValue behaviorBuilder() {
        return new ActorBehaviorBuilderKeyValue((ps) -> getMailboxAsAggregation().initMessageTable(ps));
    }

    public MailboxAggregation getMailboxAsAggregation() {
        return (MailboxAggregation) mailbox;
    }

    @Override
    public boolean processMessageNext() {
        if (getMailboxAsAggregation().processTable()) {
            return true;
        }
        return super.processMessageNext();
    }

    @Override
    protected void processMessage(Message<?> message) {
        processMessageBehavior(message);
    }

    public void processMessageBehavior(Message<?> message) {
        prune();
        if (message.getData() instanceof MailboxAggregation.TraversalProcess) {
            getMailboxAsAggregation()
                    .processTraversal(this,
                            ((MailboxAggregation.TraversalProcess) message.getData()).entryId,
                            this::nextConsumingSize);
        } else {
            super.processMessage(message);
        }
    }

    public int nextConsumingSize(long size) {
        int consuming = (int) Math.min(Integer.MAX_VALUE, size);
        int rrt = reduceRuntimeCheckingThreshold();
        if (consuming > rrt) { //refer free memory size
            Runtime rt = Runtime.getRuntime();
            consuming = (int) Math.min(consuming,
                    Math.max(rrt, (rt.maxMemory() - rt.totalMemory()) * reduceRuntimeRemainingBytesToSizeRatio()));
        }
        return consuming;
    }

    public void prune() {
        getMailboxAsAggregation().prune(
                pruneGreaterThanLeaf(),
                pruneLessThanNonZeroLeafRate());
    }
}
