package csl.actor.kelp2;

import csl.actor.*;
import csl.actor.cluster.PersistentFileManager;
import csl.actor.util.FileSplitter;
import csl.actor.util.StagingActor;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;

public abstract class ActorKelp<SelfType extends ActorKelp<SelfType>> extends ActorDefault
        implements StagingActor.StagingSupported {
    protected ActorRef nextStage;
    protected FileSplitter fileSplitter;
    protected ConfigKelp config;

    public ActorKelp(ActorSystem system, String name, Mailbox mailbox, ActorBehavior behavior, ConfigKelp config) {
        super(system, name, mailbox, behavior);
        if (config == null) {
            this.config = ConfigKelp.CONFIG_DEFAULT;
        } else {
            this.config = config;
        }
    }

    public ActorKelp(ActorSystem system, String name, ConfigKelp config) {
        this(system, name, null, null, config);
        this.mailbox = initMailbox();
        this.behavior = initBehavior();
    }

    public ActorKelp(ActorSystem system, ConfigKelp config) {
        this(system, null, config);
        setNameRandom();
    }

    public ActorKelp(ActorSystem system) {
        this(system, null, (ConfigKelp) null);
        setNameRandom();
    }

    public void setNameRandom() {
        name = getClass().getSimpleName() + "_" + UUID.randomUUID();
        system.register(this);
    }

    /////////////


    public ConfigKelp getConfig() {
        return config;
    }

    public String getMailboxPath() {
        return config.mailboxPath;
    }

    public boolean isPersist() {
        return config.persist;
    }

    public long getMailboxOnMemorySize() {
        return config.mailboxOnMemorySize;
    }

    public long getSplitLength() {
        return config.splitLength;
    }

    ///////////////

    protected PersistentFileManager getPersistentFile() {
        String path = getMailboxPath();
        return PersistentFileManager.getPersistentFile(system, () -> path);
    }

    protected Mailbox initMailbox() {
        PersistentFileManager m = getPersistentFile();
        return initMailboxDefault(m);
    }

    protected MailboxDefault initMailboxDefault(PersistentFileManager m) {
        if (isPersist()) {
            return new MailboxKelp(m, getMailboxOnMemorySize());
        } else {
            return new MailboxDefault();
        }
    }

    @Override
    protected ActorBehaviorBuilderKelp behaviorBuilder() {
        return new ActorBehaviorBuilderKelp();
    }

    public static class MessageBundle<DataType> extends Message<List<DataType>> {
        public static final long serialVersionUID = 1L;

        public MessageBundle(ActorRef target, ActorRef sender, Iterable<? extends DataType> items) {
            super(target, sender, toList(items));
        }

        @Override
        public Message<List<DataType>> renewTarget(ActorRef target) {
            return new MessageBundle<>(target, sender, data);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" +
                    toStringData(Objects::toString) + " : " + target + " <- " + sender + ")";
        }

        @Override
        public String toString(Function<Object, Object> dataToStr) {
            return getClass().getSimpleName() + "(" +
                    toStringData(dataToStr) + " : " + target + " <- " + sender + ")";
        }

        public String toStringData(Function<Object,Object> dataToStr) {
            if (data == null) {
                return "null";
            } else if (data.isEmpty()) {
                return "[0]{}";
            } else {
                return String.format("[%,d]{%s, ...}", data.size(),
                        dataToStr.apply(data.get(0)));
            }
        }
    }

    public static <DataType> List<DataType> toList(Iterable<? extends DataType> items) {
        ArrayList<DataType> list = new ArrayList<>();
        for (DataType t : items) {
            list.add(t);
        }
        list.trimToSize();
        return list;
    }

    public void setNextStage(ActorRef nextStage) {
        this.nextStage = nextStage;
    }

    @Override
    public ActorRef nextStageActor() {
        return nextStage;
    }

    @Override
    public Iterable<? extends ActorRef> nextStageActors() {
        if (nextStage instanceof ActorRefShuffle) {
            return ((ActorRefShuffle) nextStage).getActors();
        } else if (nextStage != null) {
            return Collections.singletonList(nextStage);
        } else {
            return Collections.emptyList();
        }
    }

    /////
    public void processFileSplit(FileSplitter.FileSplit split) {
        if (fileSplitter == null) {
            fileSplitter = FileSplitter.getWithSplitLength(getSplitLength()); 
        }
        try {
            if (split.getFileLength() == 0) {
                if (nextStage != null) {
                    fileSplitter.splitIterator(split.getPath())
                            .forEachRemaining(nextStage::tell);
                } else {
                    fileSplitter.splitIterator(split.getPath())
                            .forEachRemaining(this::tell); //process by self
                }
            } else {
                fileSplitter.openLineIterator(split).forEachRemaining(line ->
                        processMessage(new Message<>(this, this, line)));
                getSystem().getLogger().log("read finish: %s : %s", split, this);
            }
            ActorRefShuffle.flush(nextStageActor(), this);
        } catch (Exception ex) {
            getSystem().getLogger().log(true, 0, ex, "splitter=%s split=%s", fileSplitter, split);
        }
    }

    @SuppressWarnings("unchecked")
    public ActorKelpSerializable<SelfType> toSerializable() {
        return new ActorKelpSerializable<SelfType>((SelfType) this);
    }

    public Serializable toInternalState() {
        return null;
    }

    public void setInternalState(Serializable data) {

    }

    public static class ActorKelpSerializable<SelfType extends ActorKelp<SelfType>> implements Serializable {
        public static final long serialVersionUID = 1L;
        public Class<SelfType> actorType;
        public String name;
        public ConfigKelp config;
        //TODO mailbox
        public Serializable internalState;

        public ActorKelpSerializable(SelfType actor) {
            init(actor);
        }

        protected void init(SelfType actor) {
            initActorType(actor);
            initName(actor);
            initConfig(actor);
            initInternalState(actor);
        }

        @SuppressWarnings("unchecked")
        protected void initActorType(SelfType actor) {
            actorType = (Class<SelfType>) actor.getClass();
        }

        protected void initName(SelfType actor) {
            name = actor.getName();
        }

        protected void initConfig(SelfType actor) {
            config = actor.getConfig();
        }

        protected void initInternalState(SelfType actor) {
            internalState = actor.toInternalState();
        }

        public SelfType restore(ActorSystem system, long num, ConfigKelp config) throws Exception {
            SelfType a = create(system, restoreName(num), config);
            restoreInit(a);
            return a;
        }

        protected String restoreName(long num) {
            return name == null ? ("$" + num) : name;
        }

        protected SelfType create(ActorSystem system, String name, ConfigKelp config) throws Exception {
            return actorType.getConstructor(ActorSystem.class, String.class, ConfigKelp.class)
                    .newInstance(system, name, config);
        }

        protected void restoreInit(SelfType actor) {
            actor.setInternalState(internalState);
        }
    }
}
