package csl.actor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Actor implements ActorRef {
    protected transient ActorSystem system;
    protected Mailbox mailbox;
    protected volatile String name;
    protected transient AtomicBoolean processLock = new AtomicBoolean(false);
    protected transient AtomicBoolean processLockSpecial = new AtomicBoolean(false);
    protected Mailbox specialMailbox;
    protected volatile Mailbox delayedMailbox;

    protected Map<Object, Integer> clocks = new HashMap<>();

    public ActorSystem.ProcessMessage messageRunner;
    public ActorSystem.ProcessMessage messageRunnerSpecial;

    public Actor(ActorSystem system) {
        this(system, null);
    }

    public Actor(ActorSystem system, String name) {
        this(system, name, null);
        mailbox = initMailbox();
    }

    public Actor(ActorSystem system, String name, Mailbox mailbox) {
        this.system = system;
        this.name = name;
        if (system != null && name != null) {
            system.register(this);
        }
        this.mailbox = mailbox;
        this.specialMailbox = new MailboxDefault();
        if (system != null) {
            messageRunner = system.createProcessMessageSubsequently(this, false);
            messageRunnerSpecial = system.createProcessMessageSubsequently(this, true);
        }
    }

    public static final String NAME_SYSTEM_SEPARATOR = "#";
    public static final String NAME_ID_SEPARATOR = "@";

    /**
     * the naming rule for actors:
     *  <ul>
     *      <li>an OS process has one namespace. In a different process might has an actor with the duplicated name </li>
     *      <li>the library code can append suffix <code>#str</code>.
     *           Also, some name <code>#str</code> might be reserved by the library</li>
     *      <li>a network address for an actor becomes <code>host:port/name</code>.</li>
     *      <li>also an anonymous actor address takes the form <code>host:port/#anon:typeName@hexCode</code></li>
     *      <li>name can be <code>name@randomUUID</code>, e.g. <code>MyActorType@2920a51e-3519-454e-82de-5e1355aa539c</code></li>
     *      <li>name can be separated by <code>.</code> as type names or member references,
     *           e.g. <code>my.pack.MyType.placement</code></li>
     *  </ul>
     *  So the syntax is like following:
     *  <pre>
     *      addr ::= host ":" port "/" name
     *      name ::= ["#"] nameSuffix
     *      nameSuffix ::= str ["@" str] ["#" nameSuffix]
     *      str ::=  ("."|a-zA-Z0-9|"$"|"_"|"-")+
     *  </pre>
     * @return the name or null
     */
    public String getName() {
        return name;
    }

    protected Mailbox initMailbox() {
        return new MailboxDefault();
    }

    public Mailbox getMailbox() {
        return mailbox;
    }

    public ActorSystem getSystem() {
        return system;
    }

    public void offer(Message<?> message) {
        if (system.isSpecialMessage(message)) {
            specialMailbox.offer(message);
        } else if (isDelayedMessage(message)) {
            getDelayedMailbox().offer(message);
        } else {
            mailbox.offer(message);
        }
    }

    public boolean isDelayedMessage(Message<?> msg) {
        return msg != null && isDelayedMessageData(msg.getData());
    }

    public boolean isDelayedMessageData(Object data) {
        return data instanceof Message.MessageDataDelayed ||
                (data instanceof Message.MessageDataHolder<?> &&
                        isDelayedMessageData(((Message.MessageDataHolder<?>) data).getData()));
    }

    public void putClock(Object addr, int clock) {
        clocks.compute(addr, (_a, exClock) -> (exClock == null || exClock < clock) ? clock : exClock);
    }

    /**
     * @return a table: usually {senderHost -&gt; receivedMaxClock}. only updated by {@link #processMessageSystemClock(Message.MessageDataClock)}
     */
    public Map<Object, Integer> getClocks() {
        return clocks;
    }

    public boolean processMessageBefore() {
        return processLock.compareAndSet(false, true);
    }

    public boolean processMessageLocked() {
        return processLock.get();
    }

    public void processMessageAfter() {
        processLock.set(false);
    }

    public boolean processMessageBeforeSpecial() {
        return processLockSpecial.compareAndSet(false, true);
    }

    public boolean processMessageLockedSpecial() {
        return processLockSpecial.get();
    }

    public void processMessageAfterSpecial() {
        processLockSpecial.set(false);
    }


    /**
     * @return true if {@link #getMailbox()} is empty. No checking for {@link #getDelayedMailbox()}
     */
    public boolean isEmptyMailbox() {
        return mailbox.isEmpty();
    }

    /**
     * @return true if both {@link #getMailbox()} and {@link #getDelayedMailbox()}
     */
    public boolean isEmptyMailboxAll() {
        Mailbox dm;
        return mailbox.isEmpty() &&
                specialMailbox.isEmpty() &&
                (((dm = delayedMailbox) == null) || dm.isEmpty());
    }

    /**
     * @return poll and process a message in the mailbox. using {@link #getMailbox()} and {@link #getDelayedMailbox()}
     */
    public boolean processMessageNext() {
        Message<?> message = mailbox.poll();
        if (message != null) {
            if (!processMessageSystem(message)) {
                processMessage(message);
            }
            return true;
        } else {
            Mailbox dm = delayedMailbox;
            if (dm != null && !dm.isEmpty() &&
                    (message = dm.poll()) != null) {
                if (!processMessageSystem(message)) {
                    processDelayedMessage(message);
                }
                return true;
            } else {
                return false;
            }
        }
    }

    public boolean processMessageNextSpecial() {
        Message<?> message = specialMailbox.poll();
        if (message != null) {
            processMessageSpecial(message);
            return true;
        } else {
            return false;
        }
    }

    public void processMessageSpecial(Message<?> message) {
        processMessage(message);
    }

    /**
     * process system messages. 
     *  As the default impl., {@link Message.MessageDataClock} is processed by {@link #processMessageSystemClock(Message.MessageDataClock)}
     * @param message any message received by the actor
     * @return true if the message is a system message, and processed it
     */
    public boolean processMessageSystem(Message<?> message) {
        Object msg;
        if ((msg = message.getData()) instanceof Message.MessageDataClock<?>) { //MessageDataClock is a holder, so it just getData
            processMessageSystemClock((Message.MessageDataClock<?>) msg);
            return true;
        } else {
            return false;
        }
    }

    /**
     * the method is only running under non-special thread
     * (from {@link #processMessageSystem(Message)} &lt; {@link #processMessageNext()}).
     * So the {@link #getClocks()} can be accessed without locking for updating.
     *
     * @param message ClockMessage(clock, ActorAddressRemote serverAddr)
     *                from ActorSystemRemote.MessageDeliveringActor{.deliver -&gt; .sendLocal}
     */
    protected void processMessageSystemClock(Message.MessageDataClock<?> message) {
        putClock(message.body, message.clock);
    }

    /**
     * process regular message; system messages are processed by {@link #processMessageSystem(Message)}
     * @param message non-system message
     */
    public abstract void processMessage(Message<?> message);


    @Override
    public void tellMessage(Message<?> message) {
        system.send(message);
    }

    /**
     * @return another mailbox used for observing phase completion
     */
    public Mailbox getDelayedMailbox() {
        if (delayedMailbox == null) {
            synchronized (this) {
                if (delayedMailbox == null) {
                    delayedMailbox = initDelayedMailbox();
                }
            }
        }
        return delayedMailbox;
    }

    protected Mailbox initDelayedMailbox() {
        return new MailboxDefault();
    }

    protected void processDelayedMessage(Message<?> message) {
        processMessage(message);
    }

}
