package csl.actor.kelp2;

import csl.actor.util.ConfigBase;

public class ConfigKelp extends ConfigBase {
    public static final long serialVersionUID = 1L;
    public static ConfigKelp CONFIG_DEFAULT = new ConfigKelp();
    public String mailboxPath = "%a/mbox";
    public boolean persist = true;
    public int mailboxOnMemorySize = 1_000;
    public int logColor = 17;

    public long systemHostUpdateMs = 3_000;
    public int systemMaxBundle = 30;
    public long systemPendingMessageSize = 10_000;
    public double systemWaitMsFactor = 0.3; //10_000 messages -> 3000ms
}
