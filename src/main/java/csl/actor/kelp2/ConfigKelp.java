package csl.actor.kelp2;

import csl.actor.util.ConfigBase;

public class ConfigKelp extends ConfigBase {
    public static ConfigKelp CONFIG_DEFAULT = new ConfigKelp();
    public String mailboxPath = "%a/mbox";
    public boolean persist = true;
    public int mailboxOnMemorySize = 1000;
}
