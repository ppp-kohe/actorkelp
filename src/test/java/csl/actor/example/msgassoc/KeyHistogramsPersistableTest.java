package csl.actor.example.msgassoc;

import csl.actor.msgassoc.ActorBehaviorBuilderKeyValue;
import csl.actor.msgassoc.KeyHistograms;
import csl.actor.msgassoc.KeyHistogramsPersistable;
import csl.actor.msgassoc.MailboxPersistable;
import csl.actor.remote.KryoBuilder;

public class KeyHistogramsPersistableTest {
    public static void main(String[] args) {
        new KeyHistogramsPersistableTest().run();
    }

    public void run() {
        KeyHistogramsPersistable kh = new KeyHistogramsPersistable(new Conf(), new MailboxPersistable.PersistentFileManager("target/debug-persist",
                new KryoBuilder.SerializerPoolDefault(null)));
        KeyHistogramsPersistable.HistogramTreePersistable tree = kh.create(new ActorBehaviorBuilderKeyValue.KeyComparatorDefault<>(), 3);

        KeyHistograms.HistogramPutContextMap ctx = new KeyHistograms.HistogramPutContextMap();
        ctx.putPosition = 0;
        ctx.putRequiredSize = 2;
        String key = "abcdefghik";
        for (int i = 0; i < 10_000; ++i) {
            String k = "" + key.charAt(i % key.length());
            String data = k + (i / key.length());
            ctx.putValue = data;
            tree.put(k, ctx);
        }


    }

    static class Conf implements KeyHistogramsPersistable.HistogramTreePersistableConfig {

    }
}
