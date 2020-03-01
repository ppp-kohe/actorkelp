package csl.actor.example.wordcount;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.keyaggregate.ActorKeyAggregation;
import csl.actor.keyaggregate.Config;
import csl.actor.keyaggregate.KeyHistogramsPersistable;
import csl.actor.remote.ActorSystemRemote;

public class PersistExample {
    public static void main(String[] args) throws Exception {
        ActorSystemRemote r = new ActorSystemRemote();

        Config conf = new Config();
        conf.persistMailboxPath = "target/debug-persist";
        WordCount.WordCountReducer m = new WordCount.WordCountReducer(r, "r", conf, "");

        ActorKeyAggregation.ActorKeyAggregationSerializable s = m.toSerializable(1);

        Output out = new Output(4096);
        r.getSerializer().write(out, s);

        Input in = new Input(out.getBuffer());
        ActorKeyAggregation.ActorKeyAggregationSerializable s2 = (ActorKeyAggregation.ActorKeyAggregationSerializable) r.getSerializer().read(in);
        ActorKeyAggregation a = s2.create(r, 2);

        KeyHistogramsPersistable.HistogramTreePersistable p = (KeyHistogramsPersistable.HistogramTreePersistable) a.getMailboxAsKeyAggregation().getHistogram(0);
        System.out.println(p.getHistory());

        r.close();
    }
}
