package csl.actor.kelp.actors;

import csl.actor.ActorSystem;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ConfigKelp;

/**
 * default reader class
 * <pre>
 *     var r = new ActorKelpFileReader(system);
 *     r.connects(new MapperActor()); //mappers will receive FileSplit and String lines
 *
 *     r.startReading("file.txt").get();
 * </pre>
 */
public class ActorKelpFileReader extends ActorKelp<ActorKelpFileReader> {
    public ActorKelpFileReader(ActorSystem system, String name, ConfigKelp config) {
        super(system, name, config);
    }

    public ActorKelpFileReader(ActorSystem system, ConfigKelp config) {
        super(system, config);
    }

    public ActorKelpFileReader(ActorSystem system) {
        super(system);
    }

    @Override
    public int getShuffleBufferSizeMax() {
        return getShuffleBufferSizeFile();
    }

    @Override
    public double memoryManageRatioTotalActor() {
        return 0.1; //only handling the initial FileSplit
    }
}
