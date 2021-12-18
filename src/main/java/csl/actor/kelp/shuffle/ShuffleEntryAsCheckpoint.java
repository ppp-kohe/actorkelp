package csl.actor.kelp.shuffle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.persist.PersistentFileManager;
import csl.actor.util.Staging;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ShuffleEntryAsCheckpoint extends ActorRefShuffle.ShuffleEntry {
    public static final long serialVersionUID = -1;
    protected String path;
    protected String name;

    protected transient ActorSystem system;
    protected transient PersistentFileManager.PersistentFileWriter writer;

    public ShuffleEntryAsCheckpoint() {}

    public ShuffleEntryAsCheckpoint(ActorRef actor, int bufferSize, int index,
                                    ActorSystem system, String path, String name) {
        super(actor, bufferSize, index);
        this.system = system;
        this.path = path;
        this.name = name;
    }

    @Override
    public ActorRefShuffle.ShuffleEntry copy() {
        return new ShuffleEntryAsCheckpoint(actor, bufferSize, index, system, path, name);
    }

    public void setSystemBySerializer(ActorSystem system) {
        this.system = system;
    }


    public static List<ActorRefShuffle.ShuffleEntry> createDispatchUnits(Iterable<? extends ActorRef> actors, int bufferSize,
                                                                         ActorSystem system, String path, String name) {
        ArrayList<ActorRefShuffle.ShuffleEntry> refs = new ArrayList<>();
        int i = 0;
        for (ActorRef ref : actors) {
            refs.add(new ShuffleEntryAsCheckpoint(ref, bufferSize, i, system, path, name));
            ++i;
        }
        refs.trimToSize();
        return refs;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        super.write(kryo, output);
        output.writeString(path);
        output.writeString(name);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        super.read(kryo, input);
        path = input.readString();
        name = input.readString();
    }

    @Override
    public void tell(Object data) {
        super.tell(data);
        if (writer == null) {
            writer = PersistentFileManager.getPersistentFile(system, path)
                    .createWriterForHead(String.format("shuffle-%s-%03d", toFileName(), index));
            writer.write(this); //write self as header
        }
        writer.write(data);
    }

    public static Pattern FILE_PATTERN = Pattern.compile("(?U)[^\\w\\-_]+");

    protected String toFileName() {
        String str = Staging.shortenId(name);
        str = FILE_PATTERN.matcher(str)
                .replaceAll("_");
        if (str.length() > 200) {
            return str.substring(0, 200);
        }
        return str;
    }

    @Override
    public void flush() {
        super.flush();
        if (writer != null) {
            writer.flush();
        }
    }

    public String getPath() {
        return path;
    }

    public String getName() {
        return name;
    }
}
