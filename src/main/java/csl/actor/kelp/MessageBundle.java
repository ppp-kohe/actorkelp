package csl.actor.kelp;

import csl.actor.ActorRef;
import csl.actor.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class MessageBundle<DataType> extends Message<List<DataType>> {
    public static final long serialVersionUID = 1L;

    public MessageBundle(ActorRef target, ActorRef sender, Iterable<? extends DataType> items) {
        super(target, sender, toList(items));
    }

    public static <DataType> List<DataType> toList(Iterable<? extends DataType> items) {
        ArrayList<DataType> list = new ArrayList<>();
        for (DataType t : items) {
            list.add(t);
        }
        list.trimToSize();
        return list;
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

    public String toStringData(Function<Object, Object> dataToStr) {
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
