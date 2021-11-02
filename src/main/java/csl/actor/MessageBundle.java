package csl.actor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class MessageBundle<DataType> extends Message<List<DataType>> {
    public static final long serialVersionUID = 1L;

    public MessageBundle() {}

    public MessageBundle(ActorRef target, Iterable<? extends DataType> items) {
        super(target, toList(items));
    }

    @Override
    public int dataSize() {
        return data.size();
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
        return new MessageBundle<>(target, data);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
                toStringData(Objects::toString) + " : " + target  + ")";
    }

    @Override
    public String toString(Function<Object, Object> dataToStr) {
        return getClass().getSimpleName() + "(" +
                toStringData(dataToStr) + " : " + target  + ")";
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

    public static void processMessageBundle(Actor self, MessageBundle<Object> mb) {
        mb.getData().forEach(d ->
                self.processMessage(new MessageBundle.MessageAccepted<>(self, d))); //MessageBundle is already accepted by Dispatcher
    }

    public static class MessageAccepted<T> extends Message<T> {
        public static final long serialVersionUID = -1;

        public MessageAccepted() {}

        public MessageAccepted(ActorRef target, T data) {
            super(target, data);
        }
    }
}
