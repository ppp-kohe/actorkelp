package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;

public class MessageSerializer {
    public static void register(Kryo kryo) {
        kryo.register(Message.class, new MessageSerializerMessage());
        kryo.register(Message.MessageNone.class, new MessageSerializerNone());
        kryo.register(MessageBundle.MessageAccepted.class, new MessageSerializerAccepted());
        kryo.register(MessageBundle.class, new MessageSerializerBundle());

        kryo.register(Message.MessageDataClock.class, new MessageDataHolderSerializerDataPacket());
        kryo.register(CallableMessage.CallablePacket.class, new MessageDataHolderSerializerCallable());
        kryo.register(Message.MessageDataPacket.class, new MessageDataHolderSerializerDataPacket());
        kryo.register(ActorSystemRemote.MessageDataTransferred.class, new MessageDataHolderSerializerTransferred());
    }


    public static class MessageSerializerMessage extends Serializer<Message<?>> {
        public MessageSerializerMessage() {
            super(true, true);
        }

        @Override
        public void write(Kryo kryo, Output output, Message<?> object) {
            kryo.writeClassAndObject(output, object.target);
            kryo.writeClassAndObject(output, object.data);
        }

        @Override
        public Message<?> read(Kryo kryo, Input input, Class<? extends Message<?>> type) {
            ActorRef tgt = (ActorRef) kryo.readClassAndObject(input);
            Object d = kryo.readClassAndObject(input);
            Message<?> m = new Message<>(tgt, d);
            kryo.reference(m);
            return m;
        }
    }

    public static class MessageSerializerNone extends Serializer<Message.MessageNone> {
        public MessageSerializerNone() {
            super(true, true);
        }

        @Override
        public void write(Kryo kryo, Output output, Message.MessageNone object) {
            kryo.writeClassAndObject(output, object.target);
        }

        @Override
        public Message.MessageNone read(Kryo kryo, Input input, Class<? extends Message.MessageNone> type) {
            ActorRef tgt = (ActorRef) kryo.readClassAndObject(input);
            Message.MessageNone m = new Message.MessageNone(tgt);
            kryo.reference(m);
            return m;
        }
    }

    public static class MessageSerializerAccepted extends Serializer<MessageBundle.MessageAccepted<?>> {
        public MessageSerializerAccepted() {
            super(true, true);
        }

        @Override
        public void write(Kryo kryo, Output output, MessageBundle.MessageAccepted<?> object) {
            kryo.writeClassAndObject(output, object.target);
            kryo.writeClassAndObject(output, object.data);
        }

        @Override
        public MessageBundle.MessageAccepted<?> read(Kryo kryo, Input input, Class<? extends MessageBundle.MessageAccepted<?>> type) {
            ActorRef tgt = (ActorRef) kryo.readClassAndObject(input);
            Object d = kryo.readClassAndObject(input);
            MessageBundle.MessageAccepted<?> m = new MessageBundle.MessageAccepted<>(tgt, d);
            kryo.reference(m);
            return m;
        }
    }

    public static class MessageSerializerBundle extends Serializer<MessageBundle<?>> {
        public MessageSerializerBundle() {
            super(true, true);
        }

        @Override
        public void write(Kryo kryo, Output output, MessageBundle<?> object) {
            kryo.writeClassAndObject(output, object.target);
            kryo.writeClassAndObject(output, object.data);
        }

        @Override
        public MessageBundle<?> read(Kryo kryo, Input input, Class<? extends MessageBundle<?>> type) {
            ActorRef tgt = (ActorRef) kryo.readClassAndObject(input);
            Object d = kryo.readClassAndObject(input);
            MessageBundle<?> m = new MessageBundle<>(tgt, (Iterable<?>) d);
            kryo.reference(m);
            return m;
        }
    }
    public static class MessageDataHolderSerializerClock extends Serializer<Message.MessageDataClock<?>> {
        public MessageDataHolderSerializerClock() {
            super(true, true);
        }

        @Override
        public void write(Kryo kryo, Output output, Message.MessageDataClock<?> object) {
            Message.MessageDataClock<?> c = (Message.MessageDataClock<?>) object;
            output.writeVarInt(c.clock, true);
            kryo.writeClassAndObject(output, c.body);
        }

        @Override
        public Message.MessageDataClock<?> read(Kryo kryo, Input input, Class<? extends Message.MessageDataClock<?>> type) {
            int clock = input.readVarInt(true);
            Object d = kryo.readClassAndObject(input);
            Message.MessageDataClock<?> h = new Message.MessageDataClock<>(clock, d);
            kryo.reference(h);
            return h;
        }
    }

    public static class MessageDataHolderSerializerCallable extends Serializer<CallableMessage.CallablePacket<?,?>> {
        public MessageDataHolderSerializerCallable() {
            super(true, true);
        }

        @Override
        public void write(Kryo kryo, Output output, CallableMessage.CallablePacket<?,?> object) {
            Message.MessageDataPacket<?> m = object;
            kryo.writeClassAndObject(output, m.data);
            kryo.writeClassAndObject(output, m.sender);
        }

        @Override
        public CallableMessage.CallablePacket<?,?> read(Kryo kryo, Input input, Class<? extends CallableMessage.CallablePacket<?,?>> type) {
            Object d = kryo.readClassAndObject(input);
            ActorRef s = (ActorRef) kryo.readClassAndObject(input);
            CallableMessage.CallablePacket<?,?> h = new CallableMessage.CallablePacket<>((CallableMessage<? extends Actor, ? extends Object>) d, s);
            kryo.reference(h);
            return h;
        }
    }
    public static class MessageDataHolderSerializerDataPacket extends Serializer<Message.MessageDataPacket<?>> {
        public MessageDataHolderSerializerDataPacket() {
            super(true, true);
        }

        @Override
        public void write(Kryo kryo, Output output, Message.MessageDataPacket<?> object) {
            Message.MessageDataPacket<?> m = object;
            kryo.writeClassAndObject(output, m.data);
            kryo.writeClassAndObject(output, m.sender);
        }

        @Override
        public Message.MessageDataPacket<?> read(Kryo kryo, Input input, Class<? extends Message.MessageDataPacket<?>> type) {
            Object d = kryo.readClassAndObject(input);
            ActorRef s = (ActorRef) kryo.readClassAndObject(input);
            Message.MessageDataPacket<?> h = new Message.MessageDataPacket<>(d, s);
            kryo.reference(h);
            return h;
        }
    }
    public static class MessageDataHolderSerializerTransferred extends Serializer<ActorSystemRemote.MessageDataTransferred> {
        public MessageDataHolderSerializerTransferred() {
            super(true, true);
        }

        @Override
        public void write(Kryo kryo, Output output, ActorSystemRemote.MessageDataTransferred object) {
            output.writeInt(object.id);
            kryo.writeClassAndObject(output, object.fromAddress);
            kryo.writeClassAndObject(output, object.body);
        }

        @Override
        public ActorSystemRemote.MessageDataTransferred read(Kryo kryo, Input input, Class<? extends ActorSystemRemote.MessageDataTransferred> type) {
            int id = input.readInt();
            ActorAddress.ActorAddressRemote fromAddress = (ActorAddress.ActorAddressRemote) kryo.readClassAndObject(input);
            Object body = kryo.readClassAndObject(input);
            ActorSystemRemote.MessageDataTransferred h = new ActorSystemRemote.MessageDataTransferred(id, fromAddress, body);
            kryo.reference(h);
            return h;
        }
    }

}
