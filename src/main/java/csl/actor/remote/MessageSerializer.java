package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.*;

public class MessageSerializer {
    public static void register(Kryo kryo) {
        kryo.register(Message.class, new MessageBaseSerializer());
        kryo.register(Message.MessageNone.class, new MessageBaseSerializer());
        kryo.register(MessageBundle.MessageAccepted.class, new MessageBaseSerializer());
        kryo.register(MessageBundle.class, new MessageBaseSerializer());

        kryo.register(Message.MessageDataClock.class, new MessageDataHolderSerializer());
        kryo.register(CallableMessage.CallablePacket.class, new MessageDataHolderSerializer());
        kryo.register(Message.MessageDataHolder.class, new MessageDataHolderSerializer());
    }

    public static class MessageBaseSerializer extends Serializer<Message<?>> {
        public MessageBaseSerializer() {
            super(true, true);
        }

        @Override
        public void write(Kryo kryo, Output output, Message<?> object) {
            if (object instanceof KryoSerializable) {
                ((KryoSerializable) object).write(kryo, output);
            } else {
                kryo.writeClassAndObject(output, object.target);
                kryo.writeClassAndObject(output, object.data);
            }
        }

        @Override
        public Message<?> read(Kryo kryo, Input input, Class<? extends Message<?>> type) {
            ActorRef tgt = (ActorRef) kryo.readClassAndObject(input);
            Object d = kryo.readClassAndObject(input);
            if (type.equals(Message.class)) {
                Message<?> m = new Message<>(tgt, d);
                kryo.reference(m);
                return m;
            } else if (type.equals(Message.MessageNone.class)) {
                Message.MessageNone m = new Message.MessageNone(tgt);
                kryo.reference(m);
                return m;
            } else if (type.equals(MessageBundle.MessageAccepted.class)) {
                MessageBundle.MessageAccepted<?> m = new MessageBundle.MessageAccepted<>(tgt, d);
                kryo.reference(m);
                return m;
            } else if (type.equals(MessageBundle.class)) {
                MessageBundle<?> m = new MessageBundle<>(tgt, (Iterable<?>) d);
                kryo.reference(m);
                return m;
            } else {
                Object o = kryo.newInstance(type);
                kryo.reference(o);
                if (o instanceof KryoSerializable) {
                    ((KryoSerializable) o).read(kryo, input);
                }
                return (Message<?>) o;
            }
        }
    }

    public static class MessageDataHolderSerializer extends Serializer<Message.MessageDataHolder<?>> {
        public MessageDataHolderSerializer() {
            super(true, true);
        }

        @Override
        public void write(Kryo kryo, Output output, Message.MessageDataHolder<?> object) {
            if (object instanceof Message.MessageDataClock<?>) {
                Message.MessageDataClock<?> c = (Message.MessageDataClock<?>) object;
                output.writeVarInt(c.clock, true);
                kryo.writeClassAndObject(output, c.body);
            } else if (object instanceof CallableMessage.CallablePacket<?,?>) {
                Message.MessageDataPacket<?> m = (Message.MessageDataPacket<?>) object;
                kryo.writeClassAndObject(output, m.data);
                kryo.writeClassAndObject(output, m.sender);
            } else if (object instanceof Message.MessageDataPacket<?>) {
                Message.MessageDataPacket<?> m = (Message.MessageDataPacket<?>) object;
                kryo.writeClassAndObject(output, m.data);
                kryo.writeClassAndObject(output, m.sender);
            } else if (object instanceof KryoSerializable) {
                ((KryoSerializable) object).write(kryo, output);
            }
        }

        @Override
        public Message.MessageDataHolder<?> read(Kryo kryo, Input input, Class<? extends Message.MessageDataHolder<?>> type) {
            if (Message.MessageDataClock.class.equals(type)) {
                int clock = input.readVarInt(true);
                Object d = kryo.readClassAndObject(input);
                Message.MessageDataHolder<?> h = new Message.MessageDataClock<>(clock, d);
                kryo.reference(h);
                return h;
            } else if (CallableMessage.CallablePacket.class.equals(type)) {
                Object d = kryo.readClassAndObject(input);
                ActorRef s = (ActorRef) kryo.readClassAndObject(input);
                Message.MessageDataHolder<?> h = new CallableMessage.CallablePacket<>((CallableMessage<? extends Actor, ? extends Object>) d, s);
                kryo.reference(h);
                return h;
            } else if (Message.MessageDataPacket.class.equals(type)) {
                Object d = kryo.readClassAndObject(input);
                ActorRef s = (ActorRef) kryo.readClassAndObject(input);
                Message.MessageDataHolder<?> h = new Message.MessageDataPacket<>(d, s);
                kryo.reference(h);
                return h;
            } else {
                Object o = kryo.newInstance(type);
                kryo.reference(o);
                if (o instanceof KryoSerializable) {
                    ((KryoSerializable) o).read(kryo, input);
                }
                return (Message.MessageDataHolder<?>) o;
            }
        }
    }
}
