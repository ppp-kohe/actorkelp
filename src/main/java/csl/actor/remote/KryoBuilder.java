package csl.actor.remote;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import org.objenesis.instantiator.basic.ObjectStreamClassInstantiator;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.File;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.*;
import java.nio.*;
import java.nio.charset.Charset;
import java.text.*;
import java.time.*;
import java.time.chrono.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import java.util.function.Function;
import java.util.regex.Pattern;

public class KryoBuilder {
    protected ActorSystem system;
    protected Kryo kryo;

    public KryoBuilder setKryo(Kryo kryo) {
        this.kryo = kryo;
        return this;
    }

    public KryoBuilder setSystem(ActorSystem system) {
        this.system = system;
        return this;
    }

    public static Function<ActorSystem, Kryo> builder() {
        return (sys) ->
            new KryoBuilder().setSystem(sys)
                    .build();
    }

    /** @return implementation field getter */
    public Kryo getKryo() {
        return kryo;
    }

    /** @return implementation field getter */
    public ActorSystem getSystem() {
        return system;
    }

    @SuppressWarnings("unchecked")
    public Kryo build() {
        Kryo kryo = (this.kryo == null ? new Kryo() : this.kryo);
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        kryo.addDefaultSerializer(ActorRef.class, new ActorRefRemoteSerializer<>(system));

        register(kryo, getDefaultSerializerClasses());
        register(kryo, getBaseClasses());

        kryo.register(SerializedLambda.class);
        kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());

        registerObjectStream(kryo, EnumMap.class);
        registerObjectStream(kryo, SimpleTimeZone.class);

        for (Class<?> cls : Collections.class.getDeclaredClasses()) {
            if (Serializable.class.isAssignableFrom(cls)) {
                registerObjectStream(kryo, (Class<Serializable>) cls);
            }
        }

        kryo.register(BitSet.class, new BitSetSerializer());

        kryo.register(ActorRef.class, new ActorRefRemoteSerializer<>(system)); //??

        register(kryo, getActorClasses());
        return kryo;
    }


    public void register(Kryo kryo, List<Class<?>> types) {
        for (Class<?> t : types) {
            kryo.register(t);
        }
    }

    @SuppressWarnings("unchecked")
    public void registerObjectStream(Kryo kryo, Class<? extends Serializable> cls) {
        Registration r = kryo.register(cls);
        try {
            cls.getConstructor();
        } catch (Exception ex) {
            r.setInstantiator(new ObjectStreamClassInstantiator(cls));
            r.setSerializer(new JavaSerializer());
        }
    }


    @SuppressWarnings("unchecked")
    public void registerWithSerializable(Kryo kryo, List<Class<?>> types) {
        for (Class<?> t : types) {
            Serializer s = kryo.getDefaultSerializer(t);
            if (Serializable.class.isAssignableFrom(t) && s instanceof FieldSerializer) {
                Registration r = kryo.register(t);
                r.setInstantiator(new ObjectStreamClassInstantiator(t));
                r.setSerializer(new JavaSerializer());
            } else {
                kryo.register(t);
            }
        }
    }


    public List<Class<?>> getDefaultSerializerClasses() {
        return Arrays.asList(
                byte[].class,
                char[].class,
                short[].class,
                int[].class,
                long[].class,
                float[].class,
                double[].class,
                boolean[].class,
                String[].class,
                Object[].class,
                KryoSerializable.class,
                BigInteger.class,
                BigDecimal.class,
                Class.class,
                Date.class,
                Enum.class,
                EnumSet.class,
                Currency.class,
                StringBuffer.class,
                StringBuilder.class,
                Collections.EMPTY_LIST.getClass(),
                Collections.EMPTY_MAP.getClass(),
                Collections.EMPTY_SET.getClass(),
                Collections.singletonList(null).getClass(),
                Collections.singletonMap(null,null).getClass(),
                Collections.singleton(null).getClass(),
                TreeSet.class,
                Collection.class,
                TreeMap.class,
                Map.class,
                TimeZone.class,
                Calendar.class,
                Locale.class,
                Charset.class,
                URL.class,
                Arrays.asList().getClass(),
                void.class,
                PriorityQueue.class);
    }

    public List<Class<?>> getBaseClasses() { //java.base
        return Arrays.asList(
                //java.lang
                Boolean.class, Byte.class, Character.class, Double.class, Object.class,
                Float.class, Integer.class, Long.class, Number.class, Object.class, Short.class, Throwable.class, Void.class,

                //java.io
                File.class,

                //java.net
                Inet4Address.class, Inet6Address.class, InetSocketAddress.class, InterfaceAddress.class, URI.class,

                //java.nio
                Buffer.class, ByteBuffer.class, ByteOrder.class, CharBuffer.class, DoubleBuffer.class, FloatBuffer.class,
                IntBuffer.class, LongBuffer.class, ShortBuffer.class,

                //java.text
                AttributedCharacterIterator.Attribute.class, AttributedString.class, ChoiceFormat.class, DateFormat.class,
                DateFormat.Field.class, DateFormatSymbols.class, DecimalFormat.class, DecimalFormatSymbols.class,
                Format.class, Format.Field.class, MessageFormat.class, MessageFormat.Field.class,
                NumberFormat.class, NumberFormat.Field.class, SimpleDateFormat.class,

                //java.time
                Duration.class, Instant.class, LocalDate.class, LocalDateTime.class, LocalTime.class,
                MonthDay.class, OffsetDateTime.class, OffsetTime.class, Period.class, Year.class, YearMonth.class,
                ZonedDateTime.class, ZoneId.class, ZoneOffset.class, DayOfWeek.class, Month.class,

                //java.time.chrono
                HijrahDate.class, HijrahEra.class, JapaneseEra.class, JapaneseDate.class, MinguoEra.class, MinguoDate.class,
                ThaiBuddhistDate.class, ThaiBuddhistEra.class, IsoEra.class,

                //java.util
                ArrayList.class, ArrayDeque.class, BitSet.class, Calendar.class, Currency.class, Date.class,
                EnumMap.class, GregorianCalendar.class, HashMap.class, HashSet.class,  Hashtable.class, IdentityHashMap.class,
                LinkedHashMap.class, LinkedHashSet.class, LinkedList.class, Locale.class,
                Locale.class, Optional.class, OptionalInt.class, OptionalDouble.class, OptionalLong.class,
                PriorityQueue.class, Properties.class, Random.class, SimpleTimeZone.class, Stack.class,
                TreeMap.class, TreeSet.class, UUID.class, Vector.class, WeakHashMap.class,

                //java.util.concurrent
                TimeUnit.class,

                //java.util.concurrent.atomic
                AtomicBoolean.class, AtomicInteger.class, AtomicIntegerArray.class, AtomicLong.class, AtomicLongArray.class,
                AtomicReference.class, AtomicReferenceArray.class,

                //java.util.regex,
                Pattern.class);
    }

    public List<Class<?>> getActorClasses() {
        return Arrays.asList(
                Message.class,
                ActorAddress.ActorAddressRemote.class,
                ActorAddress.ActorAddressRemoteActor.class);
    }

    public static class BitSetSerializer extends Serializer<BitSet> {
        @Override
        public void write(Kryo kryo, Output output, BitSet object) {
            byte[] bs = object.toByteArray();
            output.writeInt(bs.length);
            output.write(bs);
        }

        @Override
        public BitSet read(Kryo kryo, Input input, Class<? extends BitSet> type) {
            int n = input.readInt();
            return BitSet.valueOf(input.readBytes(n));
        }
    }
}
