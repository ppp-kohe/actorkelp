package csl.actor.remote;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.util.Pool;
import csl.actor.*;
import csl.actor.util.ConfigBase;
import csl.actor.util.FileSplitter;
import csl.actor.util.ResponsiveCalls;
import csl.actor.util.StagingActor;
import org.objenesis.instantiator.basic.ObjectStreamClassInstantiator;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.File;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Constructor;
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
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class KryoBuilder {
    protected ActorSystem system;
    protected Kryo kryo;

    public static boolean debugLog = System.getProperty("csl.actor.debugKryo", "false").equals("true");
    public static int debugLogColor = ActorSystem.systemPropertyColor("csl.actor.debugKryo.color", 100);

    public KryoBuilder setKryo(Kryo kryo) {
        this.kryo = kryo;
        return this;
    }

    public KryoBuilder setSystem(ActorSystem system) {
        this.system = system;
        return this;
    }

    public static Function<ActorSystem, Kryo> builder() {
        return builder(new Creator(KryoBuilder.class));
    }

    public static Function<ActorSystem, Kryo> builder(Supplier<KryoBuilder> builderCreator) {
        return new Builder(builderCreator);
    }

    public static Function<ActorSystem, Kryo> builder(Class<? extends KryoBuilder> kryoBuilderType) {
        try {
            return KryoBuilder.builder(new Creator(kryoBuilderType));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class Builder implements Function<ActorSystem, Kryo> {
        protected Supplier<KryoBuilder> creator;

        public Builder(Supplier<KryoBuilder> creator) {
            this.creator = creator;
        }

        @Override
        public Kryo apply(ActorSystem actorSystem) {
            return creator.get().setSystem(actorSystem).build();
        }
    }

    public static class Creator implements Supplier<KryoBuilder> {
        protected Class<? extends KryoBuilder> type;
        protected Constructor<? extends KryoBuilder> cons;

        public Creator(Class<? extends KryoBuilder> type) {
            this.type = type;
            try {
                cons = type.getConstructor();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public KryoBuilder get() {
            try {
                return cons.newInstance();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /** @return implementation field getter */
    public Kryo getKryo() {
        return kryo;
    }

    /** @return implementation field getter */
    public ActorSystem getSystem() {
        return system;
    }

    public Kryo build() {
        Kryo kryo = buildKryoGetOrCreate();
        buildKryoInit(kryo);
        buildRegisterBasic(kryo);
        buildRegisterLambda(kryo);
        //java17: buildRegisterBasicAdditional(kryo);
        buildRegisterActor(kryo);
        return kryo;
    }

    protected Kryo buildKryoGetOrCreate() {
        return (this.kryo == null ? new Kryo() : this.kryo);
    }

    protected void buildKryoInit(Kryo kryo) {
        kryo.setRegistrationRequired(false);
        kryo.setReferences(true);
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        /*
        var conf = new FieldSerializer.FieldSerializerConfig();
        conf.setFieldsAsAccessible(false);
        kryo.setDefaultSerializer(new com.esotericsoftware.kryo.SerializerFactory.FieldSerializerFactory(conf));
         */
        kryo.setDefaultSerializer(new PublicFieldSerializer.SerializerFactoryPublicField<Object>());
    }

    protected void buildRegisterBasic(Kryo kryo) {
        KyroBaseSerializer.register(kryo);
        register(kryo, getDefaultSerializerClasses());
        register(kryo, getBaseClasses());
    }

    protected void buildRegisterLambda(Kryo kryo) {
        //kryo.register(SerializedLambda.class);
        //kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer()); //java17
        kryo.register(SerializedLambda.class, new JavaSerializer());
        kryo.register(ClosureSerializer.Closure.class, new JavaSerializer());
    }

    @SuppressWarnings("unchecked")
    @Deprecated
    protected void buildRegisterBasicAdditional(Kryo kryo) {
        registerObjectStream(kryo, EnumMap.class);
        registerObjectStream(kryo, SimpleTimeZone.class);

        for (Class<?> cls : Collections.class.getDeclaredClasses()) {
            if (Serializable.class.isAssignableFrom(cls)) {
                registerObjectStream(kryo, (Class<Serializable>) cls);
            }
        }
    }

    protected void buildRegisterActor(Kryo kryo) {
        buildRegisterActorRef(kryo);
        register(kryo, getActorClasses());
    }

    protected void buildRegisterActorRef(Kryo kryo) {
        kryo.addDefaultSerializer(ActorRef.class, new ActorRefRemoteSerializer<>(system)); //for sub-types
         // subsequent kryo.register(t) obtains the default serializer. So the sub-types of ActorRef will use the serializer
        kryo.addDefaultSerializer(ActorAddress.class, new ActorAddressSerializer());
    }

    public void register(Kryo kryo, List<Class<?>> types) {
        for (Class<?> t : types) {
            kryo.register(t);
        }
    }

    public void register(Kryo kryo, Class<?>... types) {
        register(kryo, Arrays.asList(types));
    }

    @SuppressWarnings("unchecked")
    public void registerObjectStream(Kryo kryo, Class<? extends Serializable> cls) {
        Registration r = kryo.register(cls);
        try {
            cls.getConstructor();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
            //java17
            //r.setInstantiator(new ObjectStreamClassInstantiator<>(cls));
            //r.setSerializer(new JavaSerializer());
        }
    }


    @SuppressWarnings("unchecked")
    public void registerWithSerializable(Kryo kryo, List<Class<?>> types) {
        for (Class<?> t : types) {
            Serializer<?> s = kryo.getDefaultSerializer(t);
            if (Serializable.class.isAssignableFrom(t) && s instanceof FieldSerializer) {
                Registration r = kryo.register(t);
                r.setInstantiator(new ObjectStreamClassInstantiator<>(t));
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
                PriorityQueue.class,
                BitSet.class,

                Boolean.class, Byte.class, Character.class, Double.class, Object.class,
                Float.class, Integer.class, Long.class, Number.class, Object.class, Short.class, Throwable.class, Void.class,
                //java.time
                Duration.class, Instant.class, LocalDate.class, LocalDateTime.class, LocalTime.class,
                MonthDay.class, OffsetDateTime.class, OffsetTime.class, Period.class, Year.class, YearMonth.class,
                ZonedDateTime.class, ZoneId.class, ZoneOffset.class, DayOfWeek.class, Month.class);
    }
    public List<Class<?>> getBaseClasses() { //java.base
        return Arrays.asList(
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



                //java.util
                ArrayList.class, ArrayDeque.class, BitSet.class,
                EnumMap.class, GregorianCalendar.class, HashMap.class, HashSet.class,  Hashtable.class, IdentityHashMap.class,
                LinkedHashMap.class, LinkedHashSet.class, LinkedList.class,

                PriorityQueue.class, Properties.class, Random.class, SimpleTimeZone.class, Stack.class,
                TreeMap.class, TreeSet.class , Vector.class, WeakHashMap.class,

                Optional.class, OptionalInt.class, OptionalDouble.class, OptionalLong.class,

                UUID.class, Locale.class, Calendar.class, Date.class, Currency.class,

                //java.util.concurrent
                TimeUnit.class,

                //java.time.chrono
                HijrahDate.class, HijrahEra.class, JapaneseEra.class, JapaneseDate.class, MinguoEra.class, MinguoDate.class,
                ThaiBuddhistDate.class, ThaiBuddhistEra.class, IsoEra.class,

                //java.util.concurrent.atomic
                AtomicBoolean.class, AtomicInteger.class, AtomicIntegerArray.class, AtomicLong.class, AtomicLongArray.class,
                AtomicReference.class, AtomicReferenceArray.class,

                //java.util.regex,
                Pattern.class);
    }

    public List<Class<?>> getActorClasses() {
        return Arrays.asList(
                ActorRef.class,
                Message.class,
                ActorAddress.ActorAddressRemote.class,
                ActorAddress.ActorAddressRemoteActor.class,
                ActorSystemDefault.DeadLetter.class,
                CallableMessage.class,
                CallableMessage.CallableFailure.class,
                CallableMessage.CallableResponse.class,
                CallableMessage.CallableResponseVoid.class,
                Message.MessageNone.class,
                ActorSystemRemote.ConnectionCloseNotice.class,
                ActorSystemRemote.TransferredMessage.class,

                ActorRefRemote.class, //those classes are not serialized, but may appear as class-info by writeClassAndObject
                Actor.class,
                ActorDefault.class,
                ActorRefLocalNamed.class,
                ActorRefLocalNamed.ActorRefLocalNamedNoName.class,
                ActorAddress.ActorAddressAnonymousActor.class,
                ActorAddress.ActorAddressError.class,

                //util
                ConfigBase.class,
                FileSplitter.FileSplit.class,
                ResponsiveCalls.DeadLetterException.class,
                StagingActor.CallConsumerI.class,
                StagingActor.CompletionHandlerForActor.class,
                StagingActor.CompletionHandlerTask.class,
                StagingActor.StagingCompleted.class,
                StagingActor.StagingHandlerCompleted.class,
                StagingActor.StagingNotification.class,
                StagingActor.StagingTask.class,
                StagingActor.StagingWatcher.class);
    }

    public interface SerializerFunction {
        Object read(Input input);
        void write(Output out, Object o);
        Object copy(Object src);
    }

    public static class SerializerPool implements SerializerFunction {
        protected Pool<Kryo> pool;
        protected ActorSystem.SystemLogger logger;

        public SerializerPool(Pool<Kryo> pool) {
            this.pool = pool;
        }

        public Object copy(Object src) {
            Kryo k = pool.obtain();
            Object o = k.copy(src);
            pool.free(k);
            return o;
        }

        @Override
        public Object read(Input input) {
            Kryo k = pool.obtain();
            try {
                Object o = k.readClassAndObject(input);
                pool.free(k);
                return o;
            } catch (Exception ex) {
                log(ex, "Kryo error: read");
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void write(Output out, Object o) {
            Kryo k = pool.obtain();
            try {
                k.writeClassAndObject(out, o);
                pool.free(k);
            } catch (Exception ex) {
                log(ex, "Kryo error: write %s", getLogger().toStringLimit(o));
                throw new RuntimeException(ex);
            }
        }

        public ActorSystem.SystemLogger getLogger() {
            if (logger == null) {
                logger = new ActorSystemDefault.SystemLoggerErr();
            }
            return logger;
        }

        protected void log(Throwable ex, String fmt, Object... args) {
            if (debugLog) {
                getLogger().log(true, debugLogColor, ex, fmt, args);
            } else {
                getLogger().log(true, debugLogColor, fmt, args);
            }
        }
    }

    public static class SerializerPoolDefault extends SerializerPool {
        protected Function<ActorSystem, Kryo> defaultBuilder;
        public SerializerPoolDefault() {
            this(null);
        }

        public SerializerPoolDefault(ActorSystem system) {
            this(system, KryoBuilder.builder());
        }

        public SerializerPoolDefault(ActorSystem system, Function<ActorSystem, Kryo> defaultBuilder) {
            super(new Pool<>(true, false, 8) {
                @Override
                protected Kryo create() {
                    if (system instanceof SerializerFactory) {
                        return ((SerializerFactory) system).createSerializer();
                    } else {
                        return defaultBuilder.apply(system); //TODO null system
                    }
                }
            });
            this.defaultBuilder = defaultBuilder;
            if (system != null) {
                logger = system.getLogger();
            }
        }
    }

    public interface SerializerFactory {
        Kryo createSerializer();
        SerializerFunction getSerializer();
    }
}
