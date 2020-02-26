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
import com.esotericsoftware.kryo.util.Pool;
import csl.actor.*;
import csl.actor.cluster.*;
import csl.actor.keyaggregate.*;
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
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class KryoBuilder {
    protected ActorSystem system;
    protected Kryo kryo;

    public static boolean debugLog = System.getProperty("csl.actor.debugKryo", "false").equals("true");

    public KryoBuilder setKryo(Kryo kryo) {
        this.kryo = kryo;
        return this;
    }

    public KryoBuilder setSystem(ActorSystem system) {
        this.system = system;
        return this;
    }

    public static Function<ActorSystem, Kryo> builder() {
        return builder(KryoBuilder::new);
    }

    public static Function<ActorSystem, Kryo> builder(Supplier<KryoBuilder> builderCreator) {
        return (sys) -> builderCreator.get().setSystem(sys).build();
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
        kryo.setReferences(true);
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        register(kryo, getDefaultSerializerClasses());
        register(kryo, getBaseClasses());

        kryo.register(SerializedLambda.class);
        kryo.register(ClosureSerializer.Closure.class, new PatchedClosureSerializer());

        registerObjectStream(kryo, EnumMap.class);
        registerObjectStream(kryo, SimpleTimeZone.class);

        for (Class<?> cls : Collections.class.getDeclaredClasses()) {
            if (Serializable.class.isAssignableFrom(cls)) {
                registerObjectStream(kryo, (Class<Serializable>) cls);
            }
        }

        kryo.addDefaultSerializer(ActorRef.class, new ActorRefRemoteSerializer<>(system)); //for sub-types

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
                PriorityQueue.class,
                BitSet.class);
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
                ActorRef.class,
                Message.class,
                ActorAddress.ActorAddressRemote.class,
                ActorAddress.ActorAddressRemoteActor.class,
                ActorSystemDefault.DeadLetter.class,
                CallableMessage.class,
                CallableMessage.CallableFailure.class,
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
                ResponsiveCalls.ResponsiveCallableActor.class,

                ResponsiveCalls.DeadLetterException.class,
                ActorPlacement.AddressList.class,
                ActorPlacement.AddressListEntry.class,
                ActorPlacement.ActorCreationRequest.class,
                ActorPlacement.CallableMasterThreads.class,
                ActorPlacementKeyAggregation.Task.class,
                ActorPlacementKeyAggregation.TaskChain.class,
                ActorKeyAggregation.StateUnit.class,
                ActorKeyAggregation.ActorKeyAggregationSerializable.class,
                ActorKeyAggregation.CallableToLocalSerializable.class,
                KeyHistograms.HistogramTree.class,
                KeyHistograms.HistogramNodeTree.class,
                KeyHistograms.HistogramNodeLeaf.class,
                KeyHistograms.HistogramLeafList.class,
                KeyHistograms.HistogramLeafCell.class,
                ActorBehaviorKeyAggregation.HistogramNodeLeaf1.class,
                ActorBehaviorKeyAggregation.HistogramNodeLeaf2.class,
                ActorBehaviorKeyAggregation.HistogramNodeLeaf3.class,
                ActorBehaviorKeyAggregation.HistogramNodeLeaf4.class,
                ActorBehaviorKeyAggregation.HistogramNodeLeafList.class,
                ActorBehaviorKeyAggregation.HistogramNodeLeafListReducible.class,
                ActorBehaviorKeyAggregation.HistogramNodeLeafListReducibleForPhase.class,
                KeyHistograms.HistogramNodeLeafMap.class,
                KeyHistograms.HistogramLeafCellSerializedEnd.class,
                ConfigBase.class,
                csl.actor.keyaggregate.Config.class,
                MailboxPersistable.MessageOnStorage.class,
                MailboxPersistable.PersistentFileEnd.class,
                MailboxPersistable.PersistentFileReaderSource.class,
                MailboxPersistable.MessageOnStorageFile.class,
                KeyHistogramsPersistable.HistogramTreePersistable.class,
                KeyHistogramsPersistable.PutIndexHistory.class,
                KeyHistogramsPersistable.HistogramLeafListPersistable.class,
                KeyHistogramsPersistable.HistogramLeafCellOnStorageFile.class,
                KeyHistogramsPersistable.PersistentFileReaderSourceWithSize.class,
                KeyHistogramsPersistable.HistogramNodeTreeOnStorage.class,
                KeyHistogramsPersistable.HistogramNodeLeafOnStorage.class,
                KeyHistogramsPersistable.NodeTreeData.class,

                KeyAggregationVisitor.class,
                KeyAggregationVisitor.VisitorNoSender.class,
                PhaseShift.class,
                PhaseShift.PhaseCompleted.class,
                PhaseShift.PhaseShiftIntermediate.class,
                PhaseShift.PhaseShiftIntermediateType.class,
                ActorKeyAggregation.CancelChange.class,
                ActorKeyAggregation.CanceledChangeType.class,

                ClusterDeployment.ConfigSet.class,
                ConfigDeployment.class,
                FileSplitter.FileSplit.class,
                ActorPlacementKeyAggregation.Task.class,
                ActorPlacementKeyAggregation.TaskChain.class);
    }

    public interface SerializerFunction {
        Object read(Input input);
        void write(Output out, Object o);
    }

    public static class SerializerPool implements SerializerFunction {
        protected Pool<Kryo> pool;

        public SerializerPool(Pool<Kryo> pool) {
            this.pool = pool;
        }

        @Override
        public Object read(Input input) {
            Kryo k = pool.obtain();
            try {
                Object o = k.readClassAndObject(input);
                pool.free(k);
                return o;
            } catch (Exception ex) {
                System.err.println(String.format("Kryo error: %s", ex));
                if (debugLog) {
                    ex.printStackTrace();
                }
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
                System.err.println(String.format("Kryo error: %s", ex));
                if (debugLog) {
                    ex.printStackTrace();
                }
                throw new RuntimeException(ex);
            }
        }
    }

    public static class SerializerPoolDefault extends SerializerPool {
        public SerializerPoolDefault() {
            this(null);
        }

        public SerializerPoolDefault(ActorSystem system) {
            super(new Pool<Kryo>(true, false, 8) {
                @Override
                protected Kryo create() {
                    return KryoBuilder.builder().apply(system); //TODO null system
                }
            });
        }
    }
}
