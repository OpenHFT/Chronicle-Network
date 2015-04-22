package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub.CoreFields;
import net.openhft.chronicle.engine.client.ParameterizeWireKey;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by Rob Austin
 */
public abstract class AbstactStatelessClient<E extends ParameterizeWireKey> {

    protected final ClientWiredStatelessTcpConnectionHub hub;
    private final long cid;
    protected final String channelName;
    protected String csp;

    static final WriteMarshallable EMPTY = wire -> {
        // nothing
    };


    /**
     * @param channelName
     * @param hub
     * @param type        the type of wire handler for example "MAP" or "QUEUE"
     * @param cid         used by proxies such as the entry-set
     */
    public AbstactStatelessClient(@NotNull final String channelName,
                                  @NotNull final ClientWiredStatelessTcpConnectionHub hub,
                                  @NotNull final String type,
                                  long cid) {
        this.cid = cid;
        this.csp = "//" + channelName + "#" + type;
        this.hub = hub;
        this.channelName = channelName;

    }

    static <E> E readObject(@NotNull final WireKey argName,
                            @NotNull final Wire wireIn,
                            @Nullable final E usingValue,
                            @NotNull final Class<E> clazz) {

        final ValueIn valueIn = wireIn.read(argName);
        if (valueIn.isNull())
            return null;

        if (StringBuilder.class.isAssignableFrom(clazz)) {
            valueIn.text((StringBuilder) usingValue);
            return usingValue;
        } else if (Marshallable.class.isAssignableFrom(clazz)) {


            final E v;
            if (usingValue == null)
                try {
                    v = clazz.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            else
                v = usingValue;


            valueIn.marshallable((Marshallable) v);
            return v;

        } else if (String.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) valueIn.text();

        } else if (Long.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) (Long) valueIn.int64();
        } else if (Double.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) (Double) valueIn.float64();

        } else if (Integer.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) (Integer) valueIn.int32();

        } else if (Float.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) (Float) valueIn.float32();

        } else if (Short.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) (Short) valueIn.int16();

        } else if (Character.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            final String text = valueIn.text();
            if (text == null || text.length() == 0)
                return null;
            return (E) (Character) text.charAt(0);

        } else if (Byte.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return (E) (Byte) valueIn.int8();


        } else {
            throw new IllegalStateException("unsupported type");
        }
    }

    @SuppressWarnings("SameParameterValue")
    protected long proxyReturnLong(@NotNull final WireKey eventId) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, null);
        return readLong(tid, startTime, CoreFields.reply);
    }

    @SuppressWarnings("SameParameterValue")
    protected int proxyReturnInt(@NotNull final WireKey eventId) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, null);
        return readInt(tid, startTime);
    }

    public <T>T proxyReturnWireConsumer(@NotNull final WireKey eventId,
                                        @NotNull final Function<WireIn, T> consumer) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, null);
        return readWire(tid, startTime, consumer);
    }


    public <T>T proxyReturnWireConsumerInOut(@NotNull final WireKey eventId,
                                        @Nullable final Consumer<ValueOut> consumerOut,
                                        @NotNull final Function<WireIn, T> consumerIn) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, consumerOut);
        return readWire(tid, startTime, consumerIn);
    }

    @SuppressWarnings("SameParameterValue")
    protected void proxyReturnVoid(@NotNull final WireKey eventId,
                                   @Nullable final Consumer<ValueOut> consumer) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, consumer);
        readVoid(tid, startTime);
    }

    protected long proxyBytesReturnLong(@NotNull final WireKey eventId,
                                        @Nullable final Bytes bytes, WireKey reply) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEventBytes(startTime, eventId, bytes);
        return readLong(tid, startTime, reply);
    }

    @SuppressWarnings("SameParameterValue")
    protected void proxyReturnVoid(@NotNull final WireKey eventId) {
        proxyReturnVoid(eventId, null);
    }

//    @SuppressWarnings("SameParameterValue")
//    protected Marshallable proxyReturnMarshallable(@NotNull final WireKey eventId,
//                                                   @Nullable final Consumer<ValueOut> consumer) {
//        final long startTime = System.currentTimeMillis();
//        long tid = sendEvent(startTime, eventId, consumer);
//        return proxyReturnWireConsumer(tid, startTime);
//    }

    @SuppressWarnings("SameParameterValue")
    protected Marshallable proxyReturnMarshallable(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumerInOut(eventId,null, wireIn -> wireIn.read(()->"reply").typedMarshallable());
    }

    protected long sendEvent(final long startTime,
                             @NotNull final WireKey eventId,
                             @Nullable final Consumer<ValueOut> consumer) {
        long tid;
        hub.outBytesLock().lock();
        try {

            tid = writeHeader(startTime);
            hub.outWire().writeDocument(false, wireOut -> {

                final ValueOut valueOut = wireOut.writeEventName(eventId);

                if (consumer == null)
                    valueOut.marshallable(EMPTY);
                else
                    consumer.accept(valueOut);

            });

            hub.writeSocket(hub.outWire());

        } finally {
            hub.outBytesLock().unlock();
        }
        return tid;
    }


    protected long sendEventBytes(final long startTime,
                                  @NotNull final WireKey eventId,
                                  @Nullable final Bytes c) {
        long tid;
        hub.outBytesLock().lock();
        try {

            tid = writeHeader(startTime);


            hub.outWire().writeDocument(false, wireOut -> {

                final ValueOut valueOut = wireOut.writeEventName(eventId);
                wireOut.bytes().write(c);
            });


            hub.writeSocket(hub.outWire());

        } finally {
            hub.outBytesLock().unlock();
        }
        return tid;
    }

    protected long writeHeader(long startTime) {
        return hub.writeHeader(startTime, hub.outWire(), csp, cid);
    }

    protected void checkIsData(Wire wireIn) {
        int datalen = wireIn.bytes().readVolatileInt();

        if (!Wires.isData(datalen))
            throw new IllegalStateException("expecting a data blob, from ->" + Bytes.toDebugString
                    (wireIn.bytes(), 0, wireIn.bytes().limit()));

    }

    protected static void writeField(ValueOut wireOut, Object value) {
        writeField(value, wireOut);
    }

     static void writeField(Object value, ValueOut valueOut) {

        if (value instanceof Byte)
            valueOut.int8((Byte) value);
        else if (value instanceof Character)
            valueOut.text(value.toString());
        else if (value instanceof Short)
            valueOut.int16((Short) value);
        else if (value instanceof Integer)
            valueOut.int32((Integer) value);
        else if (value instanceof Long)
            valueOut.int64((Long) value);
        else if (value instanceof CharSequence) {
            valueOut.text((CharSequence) value);
        } else if (value instanceof Marshallable) {
            valueOut.marshallable((Marshallable) value);
        } else {
            throw new IllegalStateException("type=" + value.getClass() +
                    " is unsupported, it must either be of type Marshallable or CharSequence");
        }
    }

    protected boolean readBoolean(long tid, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wireIn = hub.proxyReply(timeoutTime, tid);
            checkIsData(wireIn);
            return wireIn.read(CoreFields.reply).bool();
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    @SuppressWarnings("SameParameterValue")
    protected boolean proxyReturnBooleanArgs(
            @NotNull final E eventId, Object... args) {
        final long startTime = System.currentTimeMillis();

        final long tid = sendEvent(startTime, eventId, toParameters(eventId, args));
        return readBoolean(tid, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    protected boolean proxyReturnBoolean(@NotNull final WireKey eventId) {
        final long startTime = System.currentTimeMillis();


        final long tid = sendEvent(startTime, eventId, null);
        return readBoolean(tid, startTime);

    }

    protected void readVoid(long tid, long startTime) {
        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            hub.proxyReply(timeoutTime, tid);
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    private long readLong(long tid, long startTime, WireKey replyId) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wire = hub.proxyReply(timeoutTime, tid);
            checkIsData(wire);
            return wire.read(replyId).int64();
        } finally {
            hub.inBytesLock().unlock();
        }
    }

//    private Marshallable readMarshallable(long tid, long startTime) {
//        assert !hub.outBytesLock().isHeldByCurrentThread();
//        final long timeoutTime = startTime + hub.timeoutMs;
//
//        // receive
//        hub.inBytesLock().lock();
//        try {
//            final Wire wire = hub.proxyReply(timeoutTime, tid);
//            checkIsData(wire);
//            return wire.read(CoreFields.reply).typedMarshallable();
//        } finally {
//            hub.inBytesLock().unlock();
//        }
//    }


    private int readInt(long tid, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wire = hub.proxyReply(timeoutTime, tid);
            checkIsData(wire);
            return wire.read(CoreFields.reply).int32();
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    private <T> T readWire(long tid, long startTime, Function<WireIn, T> c) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wire = hub.proxyReply(timeoutTime, tid);
            checkIsData(wire);
            return c.apply(wire);
        } finally {
            hub.inBytesLock().unlock();
        }
    }


    public static <E extends ParameterizeWireKey> Consumer<ValueOut> toParameters
            (@NotNull E eventId,
             @Nullable Object... args) {

        return out -> {
            final WireKey[] paramNames = eventId.params();

            if (paramNames.length == 1) {
                writeField(out, args[0]);
                return;
            }

            assert args.length == paramNames.length :
                    "methodName=" + eventId +
                            ", args.length=" + args.length +
                            ", paramNames.length=" + paramNames.length;

            out.marshallable(m -> {

                for (int i = 0; i < paramNames.length; i++) {
                    final ValueOut vo = m.write(paramNames[i]);
                    writeField(vo, args[i]);
                }

            });

        };
    }
}
