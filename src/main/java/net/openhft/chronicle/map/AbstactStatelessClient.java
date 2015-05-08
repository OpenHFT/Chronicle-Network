package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.wire.*;
import net.openhft.chronicle.wire.util.ExceptionMarshaller;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
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
    private final ExceptionMarshaller exceptionMarshaller = new ExceptionMarshaller();


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


    @SuppressWarnings("SameParameterValue")
    protected long proxyReturnLong(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, f -> f.int64());
    }

    @SuppressWarnings("SameParameterValue")
    protected int proxyReturnInt(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, f -> f.int32());
    }

    protected int proxyReturnUint16(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, f -> f.uint16());
    }


    public <T> T proxyReturnWireConsumer(@NotNull final WireKey eventId,
                                         @NotNull final Function<ValueIn, T> consumer) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, null);
        return readWire(tid, startTime, CoreFields.reply, consumer);
    }


    public <T> T proxyReturnWireConsumerInOut(@NotNull final WireKey eventId,
                                              CoreFields reply, @Nullable final Consumer<ValueOut> consumerOut,
                                              @NotNull final Function<ValueIn, T> consumerIn) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, consumerOut);
        return readWire(tid, startTime, reply, consumerIn);
    }

    @SuppressWarnings("SameParameterValue")
    protected void proxyReturnVoid(@NotNull final WireKey eventId,
                                   @Nullable final Consumer<ValueOut> consumer) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, consumer);
        readWire(tid, startTime, CoreFields.reply, v -> v.marshallable(wireIn -> {
        }));
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

    @SuppressWarnings("SameParameterValue")
    protected Marshallable proxyReturnMarshallable(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumerInOut(eventId, CoreFields.reply, null, ValueIn::typedMarshallable);
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
                    valueOut.marshallable(WireOut.EMPTY);
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
                wireOut.writeEventName(eventId);
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

    StringBuilder eventName = new StringBuilder();

    protected boolean readBoolean(long tid, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wireIn = hub.proxyReply(timeoutTime, tid);
            checkIsData(wireIn);

            return readReply(wireIn, CoreFields.reply, v -> v.bool());

        } finally {
            hub.inBytesLock().unlock();
        }
    }


    <R> R readReply(WireIn wireIn, WireKey replyId, Function<ValueIn, R> function) {
        final ValueIn event = wireIn.read(eventName);

        if (replyId.contentEquals(eventName))
            return function.apply(event);

        if (CoreFields.exception.contentEquals(eventName))
            exceptionMarshaller.readMarshallable(wireIn);

        throw new UnsupportedOperationException("unknown event=" + eventName);
    }


    @SuppressWarnings("SameParameterValue")
    protected boolean proxyReturnBooleanWithArgs(
            @NotNull final E eventId,
            @NotNull final Object... args) {
        final long startTime = System.currentTimeMillis();

        final long tid = sendEvent(startTime, eventId, toParameters(eventId, args));
        return readBoolean(tid, startTime);
    }

    protected boolean proxyReturnBooleanWithSequence(
            @NotNull final E eventId,
            @NotNull final Collection sequence) {
        final long startTime = System.currentTimeMillis();

        final long tid = sendEvent(startTime, eventId, out -> sequence.forEach(out::object));
        return readBoolean(tid, startTime);
    }


    @SuppressWarnings("SameParameterValue")
    protected boolean proxyReturnBoolean(@NotNull final WireKey eventId) {
        final long startTime = System.currentTimeMillis();
        final long tid = sendEvent(startTime, eventId, null);
        return readBoolean(tid, startTime);
    }

    protected void readVoid(long tid, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wire = hub.proxyReply(timeoutTime, tid);
            checkIsData(wire);
            readReply(wire, CoreFields.reply, valueIn -> null);
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
            return readReply(wire, replyId, ValueIn::int64);
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    /*private int readInt(long tid, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wire = hub.proxyReply(timeoutTime, tid);
            checkIsData(wire);
            return readReply(wire, CoreFields.reply, ValueIn::int32);

        } finally {
            hub.inBytesLock().unlock();
        }
    }
*/
    private <T> T readWire(long tid, long startTime, WireKey reply, Function<ValueIn, T> c) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wire = hub.proxyReply(timeoutTime, tid);
            checkIsData(wire);
            return readReply(wire, reply, c);
        } finally {
            hub.inBytesLock().unlock();
        }
    }


    public static <E extends ParameterizeWireKey>
    Consumer<ValueOut> toParameters(@NotNull final E eventId,
                                    @Nullable final Object... args) {

        return out -> {
            final WireKey[] paramNames = eventId.params();

            assert args != null;
            assert args.length == paramNames.length :
                    "methodName=" + eventId +
                            ", args.length=" + args.length +
                            ", paramNames.length=" + paramNames.length;

            if (paramNames.length == 1) {
                out.object(args[0]);
                return;
            }


            out.marshallable(m -> {

                for (int i = 0; i < paramNames.length; i++) {
                    final ValueOut vo = m.write(paramNames[i]);
                    vo.object(args[i]);
                }

            });

        };
    }
}
