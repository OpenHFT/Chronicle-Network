package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub.CoreFields;
import net.openhft.chronicle.engine.client.ParameterizeWireKey;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

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

    @SuppressWarnings("SameParameterValue")
    protected long proxyReturnLong(@NotNull final WireKey eventId) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, null);
        return readLong(tid, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    protected int proxyReturnInt(@NotNull final WireKey eventId) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, null);
        return readInt(tid, startTime);
    }

    public void proxyReturnWireConsumer(@NotNull final WireKey eventId,
                                        @NotNull final Consumer<WireIn> consumer) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, null);
        readWire(tid, startTime, consumer);
    }

    @SuppressWarnings("SameParameterValue")
    protected void proxyReturnVoid(@NotNull final WireKey eventId,
                                   @Nullable final Consumer<ValueOut> consumer) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, consumer);
        readVoid(tid, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    protected void proxyReturnVoid(@NotNull final WireKey eventId) {
        proxyReturnVoid(eventId, null);
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

    protected long writeHeader(long startTime) {
        return hub.writeHeader(startTime, hub.outWire(), csp, cid);
    }

    protected void checkIsData(Wire wireIn) {
        int datalen = wireIn.bytes().readVolatileInt();

        if (!Wires.isData(datalen))
            throw new IllegalStateException("expecting a data blob, from ->" + Bytes.toDebugString
                    (wireIn.bytes(), 0, wireIn.bytes().limit()));

    }

    protected void writeField(ValueOut wireOut, Object value) {
        writeField(value, wireOut);
    }

    private void writeField(Object value, ValueOut valueOut) {

        assert hub.outBytesLock().isHeldByCurrentThread();
        assert !hub.inBytesLock().isHeldByCurrentThread();


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
    protected boolean proxyReturnBoolean(
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

    private long readLong(long tid, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wire = hub.proxyReply(timeoutTime, tid);
            checkIsData(wire);
            return wire.read(CoreFields.reply).int64();
        } finally {
            hub.inBytesLock().unlock();
        }
    }

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

    private void readWire(long tid, long startTime, Consumer<WireIn> c) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wire = hub.proxyReply(timeoutTime, tid);
            checkIsData(wire);
            c.accept(wire);
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    abstract protected Consumer<ValueOut> toParameters(@NotNull final E eventId, Object... args);
}
