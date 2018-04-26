/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ConnectionDroppedException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static net.openhft.chronicle.network.connection.CoreFields.reply;

/*
 * Created by Rob Austin
 */
public abstract class AbstractStatelessClient<E extends ParameterizeWireKey> implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractStatelessClient.class);

    @NotNull
    protected final TcpChannelHub hub;
    @NotNull
    protected final String csp;
    private final long cid;

    /**
     * @param hub for this connection
     * @param cid used by proxies such as the entry-set
     * @param csp the uri of the request
     */
    protected AbstractStatelessClient(@NotNull final TcpChannelHub hub,
                                      long cid,
                                      @NotNull final String csp) {
        this.cid = cid;
        this.csp = csp;
        this.hub = hub;
    }

    protected static <E extends ParameterizeWireKey>
    WriteValue toParameters(@NotNull final E eventId,
                            @Nullable final Object... args) {
        return out -> {
            @NotNull final WireKey[] paramNames = eventId.params();

            //args can be null, e.g. when get() is called from Reference.
            if (args == null) return;

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
                    @NotNull final ValueOut vo = m.write(paramNames[i]);
                    vo.object(args[i]);
                }
            });
        };
    }

    @Nullable
    protected <R> R proxyReturnWireTypedObject(
            @NotNull final E eventId,
            @Nullable R usingValue,
            @NotNull final Class<R> resultType,
            @NotNull Object... args) {

        @Nullable Function<ValueIn, R> consumerIn = resultType == CharSequence.class && usingValue != null
                ? f -> {
            f.textTo((StringBuilder) usingValue);
            return usingValue;
        }
                : f -> f.object(resultType);
        return proxyReturnWireConsumerInOut(eventId,
                CoreFields.reply,
                toParameters(eventId, args),
                consumerIn);
    }

    @Nullable
    protected <R> R proxyReturnTypedObject(
            @NotNull final E eventId,
            @Nullable R usingValue,
            @NotNull final Class<R> resultType,
            @NotNull Object... args) {

        @NotNull Function<ValueIn, R> consumerIn = resultType == CharSequence.class && usingValue != null
                ? f -> {
            f.textTo((StringBuilder) usingValue);
            return usingValue;
        }
                : f -> f.object(resultType);
        return proxyReturnWireConsumerInOut(eventId,
                CoreFields.reply,
                toParameters(eventId, args),
                consumerIn);
    }

    @Nullable
    protected <R> R proxyReturnTypedObject(
            @NotNull final E eventId,
            @Nullable R usingValue,
            @NotNull final Class<R> resultType) {

        @NotNull Function<ValueIn, R> consumerIn =
                resultType == CharSequence.class && usingValue != null ? f -> {
                    f.textTo((StringBuilder) usingValue);
                    return usingValue;
                }
                        : f -> f.object(resultType);
        return proxyReturnWireConsumerInOut(eventId,
                CoreFields.reply,
                x -> {
                },
                consumerIn);
    }

    /**
     * this method will re attempt a number of times until successful,if connection is dropped to
     * the  remote server the TcpChannelHub may ( if configured )  automatically failover to another
     * host.
     *
     * @param s   the supply
     * @param <T> the type of supply
     * @return the result for s.get()
     */
    protected <T> T attempt(@NotNull final ThrowingSupplier<T, TimeoutException> s) {

        @Nullable ConnectionDroppedException t = null;
        @Nullable TimeoutException te = null;
        for (int i = 1; i <= 20; i++) {

            try {
                return s.get();
            } catch (ConnectionDroppedException e) {
                t = e;
            } catch (TimeoutException e) {
                te = e;
            }

            // pause then resend the request

            // do NOT make this value too small, we have to give time of the connection to be
            // re-established
            Jvm.pause(i * 25);
        }

        if (t != null)
            throw t;
        throw new ConnectionDroppedException(te);
    }

    @SuppressWarnings("SameParameterValue")
    protected long proxyReturnLong(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, ValueIn::int64);
    }

    @SuppressWarnings("SameParameterValue")
    protected int proxyReturnInt(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, ValueIn::int32);
    }

    protected int proxyReturnInt(@NotNull final E eventId, @NotNull Object... args) {
        return proxyReturnWireConsumerInOut(eventId,
                CoreFields.reply,
                toParameters(eventId, args), ValueIn::int32);
    }

    protected byte proxyReturnByte(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, ValueIn::int8);
    }

    protected byte proxyReturnByte(@NotNull WireKey reply, @NotNull final WireKey eventId) {
        return proxyReturnWireConsumerInOut(eventId, reply, null, ValueIn::int8);
    }

    protected int proxyReturnUint16(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, ValueIn::uint16);
    }

    protected <T> T proxyReturnWireConsumer(@NotNull final WireKey eventId,
                                            @NotNull final Function<ValueIn, T> consumer) {
        final long startTime = Time.currentTimeMillis();
        return attempt(() -> readWire(sendEvent(startTime, eventId, null), startTime, CoreFields
                .reply, consumer));
    }

    protected <T> T proxyReturnWireConsumerInOut(@NotNull final WireKey eventId,
                                                 @NotNull final WireKey reply,
                                                 @Nullable final WriteValue consumerOut,
                                                 @NotNull final Function<ValueIn, T> consumerIn) {
        final long startTime = Time.currentTimeMillis();
        return attempt(() -> readWire(sendEvent(startTime, eventId, consumerOut), startTime,
                reply, consumerIn));
    }

    @SuppressWarnings("SameParameterValue")
    protected void proxyReturnVoid(@NotNull final WireKey eventId,
                                   @Nullable final WriteValue consumer) {
        final long startTime = Time.currentTimeMillis();

        attempt(() -> readWire(sendEvent(startTime, eventId, consumer), startTime, CoreFields
                .reply, v -> v.marshallable(ReadMarshallable.DISCARD)));
    }

    @SuppressWarnings("SameParameterValue")
    protected void proxyReturnVoid(@NotNull final WireKey eventId) {
        proxyReturnVoid(eventId, null);
    }

    protected long sendEvent(final long startTime,
                             @NotNull final WireKey eventId,
                             @Nullable final WriteValue consumer) {
        long tid;
        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        try {
            final boolean success = hub.outBytesLock().tryLock(10, TimeUnit.SECONDS);
            if (!success)
                throw new IORuntimeException("failed to obtain write lock");
        } catch (InterruptedException e) {
            throw new IORuntimeException(e);
        }

        try {

            tid = writeMetaDataStartTime(startTime);

            hub.outWire().writeDocument(false, wireOut -> {

                @NotNull final ValueOut valueOut = wireOut.writeEventName(eventId);

                if (consumer == null)
                    valueOut.marshallable(WriteMarshallable.EMPTY);
                else
                    consumer.writeValue(valueOut);
            });

            hub.writeSocket(hub.outWire(), true);
        } finally {
            hub.outBytesLock().unlock();
        }
        return tid;
    }

    /**
     * @param eventId              the wire event id
     * @param consumer             a function consume the wire
     * @param reattemptUponFailure if false - will only be sent if the connection is valid
     */
    protected boolean sendEventAsync(@NotNull final WireKey eventId,
                                     @Nullable final WriteValue consumer,
                                     boolean reattemptUponFailure) {

        if (!reattemptUponFailure && !hub.isOpen())
            return false;

        if (!reattemptUponFailure) {
            hub.lock(() -> quietSendEventAsyncWithoutLock(eventId, consumer));
            return true;
        }

        attempt(() -> {
            hub.lock2(() -> quietSendEventAsyncWithoutLock(eventId, consumer), true, TryLock.LOCK);
            return true;
        });

        return false;
    }

    /**
     * @param bytes                the bytes to send
     * @param reattemptUponFailure if false - will only be sent if the connection is valid
     */
    protected boolean sendBytes(@NotNull final Bytes bytes,
                                boolean reattemptUponFailure) {

        if (reattemptUponFailure)
            hub.lock(hub::checkConnection);
        else if (!hub.isOpen())
            return false;

        if (!reattemptUponFailure) {
            hub.lock(() -> quietSendBytesAsyncWithoutLock(bytes));
            return true;
        }

        attempt(() -> {
            hub.lock(() -> quietSendBytesAsyncWithoutLock(bytes));
            return true;
        });

        return false;
    }

    private void quietSendEventAsyncWithoutLock(@NotNull final WireKey eventId, final WriteValue consumer) {
        try {
            sendEventAsyncWithoutLock(eventId, consumer);
        } catch (ConnectionDroppedException e) {
            if (LOG.isDebugEnabled())
                Jvm.warn().on(getClass(), "", e);
            else
                LOG.info(e.toString());
        } catch (IORuntimeException e) {
            // this can occur if the socket is not currently connected
            LOG.trace("socket is not currently connected.", e);
        }
    }

    private void quietSendBytesAsyncWithoutLock(@NotNull final Bytes bytes) {
        try {
            sendBytesAsyncWithoutLock(bytes);

        } catch (ConnectionDroppedException e) {
            if (Jvm.isDebug())
                Jvm.debug().on(getClass(), e);

        } catch (IORuntimeException e) {
            // this can occur if the socket is not currently connected
            Jvm.debug().on(getClass(), "socket is not currently connected.", e);
        }
    }

    private void sendBytesAsyncWithoutLock(@NotNull final Bytes bytes) {
        writeAsyncMetaData();
        hub.outWire().bytes().write(bytes);
        hub.writeSocket(hub.outWire(), true);
    }

    private void sendEventAsyncWithoutLock(@NotNull final WireKey eventId,
                                           @Nullable final WriteValue consumer) {

        writeAsyncMetaData();
        hub.outWire().writeDocument(false, wireOut -> {
            @NotNull final ValueOut valueOut = wireOut.writeEventName(eventId);
            if (consumer == null)
                valueOut.marshallable(WriteMarshallable.EMPTY);
            else
                consumer.writeValue(valueOut);
        });

        hub.writeSocket(hub.outWire(), true);
    }

    /**
     * @param startTime the start time of this transaction
     * @return the translation id ( which is sent to the server )
     */
    private long writeMetaDataStartTime(long startTime) {
        return hub.writeMetaDataStartTime(startTime, hub.outWire(), csp, cid);
    }

    /**
     * Useful for when you know the tid
     *
     * @param tid the tid transaction
     */
    protected void writeMetaDataForKnownTID(long tid) {
        hub.writeMetaDataForKnownTID(tid, hub.outWire(), csp, cid);
    }

    /**
     * if async meta data is written, no response will be returned from the server
     */
    private void writeAsyncMetaData() {
        hub.writeAsyncHeader(hub.outWire(), csp, cid);
    }

    private void checkIsData(@NotNull Wire wireIn) {
        @NotNull Bytes<?> bytes = wireIn.bytes();
        int dataLen = bytes.readVolatileInt();

        if (!Wires.isData(dataLen))
            throw new IllegalStateException("expecting a data blob, from ->" + Bytes.toString
                    (bytes, 0, bytes.readLimit()));
    }

    protected boolean readBoolean(long tid, long startTime) throws ConnectionDroppedException, TimeoutException {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        final Wire wireIn = hub.proxyReply(timeoutTime, tid);
        checkIsData(wireIn);

        return readReply(wireIn, CoreFields.reply, ValueIn::bool);

    }

    private long readLong(long tid, long startTime) throws ConnectionDroppedException, TimeoutException {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        final Wire wireIn = hub.proxyReply(timeoutTime, tid);
        checkIsData(wireIn);

        return readReply(wireIn, CoreFields.reply, ValueIn::int64);
    }

    private <R> R readReply(@NotNull WireIn wireIn, @NotNull WireKey replyId, @NotNull Function<ValueIn, R> function) {

        final StringBuilder eventName = Wires.acquireStringBuilder();
        @NotNull final ValueIn event = wireIn.read(eventName);

        if (replyId.contentEquals(eventName))
            return function.apply(event);

        if (CoreFields.exception.contentEquals(eventName)) {
            throw Jvm.rethrow(event.throwable(true));
        }

        throw new UnsupportedOperationException("unknown event=" + eventName);
    }

    @SuppressWarnings("SameParameterValue")
    protected boolean proxyReturnBooleanWithArgs(
            @NotNull final E eventId,
            @NotNull final Object... args) {
        final long startTime = Time.currentTimeMillis();
        return attempt(() -> readBoolean(
                sendEvent(startTime, eventId, toParameters(eventId, args)),
                startTime));
    }

    @SuppressWarnings("SameParameterValue")
    protected long proxyReturnLongWithArgs(
            @NotNull final E eventId,
            @NotNull final Object... args) {
        final long startTime = Time.currentTimeMillis();
        return attempt(() -> readLong(
                sendEvent(startTime, eventId, toParameters(eventId, args)),
                startTime));
    }

    protected boolean proxyReturnBooleanWithSequence(
            @NotNull final E eventId,
            @NotNull final Collection sequence) {
        final long startTime = Time.currentTimeMillis();
        return attempt(() -> readBoolean(sendEvent(startTime, eventId, out ->
                sequence.forEach(out::object)), startTime));
    }

    @SuppressWarnings("SameParameterValue")
    protected boolean proxyReturnBoolean(@NotNull final WireKey eventId) {
        final long startTime = Time.currentTimeMillis();
        return attempt(() -> readBoolean(sendEvent(startTime, eventId, null), startTime));
    }

    private <T> T readWire(long tid, long startTime,
                           @NotNull WireKey reply,
                           @NotNull Function<ValueIn, T> c) throws ConnectionDroppedException, TimeoutException {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        final Wire wire = hub.proxyReply(timeoutTime, tid);
        checkIsData(wire);
        return readReply(wire, reply, c);

    }

    protected int readInt(long tid, long startTime) throws ConnectionDroppedException, TimeoutException {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        final Wire wireIn = hub.proxyReply(timeoutTime, tid);
        checkIsData(wireIn);
        return wireIn.read(reply).int32();

    }

    @Override
    public void close() {
        hub.close();
    }
}
