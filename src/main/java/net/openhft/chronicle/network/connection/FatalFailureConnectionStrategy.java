package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.network.ConnectionStrategy;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.NetworkStatsListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Long.getLong;
import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.network.connection.TcpChannelHub.TCP_BUFFER;

/**
 * @author Rob Austin.
 *         The main functionality would be exactly same as C#:
 *         -       on any disconnect event, Chronicle Client would go through the list of hosts to connect for a
 *                   pre-configured number of times (default connection retries with each host = 3).
 *         -       A fatal failure event is raised if the connection is not established during any of these attempts.
 *         -       If connection is successful with any hosts in the list, then fatal failure is not raised,
 *                   and connection retries is set to zero (so that any future disconnection event then starts its own
 *                   “3 retries for each host”).
 *         <p>
 *         Couple of examples are given below. The assumption is that there are three servers MAIN, DR1 and DR2. The
 *         1.     Currently the client is connected with DR1. A disconnect with DR1 occurs, and then the client tries
 *                   connecting to DR2, MAIN and DR1. It attempts this for a preconfigured number of times (e.g. 3 times).
 *                   So for example, the following events occur:
 *         --a.     Connection attempt no 1 with  DR2:  failed
 *         --b.    Connection attempt no 1 with MAIN:  failed
 *         --c.     Connection attempt no 1 with DR1:  failed
 *         --d.    Connection attempt no 2 with DR2:  succeeded. No fatal failure event is raised.
 *         2.     Now the client is on DR2 and a disconnect with DR2 occurs. Following events occur
 *         --a.     Connection attempt no 1 with MAIN:  failed
 *         --b.    Connection attempt no 1 with DR1:  failed
 *         --c.     Connection attempt no 1 with DR2:  failed
 *         --d.    Connection attempt no 2 with MAIN:  failed
 *         --e.     Connection attempt no 2 with DR1:  failed
 *         --f.     Connection attempt no 2 with DR2:  failed
 *         --g.    Connection attempt no 3 with MAIN:  failed
 *         --h.     Connection attempt no 3 with DR1:  failed
 *         --i.      Connection attempt no 3 with DR2:  failed   =>  Attempt 3 finished. Fatal Failure is raised
 */
public class FatalFailureConnectionStrategy implements ConnectionStrategy {
    private static final long PAUSE_MS = getLong("FatalFailureConnectionStrategy.pauseMS", 300);
    private static final long PAUSE = TimeUnit.MILLISECONDS.toNanos(PAUSE_MS);
    private static final long ATTEMPT_INTERVAL_TICKS = getLong("FatalFailureConnectionStrategy.attemptIntervalMS", 50);

    private final int attempts;
    private int tcpBufferSize = Integer.getInteger("tcp.client.buffer.size", TCP_BUFFER);
    private boolean hasSentFatalFailure;

    /**
     * @param attempts the number of attempts before a onFatalFailure() reported
     */
    public FatalFailureConnectionStrategy(int attempts) {
        this.attempts = attempts;
    }

    @Nullable
    @Override
    public SocketChannel connect(@NotNull String name,
                                 @NotNull SocketAddressSupplier socketAddressSupplier,
                                 @Nullable NetworkStatsListener<? extends NetworkContext> networkStatsListener,
                                 boolean didLogIn,
                                 @Nullable FatalFailureMonitor fatalFailureMonitor) throws InterruptedException {

        if (socketAddressSupplier.size() == 0 && !hasSentFatalFailure && fatalFailureMonitor != null) {
            hasSentFatalFailure = true;
            fatalFailureMonitor.onFatalFailure(name, "no connections have not been configured");
            LockSupport.parkNanos(PAUSE);
            return null;
        }

        int failures = 0;
        int maxFailures = socketAddressSupplier.size() * attempts;
        long millis = TimeUnit.NANOSECONDS.toMillis(PAUSE);
        socketAddressSupplier.resetToPrimary();

        for (; ; ) {

            if (Thread.currentThread().isInterrupted())
                throw new InterruptedException();

            if (failures == maxFailures && fatalFailureMonitor != null) {
                if (!hasSentFatalFailure) {
                    hasSentFatalFailure = true;
                    fatalFailureMonitor.onFatalFailure(name, name);
                }

                return null;
            }

            SocketChannel socketChannel = null;
            try {
                @Nullable final InetSocketAddress socketAddress = socketAddressSupplier.get();
                if (socketAddress == null) {
                    failures++;
                    socketAddressSupplier.failoverToNextAddress();
                    LockSupport.parkNanos(PAUSE);
                    continue;
                }

                socketChannel = openSocketChannel(socketAddress, tcpBufferSize, millis);

                if (socketChannel == null) {
                    Jvm.warn().on(getClass(), "unable to connected to " + socketAddressSupplier.toString() + ", name=" + name);
                    failures++;
                    socketAddressSupplier.failoverToNextAddress();
                    LockSupport.parkNanos(PAUSE);
                    continue;
                }

                Jvm.debug().on(getClass(), "successfully connected to " + socketAddressSupplier.toString());

                if (networkStatsListener != null)
                    networkStatsListener.onHostPort(socketAddress.getHostString(), socketAddress.getPort());

                hasSentFatalFailure = false;
                failures = 0;

                // success
                return socketChannel;

            } catch (InterruptedException e) {
                throw e;
            } catch (Throwable e) {

                //noinspection ConstantConditions
                if (socketChannel != null)
                    closeQuietly(socketChannel);

                failures++;
                socketAddressSupplier.failoverToNextAddress();
                LockSupport.parkNanos(PAUSE);
            }

        }

    }

    /**
     * the reason for this method is that unlike the selector it uses tick time
     */
    @Nullable
    public SocketChannel openSocketChannel(@NotNull InetSocketAddress socketAddress,
                                            int tcpBufferSize,
                                            long timeoutMs) throws IOException, InterruptedException
    {
        assert timeoutMs > 0;
        long attemptPeriodEnd = Time.tickTime() + timeoutMs;
        long nextAttemptTick = Time.tickTime() + ATTEMPT_INTERVAL_TICKS;

        do {
            SocketChannel sc = ConnectionStrategy.socketChannel(socketAddress, tcpBufferSize);

            if (sc != null)
                return sc;

            do {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                Thread.yield();
            } while(Time.tickTime() < nextAttemptTick);

            nextAttemptTick += ATTEMPT_INTERVAL_TICKS;
        } while(Time.tickTime() < attemptPeriodEnd);

        Jvm.warn().on(ConnectionStrategy.class, "Timed out attempting to connect to " + socketAddress);
        return null;
    }

}
