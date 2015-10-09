package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Constructor;

/**
 * @author Rob Austin.
 */
public interface WireOutPublisher extends Closeable {
    void applyAction(@NotNull WireOut out, @NotNull Runnable runnable);

    /**
     * @param key   is NULL if the throttling is not required
     * @param event the event to send
     */
    void put(@Nullable final Object key, WriteMarshallable event);

    boolean isClosed();

    /**
     * a static factory that creates and instance in chronicle enterpise
     *
     * @param periodMs
     * @param delegate
     * @return
     */
    static WireOutPublisher newThrottledWireOutPublisher(int periodMs,
                                                         @NotNull WireOutPublisher delegate) {

        try {
            final Class<?> aClass = Class.forName("software.chronicle.enterprise.throttle.ThrottledWireOutPublisher");
            final Constructor<WireOutPublisher> constructor = (Constructor) aClass.getConstructors()[0];
            return constructor.newInstance(periodMs, delegate);
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }

    }
}
