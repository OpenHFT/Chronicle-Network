package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * @author Rob Austin.
 */
public interface WireOutPublisher extends Closeable {
    Logger LOG = LoggerFactory.getLogger(WireOutPublisher.class);

    void applyAction(@NotNull WireOut out, @NotNull Runnable runnable);

    /**
     * @param key   the key to the event, only used when throttling, otherwise NULL if the
     *              throttling is not required
     * @param event the marshallable event
     */
    void put(@Nullable final Object key, WriteMarshallable event);

    boolean isClosed();

    /**
     * a static factory that creates and instance in chronicle enterprise
     *
     * @param periodMs the period between updates of the same key
     * @param delegate the  WireOutPublisher the events will get delegated to
     * @return a throttled WireOutPublisher
     */
    static WireOutPublisher newThrottledWireOutPublisher(int periodMs,
                                                         @NotNull WireOutPublisher delegate) {

        try {
            final Class<?> aClass = Class.forName("software.chronicle.enterprise.throttle.ThrottledWireOutPublisher");
            final Constructor<WireOutPublisher> constructor = (Constructor) aClass.getConstructors()[0];
            return constructor.newInstance(periodMs, delegate);
        } catch (Exception e) {
            LOG.warn("To use this feature please install Chronicle-Enterprise");
            throw Jvm.rethrow(e);
        }

    }
}
