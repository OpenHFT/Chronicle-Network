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
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * @author Rob Austin.
 */
@FunctionalInterface
public interface WireOutPublisher extends Closeable {
    Logger LOG = LoggerFactory.getLogger(WireOutPublisher.class);

    /**
     * a static factory that creates and instance in chronicle enterprise
     *
     * @param periodMs the period between updates of the same key
     * @param delegate the  WireOutPublisher the events will get delegated to
     * @return a throttled WireOutPublisher
     */
    static WireOutPublisher newThrottledWireOutPublisher(int periodMs, @NotNull WireOutPublisher delegate) {

        try {
            final Class<?> aClass = Class.forName("software.chronicle.enterprise.throttle.ThrottledWireOutPublisher");
            @NotNull final Constructor<WireOutPublisher> constructor = (Constructor) aClass.getConstructors()[0];
            return constructor.newInstance(periodMs, delegate);

        } catch (Exception e) {
            Jvm.warn().on(WireOutPublisher.class, "To use this feature please install Chronicle-Engine-Enterprise");
            throw Jvm.rethrow(e);
        }
    }

    default void applyAction(@NotNull Bytes out) {
        throw new UnsupportedOperationException();
    }

    default void applyAction(@NotNull WireOut out) {
        applyAction(out.bytes());
    }

    default void addWireConsumer(WireOutConsumer wireOutConsumer) {
        throw new UnsupportedOperationException();
    }

    default boolean removeBytesConsumer(WireOutConsumer wireOutConsumer) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param key   the key to the event, only used when throttling, otherwise NULL if the
     *              throttling is not required
     * @param event the marshallable event
     */
    void put(@Nullable final Object key, WriteMarshallable event);

    default boolean isClosed() {
        throw new UnsupportedOperationException();
    }

    default boolean canTakeMoreData() {
        throw new UnsupportedOperationException();
    }

    default boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close() {
        throw new UnsupportedOperationException();
    }

    default void wireType(WireType wireType) {
        throw new UnsupportedOperationException();
    }

    default void clear() {
        throw new UnsupportedOperationException();
    }

    /**
     * publishes an event without a throttle key
     *
     * @param event the event to publish
     */
    default void publish(WriteMarshallable event) {
        put("", event);
    }
}
