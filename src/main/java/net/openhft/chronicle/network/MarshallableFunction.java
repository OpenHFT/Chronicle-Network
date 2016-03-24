package net.openhft.chronicle.network;

import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public interface MarshallableFunction<T, R> extends Demarshallable, WriteMarshallable,
        Function<T, R> {

    default void writeMarshallable(@NotNull WireOut wire) {

    }
}
