package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.Marshallable;

import static net.openhft.chronicle.core.Mocker.intercepting;

/**
 * @author Rob Austin.
 */
@FunctionalInterface
public interface SocketConnectionNotifier<T extends NetworkContext> extends Marshallable {

    static SocketConnectionNotifier newDefaultConnectionNotifier() {
        return intercepting(SocketConnectionNotifier.class,
                "connection ",
                msg -> Jvm.debug().on(SocketConnectionNotifier.class, msg));
    }

    void onConnected(String host, long port, T nc);

    default void onDisconnected(String host, long port) {
    }
}
