package net.openhft.chronicle.network;

import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;

import java.nio.channels.SocketChannel;

public interface NetworkContext<T extends NetworkContext> {

    T isAcceptor(boolean serverSocket);

    boolean isAcceptor();

    T isUnchecked(boolean isUnchecked);

    boolean isUnchecked();

    T heartbeatIntervalTicks(long heartBeatIntervalTicks);

    long heartbeatIntervalTicks();

    T heartBeatTimeoutTicks(long heartBeatTimeoutTicks);

    long heartBeatTimeoutTicks();

    T socketChannel(SocketChannel sc);

    SocketChannel socketChannel();

    WireOutPublisher wireOutPublisher();

    void wireOutPublisher(WireOutPublisher wireOutPublisher);

    WireType wireType();

    T wireType(WireType wireType);

    SessionDetailsProvider sessionDetails();

    T sessionDetails(SessionDetailsProvider sessionDetails);


}
