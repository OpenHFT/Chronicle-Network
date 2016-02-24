package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public class LegacyHandedFactory {

    public static <T extends NetworkContext> Function<T, TcpEventHandler>
    legacyTcpEventHandlerFactory(@NotNull final Function<T, TcpHandler> defaultHandedFactory,
                                 final long heartbeatIntervalTicks,
                                 final long heartbeatIntervalTimeout) {
        return (networkContext) -> {

            try {

                final TcpEventHandler handler = new TcpEventHandler(networkContext);

                final Function<Marshallable, TcpHandler> consumer = o -> {
                    if (o instanceof SessionDetailsProvider) {

                        final NetworkContext nc = networkContext;
                        nc.heartbeatIntervalTicks(heartbeatIntervalTicks);
                        nc.heartBeatTimeoutTicks(heartbeatIntervalTimeout);
                        nc.sessionDetails((SessionDetailsProvider) o);
                        return defaultHandedFactory.apply((T) nc);
                    } else if (o instanceof TcpHandler)
                        return (TcpHandler) o;

                    throw new UnsupportedOperationException("");
                };

                final HeaderTcpHandler headerTcpHandler = new HeaderTcpHandler(handler, consumer, networkContext);

                final WireTypeSniffingTcpHandler wireTypeSniffingTcpHandler = new
                        WireTypeSniffingTcpHandler(handler, networkContext, (nc) -> headerTcpHandler);

                handler.tcpHandler(wireTypeSniffingTcpHandler);
                return handler;

            } catch (IOException e) {
                throw Jvm.rethrow(e);
            }
        };
    }

    public static <T extends NetworkContext> Function<T, TcpEventHandler> legacyTcpEventHandlerFactory(
            @NotNull final Function<T, TcpHandler> defaultHandedFactory) {
        return legacyTcpEventHandlerFactory(defaultHandedFactory, 20_000, 40_000);
    }


    public static <T extends NetworkContext> Function<T, TcpEventHandler>
    simpleTcpEventHandlerFactory(@NotNull final Function<T, TcpHandler> defaultHandedFactory) {
        return (networkContext) -> {

            try {
                networkContext.wireOutPublisher(new VanillaWireOutPublisher(WireType.TEXT));

                final TcpEventHandler handler = new TcpEventHandler(networkContext);
                handler.tcpHandler(new WireTypeSniffingTcpHandler(handler, networkContext,
                        defaultHandedFactory));
                return handler;

            } catch (IOException e) {
                throw Jvm.rethrow(e);
            }
        };
    }
}