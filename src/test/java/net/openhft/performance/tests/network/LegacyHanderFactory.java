/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.performance.tests.network;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.network.HeaderTcpHandler;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.WireTypeSniffingTcpHandler;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.function.Function;

import static net.openhft.chronicle.wire.WireType.TEXT;

/**
 * @author Rob Austin.
 */
public class LegacyHanderFactory {

    public static <T extends NetworkContext> Function<T, TcpEventHandler>
    legacyTcpEventHandlerFactory(@NotNull final Function<T, TcpHandler> defaultHandedFactory,
                                 final long heartbeatIntervalTicks,
                                 final long heartbeatIntervalTimeout) {
        return (networkContext) -> {

            try {

                final TcpEventHandler handler = new TcpEventHandler(networkContext);

                final Function<Object, TcpHandler> consumer = o -> {
                    if (o instanceof SessionDetailsProvider) {

                        networkContext.heartbeatIntervalTicks(heartbeatIntervalTicks);
                        networkContext.heartBeatTimeoutTicks(heartbeatIntervalTimeout);
                        networkContext.sessionDetails((SessionDetailsProvider) o);
                        return defaultHandedFactory.apply(networkContext);
                    } else if (o instanceof TcpHandler)
                        return (TcpHandler) o;

                    throw new UnsupportedOperationException("");
                };

                final Function<WireType, WireOutPublisher> f = wireType -> new
                        VanillaWireOutPublisher(wireType);

                final HeaderTcpHandler headerTcpHandler = new HeaderTcpHandler(handler,
                        consumer,
                        networkContext
                );


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
    simpleTcpEventHandlerFactory(@NotNull final Function<T, TcpHandler> defaultHandedFactory, final WireType text) {
        return (networkContext) -> {

            networkContext.wireOutPublisher(new VanillaWireOutPublisher(TEXT));
            try {
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