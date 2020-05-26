/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.performance.tests.network;

import net.openhft.chronicle.network.HeaderTcpHandler;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.WireTypeSniffingTcpHandler;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static net.openhft.chronicle.wire.WireType.TEXT;

public enum LegacyHanderFactory {
    INSTANCE;

    public static <T extends NetworkContext<T>> Function<T, TcpEventHandler<T>>
    legacyTcpEventHandlerFactory(@NotNull final Function<T, TcpHandler<T>> defaultHandedFactory,
                                 final long heartbeatIntervalTicks,
                                 final long heartbeatIntervalTimeout) {
        return (networkContext) -> {
            @NotNull final TcpEventHandler<T> handler = new TcpEventHandler<>(networkContext);

            @NotNull final Function<Object, TcpHandler<T>> consumer = o -> {
                if (o instanceof SessionDetailsProvider) {
                    networkContext.sessionDetails((SessionDetailsProvider) o);
                    return defaultHandedFactory.apply(networkContext);
                } else if (o instanceof TcpHandler)
                    return (TcpHandler<T>) o;

                throw new UnsupportedOperationException("");
            };

            @NotNull final HeaderTcpHandler<T> headerTcpHandler = new HeaderTcpHandler<>(handler,
                    consumer
            );

            @NotNull final WireTypeSniffingTcpHandler<T> wireTypeSniffingTcpHandler =
                    new WireTypeSniffingTcpHandler<>(handler, (nc) -> headerTcpHandler);

            handler.tcpHandler(wireTypeSniffingTcpHandler);
            return handler;
        };
    }

    public static <T extends NetworkContext<T>> Function<T, TcpEventHandler<T>> legacyTcpEventHandlerFactory(
            @NotNull final Function<T, TcpHandler<T>> defaultHandedFactory) {
        return legacyTcpEventHandlerFactory(defaultHandedFactory, 20_000, 40_000);
    }

    public static <T extends NetworkContext<T>> Function<T, TcpEventHandler<T>> simpleTcpEventHandlerFactory(@NotNull final Function<T, TcpHandler<T>> defaultHandedFactory, final WireType text) {
        return (networkContext) -> {

            networkContext.wireOutPublisher(new VanillaWireOutPublisher(TEXT));
            @NotNull final TcpEventHandler<T> handler = new TcpEventHandler<>(networkContext);
            handler.tcpHandler(new WireTypeSniffingTcpHandler<>(handler,
                    defaultHandedFactory));
            return handler;

        };
    }

    public static <T extends NetworkContext<T>> Function<T, TcpEventHandler<T>> defaultTcpEventHandlerFactory(@NotNull final Function<T, TcpHandler<T>> defaultHandedFactory) {
        return (networkContext) -> {
            networkContext.wireOutPublisher(new VanillaWireOutPublisher(TEXT));
            @NotNull final TcpEventHandler<T> handler = new TcpEventHandler<>(networkContext);
            handler.tcpHandler(defaultHandedFactory.apply(networkContext));
            return handler;

        };
    }
}