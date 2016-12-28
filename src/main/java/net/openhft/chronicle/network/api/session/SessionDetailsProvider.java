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

package net.openhft.chronicle.network.api.session;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.network.SessionMode;
import net.openhft.chronicle.network.connection.EventId;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Created by Rob Austin
 */
public interface SessionDetailsProvider extends SessionDetails, Marshallable {

    void connectTimeMS(long connectTimeMS);

    void clientAddress(InetSocketAddress connectionAddress);

    void securityToken(String securityToken);

    void userId(String userId);

    void domain(String domain);

    void sessionMode(SessionMode sessionMode);

    void clientId(UUID clientId);

    void wireType(@Nullable WireType wireType);

    void hostId(byte id);

    @Override
    default void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        userId(wire.read(EventId.userId).text());
        domain(wire.read(EventId.domain).text());
        sessionMode(wire.read(EventId.sessionMode).object(SessionMode.class));
        securityToken(wire.read(EventId.securityToken).text());
        @Nullable final String uid = wire.read(EventId.clientId).text();
        if (uid != null)
            clientId(UUID.fromString(uid));
        wireType(wire.read(EventId.wireType).object(WireType.class));
        hostId(wire.read(EventId.hostId).int8());
    }

    @Override
    default void writeMarshallable(@NotNull WireOut w) {
        SessionDetails.super.writeMarshallable(w);
    }
}
