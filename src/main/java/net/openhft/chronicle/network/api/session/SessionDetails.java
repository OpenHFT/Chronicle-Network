/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
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
package net.openhft.chronicle.network.api.session;

import net.openhft.chronicle.network.SessionMode;
import net.openhft.chronicle.network.connection.EventId;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Session local details stored here. <p> Created by Peter Lawrey on 01/06/15.
 */
public interface SessionDetails extends WriteMarshallable {

    // a unique id used to identify this session, this field is by contract immutable
    UUID sessionId();

    // a unique id used to identify the client
    UUID clientId();

    @Nullable
    String userId();

    @Nullable
    String securityToken();

    @Nullable
    String domain();

    SessionMode sessionMode();

    @Nullable
    InetSocketAddress clientAddress();

    long connectTimeMS();

    <I> void set(Class<I> infoClass, I info);

    @NotNull
    <I> I get(Class<I> infoClass);

    @Nullable
    WireType wireType();

    byte hostId();

    @Override
    default void writeMarshallable(@NotNull WireOut w) {
        w.writeEventName(EventId.userId).text(userId())
                .writeEventName(EventId.domain).text(domain())
                .writeEventName(EventId.sessionMode).text(sessionMode().toString())
                .writeEventName(EventId.securityToken).text(securityToken())
                .writeEventName(EventId.clientId).text(clientId().toString())
                .writeEventName(EventId.hostId).int8(hostId())
                .writeEventName(EventId.wireType).asEnum(wireType());
    }
}
