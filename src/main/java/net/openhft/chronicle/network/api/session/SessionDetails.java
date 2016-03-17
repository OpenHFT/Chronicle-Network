/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
 * Session local details stored here. <p> Created by peter on 01/06/15.
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
