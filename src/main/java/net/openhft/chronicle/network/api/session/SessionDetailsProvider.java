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

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.network.SessionMode;
import net.openhft.chronicle.network.connection.EventId;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

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

    void wireType(WireType wireType);

    void hostId(byte id);

    @Override
    default void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        domain(wire.read(EventId.domain).text());
        sessionMode(wire.read(EventId.sessionMode).object(SessionMode.class));
        securityToken(wire.read(EventId.securityToken).text());
        clientId(wire.read(EventId.clientId).object(UUID.class));
        wireType(wire.read(EventId.wireType).object(WireType.class));
        hostId(wire.read(EventId.hostId).int8());
    }
}
