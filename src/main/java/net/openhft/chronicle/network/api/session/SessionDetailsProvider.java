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

import net.openhft.chronicle.core.annotation.Nullable;
import net.openhft.chronicle.network.SessionMode;
import net.openhft.chronicle.wire.WireType;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Created by Rob Austin
 */
public interface SessionDetailsProvider extends SessionDetails {

    void connectTimeMS(long connectTimeMS);

    void clientAddress(InetSocketAddress connectionAddress);

    void securityToken(String securityToken);

    void userId(String userId);

    void domain(String domain);

    void sessionMode(SessionMode sessionMode);

    void clientId(UUID clientId);

    void wireType(WireType uuid);

    @Nullable
    WireType wireType();
}
