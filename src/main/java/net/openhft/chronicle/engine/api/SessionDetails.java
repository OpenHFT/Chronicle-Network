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

package net.openhft.chronicle.engine.api;

import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Session local details stored here.
 * <p>
 * Created by peter on 01/06/15.
 */
public interface SessionDetails {

    // a unique id used to identify this session, this field is by contract immutable
    UUID sessionId();

    @Nullable
    String userId();

    @Nullable
    String securityToken();

    @Nullable
    InetSocketAddress clientAddress();

    long connectTimeMS();

    <I> void set(Class<I> infoClass, I info);

    <I> I get(Class<I> infoClass);
}
