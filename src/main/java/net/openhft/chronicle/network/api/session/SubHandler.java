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

package net.openhft.chronicle.network.api.session;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.NetworkContextManager;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public interface SubHandler<T extends NetworkContext> extends NetworkContextManager<T>, Closeable {

    void cid(long cid);

    long cid();

    void csp(@NotNull String cspText);

    String csp();

    void processData(@NotNull WireIn inWire, @NotNull WireOut outWire);

    Closeable closable();

    void remoteIdentifier(int remoteIdentifier);

    /**
     * called after all the construction and configuration has completed
     *
     * @param outWire allow data to be written
     */
    void onInitialize(WireOut outWire);

    void closeable(Closeable closeable);

}
