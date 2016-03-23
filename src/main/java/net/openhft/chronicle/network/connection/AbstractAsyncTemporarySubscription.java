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

package net.openhft.chronicle.network.connection;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * @author Rob Austin.
 */
public abstract class AbstractAsyncTemporarySubscription extends AbstractAsyncSubscription
        implements AsyncTemporarySubscription {

    /**
     * @param hub  handles the tcp connectivity.
     * @param csp  the url of the subscription.
     * @param name the name of the subscription
     */
    public AbstractAsyncTemporarySubscription(@NotNull TcpChannelHub hub, @Nullable String csp, String name) {
        super(hub, csp, name);
    }

    /**
     * @param hub  handles the tcp connectivity.
     * @param csp  the url of the subscription.
     * @param id   use as a seed to the tid, makes unique tid's makes reading the logs easier.
     * @param name the name of the subscription
     */
    public AbstractAsyncTemporarySubscription(@NotNull TcpChannelHub hub, @Nullable String csp, byte id, String name) {
        super(hub, csp, id, name);
    }
}
