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

import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;

public interface AsyncSubscription {

    /**
     * returns the unique tid that will be used in the subscription, this tid must be unique per
     * socket connection
     *
     * @return the unique tid
     */
    long tid();

    /**
     * Implement this to establish a subscription with the server
     */
    void applySubscribe();

    /**
     * Implement this to consume the subscription
     *
     * @param inWire the wire to write the subscription to
     */
    void onConsumer(@NotNull final WireIn inWire);

    /**
     * called when the socket connection is closed
     */
    void onClose();

}
