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

package net.openhft.chronicle.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rob on 25/08/2015.
 */
public enum ServerThreadingStrategy {

    SINGLE_THREADED("uses a single threaded prioritised event loop," +
            " where the reads take priority over the asynchronous writes"),
    MULTI_THREADED_BUSY_WAITING("each client connection is devoted to its own busy waiting thread, " +
            "This is ideal when you have a small number of client connections on a server with a large number of free cores");

    private static final Logger LOG = LoggerFactory.getLogger(ServerThreadingStrategy.class);
    public static ServerThreadingStrategy value = ServerThreadingStrategy.SINGLE_THREADED;

    static {
        final String serverThreadingStrategy = System.getProperty("ServerThreadingStrategy");
        if (serverThreadingStrategy != null)

            try {
                value = Enum.valueOf(ServerThreadingStrategy.class, serverThreadingStrategy);
            } catch (Exception e) {
                LOG.error("unable to apply -DServerThreadingStrategy=" + serverThreadingStrategy +
                        ", so defaulting to " + value);
            }
    }

    private final String description;

    ServerThreadingStrategy(String description) {
        this.description = description;
    }

    static ServerThreadingStrategy serverThreadingStrategy() {
        return value;
    }

    public String getDescription() {
        return description;
    }
}
