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

package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;
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
    private static ServerThreadingStrategy value = ServerThreadingStrategy.SINGLE_THREADED;

    static {
        final String serverThreadingStrategy = System.getProperty("ServerThreadingStrategy");
        if (serverThreadingStrategy != null)

            try {
                value = Enum.valueOf(ServerThreadingStrategy.class, serverThreadingStrategy);
            } catch (Exception e) {
                Jvm.warn().on(ServerThreadingStrategy.class, "unable to apply -DServerThreadingStrategy=" + serverThreadingStrategy +
                        ", so defaulting to " + value, e);
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
