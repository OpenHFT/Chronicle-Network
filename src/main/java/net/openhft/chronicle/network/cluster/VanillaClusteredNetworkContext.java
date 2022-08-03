/*
 * Copyright 2016-2022 chronicle.software
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

package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.network.VanillaNetworkContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VanillaClusteredNetworkContext<T extends VanillaClusteredNetworkContext<T, C>, C extends ClusterContext<C, T>>
        extends VanillaNetworkContext<T> implements ClusteredNetworkContext<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VanillaClusteredNetworkContext.class);

    @NotNull
    private final EventLoop eventLoop;

    @NotNull
    protected final C clusterContext;

    public VanillaClusteredNetworkContext(@NotNull C clusterContext) {
        this.clusterContext = clusterContext;
        this.eventLoop = clusterContext.eventLoop();
        heartbeatListener(this::logMissedHeartbeat);
        serverThreadingStrategy(clusterContext.serverThreadingStrategy());
    }

    @Override
    public EventLoop eventLoop() {
        return eventLoop;
    }

    @Override
    public byte getLocalHostIdentifier() {
        return clusterContext.localIdentifier();
    }

    @Override
    public C clusterContext() {
        return clusterContext;
    }

    private boolean logMissedHeartbeat() {
        LOGGER.warn("Missed heartbeat on network context " + socketChannel());
        return false;
    }
}
