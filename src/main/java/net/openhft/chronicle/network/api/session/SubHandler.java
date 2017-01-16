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

package net.openhft.chronicle.network.api.session;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.NetworkContextManager;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.RejectedExecutionException;

/**
 * @author Rob Austin.
 */
public interface SubHandler<T extends NetworkContext> extends NetworkContextManager<T>,
        Closeable {

    void cid(long cid);

    long cid();

    void csp(@NotNull String cspText);

    String csp();

    void onRead(@NotNull WireIn inWire, @NotNull WireOut outWire);

    Closeable closable();

    void remoteIdentifier(int remoteIdentifier);

    void localIdentifier(int localIdentifier);

    /**
     * called after all the construction and configuration has completed
     *
     * @param outWire allow data to be written
     */
    void onInitialize(WireOut outWire) throws RejectedExecutionException;

    void closeable(Closeable closeable);

    default void onWrite(WireOut outWire) {

    }
}
