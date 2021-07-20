/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
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
package net.openhft.chronicle.network.api.session;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.NetworkContextManager;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.RejectedExecutionException;

public interface SubHandler<T extends NetworkContext<T>> extends NetworkContextManager<T>, Closeable {

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

    /**
     * Some events may result in long-lived actions, which must yield periodically to allow (eg) buffers to clear (out messages to be sent)
     * A busy handler can indicate a long-lived action by returning <code>true</code> from inProgress
     * The caller should then use this to invoke onTouch to give the handler another slice
     *
     * @param outWire
     * @return - true if the long-lived action is complete; false if there's more to do
     */
    default boolean onTouch(WireOut outWire) { return true; }

    /**
     * Is a long-lived action (see above) in progress?
     * @return - true if long-lived action in progress; else false
     */
    default boolean inProgress() { return false; }

    void closeable(Closeable closeable);
}
