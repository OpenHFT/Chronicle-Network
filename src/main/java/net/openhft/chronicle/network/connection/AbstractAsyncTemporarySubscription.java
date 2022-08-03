/*
 * Copyright 2016-2020 chronicle.software
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
package net.openhft.chronicle.network.connection;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractAsyncTemporarySubscription extends AbstractAsyncSubscription
        implements AsyncTemporarySubscription {

    /**
     * @param hub  handles the tcp connectivity.
     * @param csp  the url of the subscription.
     * @param name the name of the subscription
     */
    protected AbstractAsyncTemporarySubscription(@NotNull TcpChannelHub hub, @Nullable String csp, String name) {
        super(hub, csp, name);
    }

    /**
     * @param hub  handles the tcp connectivity.
     * @param csp  the url of the subscription.
     * @param id   use as a seed to the tid, makes unique tid's makes reading the logs easier.
     * @param name the name of the subscription
     */
    protected AbstractAsyncTemporarySubscription(@NotNull TcpChannelHub hub, @Nullable String csp, byte id, String name) {
        super(hub, csp, id, name);
    }
}
