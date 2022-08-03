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

package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.connection.CoreFields;

import java.util.function.IntSupplier;
import java.util.function.Supplier;

final class PingSender implements EventHandler {

    private final Supplier<NetworkContext> nc;
    private final IntSupplier local;
    private final IntSupplier remote;
    private final long cid;
    private long lastPublish = 0;
    private int counter;

    PingSender(final Supplier<NetworkContext> nc, final IntSupplier local, final IntSupplier remote, final long cid) {
        this.nc = nc;
        this.local = local;
        this.remote = remote;
        this.cid = cid;
    }

    @Override
    public boolean action() throws InvalidEventHandlerException {
        if (lastPublish < System.currentTimeMillis() - 5000L) {

            nc.get().wireOutPublisher().put(null, wireOut -> {
                wireOut.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid));
                wireOut.writeDocument(false,
                        d -> d.writeEventName("ping").int32(counter++));

            });
            lastPublish = System.currentTimeMillis();
        }
        return false;
    }
}
