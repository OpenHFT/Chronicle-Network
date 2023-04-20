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

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface FatalFailureMonitor {
    Logger LOG = LoggerFactory.getLogger(FatalFailureMonitor.class);

    /**
     * A no-op implementation
     */
    FatalFailureMonitor NO_OP = new FatalFailureMonitor() {
        @Override
        public void onFatalFailure(@Nullable String name, String message) {
        }
    };

    /**
     * called if all the connection attempts/(and/or timeouts) determined by the connection strategy has been exhausted
     *
     * @param name    the name of the connection
     * @param message reason
     */
    default void onFatalFailure(@Nullable String name, String message) {
        LOG.error("name=" + name + ",message=" + message);
    }
}
