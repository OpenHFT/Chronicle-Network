/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.network;

/**
 * @author Rob Austin.
 */
@FunctionalInterface
public interface HeartbeatListener {

    /**
     * called when we don't receive a heartbeat ( or in some cases any message )
     *
     * @return false if TcpHandler is allowed to drop the connection, true if the implementer tries to recover
     */
    boolean onMissedHeartbeat();

    /**
     * If the above returned true, this method should provide the amount of time TcpHandler should wait before condsidering connection dead.
     */
    default long lingerTimeBeforeDisconnect() {
        return 0;
    }
}
