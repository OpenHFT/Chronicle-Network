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

package net.openhft.chronicle.network.cluster.handlers;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.network.NetworkTestCommon;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class HeartbeatHandlerTest extends NetworkTestCommon {

    public static final long CID = 1234L;
    public static final int VALID_HEARTBEAT_TIMEOUT_MS = 1000;
    public static final int VALID_HEARTBEAT_INTERVAL_MS = 500;
    public static final int TOO_SMALL_HEARTBEAT_TIMEOUT_MS = 999;
    public static final int TOO_SMALL_HEARTBEAT_INTERVAL_MS = 499;

    @Test
    void tooSmallHeartbeatIntervalMsThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
                HeartbeatHandler.heartbeatHandler(VALID_HEARTBEAT_TIMEOUT_MS, TOO_SMALL_HEARTBEAT_INTERVAL_MS, CID));
    }

    @Test
    void tooSmallHeartbeatIntervalMsThrowsIllegalArgumentExceptionConstructor() {
        assertThrows(IllegalArgumentException.class, () ->
            createByDeserialization(VALID_HEARTBEAT_TIMEOUT_MS, TOO_SMALL_HEARTBEAT_INTERVAL_MS));
    }

    @Test
    void tooSmallHeartbeatTimeoutMsThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
            HeartbeatHandler.heartbeatHandler(TOO_SMALL_HEARTBEAT_TIMEOUT_MS, VALID_HEARTBEAT_INTERVAL_MS, CID));
    }

    @Test
    void tooSmallHeartbeatTimeoutMsThrowsIllegalArgumentExceptionConstructor() {
        assertThrows(IllegalArgumentException.class, () ->
            createByDeserialization(TOO_SMALL_HEARTBEAT_TIMEOUT_MS, VALID_HEARTBEAT_INTERVAL_MS));
    }

    @Test
    void intervalEqualToTimeoutThrowsIllegalStateException() {
        assertThrows(IllegalArgumentException.class, () ->
            HeartbeatHandler.heartbeatHandler(VALID_HEARTBEAT_TIMEOUT_MS, VALID_HEARTBEAT_TIMEOUT_MS, CID));
    }

    @Test
    void intervalEqualToTimeoutThrowsIllegalStateExceptionConstructor() {
        assertThrows(IllegalArgumentException.class, () ->
            createByDeserialization(VALID_HEARTBEAT_TIMEOUT_MS, VALID_HEARTBEAT_TIMEOUT_MS));
    }

    @Test
    void intervalGreaterThanTimeoutThrowsIllegalStateException() {
        assertThrows(IllegalArgumentException.class, () ->
            HeartbeatHandler.heartbeatHandler(VALID_HEARTBEAT_TIMEOUT_MS, VALID_HEARTBEAT_TIMEOUT_MS + 100, CID));
    }

    @Test
    void intervalGreaterThanTimeoutThrowsIllegalStateExceptionConstructor() {
        assertThrows(IllegalArgumentException.class, () ->
            createByDeserialization(VALID_HEARTBEAT_TIMEOUT_MS, VALID_HEARTBEAT_TIMEOUT_MS + 100));
    }

    private void createByDeserialization(long heartbeatTimeoutMs, long heartbeatIntervalMs) {
        Wire wire = new BinaryWire(Bytes.elasticByteBuffer());
        wire.write("heartbeatTimeoutMs").int64(heartbeatTimeoutMs);
        wire.write("heartbeatIntervalMs").int64(heartbeatIntervalMs);
        new HeartbeatHandler<>(wire);
    }
}