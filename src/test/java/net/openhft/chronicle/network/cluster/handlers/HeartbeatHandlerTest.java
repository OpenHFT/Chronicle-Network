package net.openhft.chronicle.network.cluster.handlers;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;

public class HeartbeatHandlerTest {

    public static final long CID = 1234L;
    public static final int VALID_HEARTBEAT_TIMEOUT_MS = 1000;
    public static final int VALID_HEARTBEAT_INTERVAL_MS = 500;
    public static final int TOO_SMALL_HEARTBEAT_TIMEOUT_MS = 999;
    public static final int TOO_SMALL_HEARTBEAT_INTERVAL_MS = 499;

    @Test(expected = IllegalArgumentException.class)
    public void tooSmallHeartbeatIntervalMsThrowsIllegalArgumentException() {
        HeartbeatHandler.heartbeatHandler(VALID_HEARTBEAT_TIMEOUT_MS, TOO_SMALL_HEARTBEAT_INTERVAL_MS, CID);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooSmallHeartbeatIntervalMsThrowsIllegalArgumentExceptionConstructor() {
        createByDeserialization(VALID_HEARTBEAT_TIMEOUT_MS, TOO_SMALL_HEARTBEAT_INTERVAL_MS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooSmallHeartbeatTimeoutMsThrowsIllegalArgumentException() {
        HeartbeatHandler.heartbeatHandler(TOO_SMALL_HEARTBEAT_TIMEOUT_MS, VALID_HEARTBEAT_INTERVAL_MS, CID);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooSmallHeartbeatTimeoutMsThrowsIllegalArgumentExceptionConstructor() {
        createByDeserialization(TOO_SMALL_HEARTBEAT_TIMEOUT_MS, VALID_HEARTBEAT_INTERVAL_MS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void intervalEqualToTimeoutThrowsIllegalStateException() {
        HeartbeatHandler.heartbeatHandler(VALID_HEARTBEAT_TIMEOUT_MS, VALID_HEARTBEAT_TIMEOUT_MS, CID);
    }

    @Test(expected = IllegalArgumentException.class)
    public void intervalEqualToTimeoutThrowsIllegalStateExceptionConstructor() {
        createByDeserialization(VALID_HEARTBEAT_TIMEOUT_MS, VALID_HEARTBEAT_TIMEOUT_MS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void intervalGreaterThanTimeoutThrowsIllegalStateException() {
        HeartbeatHandler.heartbeatHandler(VALID_HEARTBEAT_TIMEOUT_MS, VALID_HEARTBEAT_TIMEOUT_MS + 100, CID);
    }

    @Test(expected = IllegalArgumentException.class)
    public void intervalGreaterThanTimeoutThrowsIllegalStateExceptionConstructor() {
        createByDeserialization(VALID_HEARTBEAT_TIMEOUT_MS, VALID_HEARTBEAT_TIMEOUT_MS + 100);
    }

    private void createByDeserialization(long heartbeatTimeoutMs, long heartbeatIntervalMs) {
        Wire wire = new BinaryWire(Bytes.elasticByteBuffer());
        wire.write("heartbeatTimeoutMs").int64(heartbeatTimeoutMs);
        wire.write("heartbeatIntervalMs").int64(heartbeatIntervalMs);
        new HeartbeatHandler<>(wire);
    }
}