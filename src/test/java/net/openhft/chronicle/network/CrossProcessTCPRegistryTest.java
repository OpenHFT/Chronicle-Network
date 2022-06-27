package net.openhft.chronicle.network;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

class CrossProcessTCPRegistryTest extends TCPRegistryTest {

    @BeforeEach
    void setUp() {
        TCPRegistry.useCrossProcessRegistry();
    }

    @AfterEach
    void tearDown() {
        TCPRegistry.useInMemoryRegistry();
    }
}
