package net.openhft.chronicle.network;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TCPRegistryTest {

    private final boolean useCrossProcess;

    public TCPRegistryTest(boolean useCrossProcess) {
        this.useCrossProcess = useCrossProcess;
    }

    @Parameterized.Parameters(name = "useCrossProcess = {0}")
    public static Collection<Object> params() {
        return Arrays.asList(false, true);
    }

    @Before
    public void setUp() {
        if (useCrossProcess) {
            TCPRegistry.useCrossProcessRegistry();
        }
    }

    @Test
    public void testResetClearsRegistry() throws IOException {
        TCPRegistry.createServerSocketChannelFor("host1", "host2", "host3");
        assertNotNull(TCPRegistry.lookup("host1"));
        assertNotNull(TCPRegistry.lookup("host2"));
        assertNotNull(TCPRegistry.lookup("host3"));
        TCPRegistry.reset();
        assertNotMapped("host1");
        assertNotMapped("host2");
        assertNotMapped("host3");
    }

    @Test
    public void testResetIsIdempotent() throws IOException {
        TCPRegistry.createServerSocketChannelFor("host1", "host2", "host3");
        TCPRegistry.reset();
        TCPRegistry.reset();
    }

    private void assertNotMapped(String hostName) {
        try {
            TCPRegistry.lookup(hostName);
            fail(String.format("Found mapping for %s", hostName));
        } catch (IllegalArgumentException ex) {
            // This is good
        }
    }
}