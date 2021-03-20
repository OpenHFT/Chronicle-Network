package net.openhft.chronicle.network;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VanillaNetworkContextTest extends NetworkTestCommon {

    @Test
    public void testClose() {
        final VanillaNetworkContext v = new VanillaNetworkContext();
        assertEquals(false, v.isClosed());
        v.close();
        assertEquals(true, v.isClosing());
        BackgroundResourceReleaser.releasePendingResources();
        assertEquals(true, v.isClosed());
        v.close();
        assertEquals(true, v.isClosed());
    }
}
