package net.openhft.chronicle.network;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VanillaNetworkContextTest {

    @Test
    public void testClose() {
        final VanillaNetworkContext v = new VanillaNetworkContext();
        assertEquals(false, v.isClosed());
        v.close();
        assertEquals(true, v.isClosed());
        v.close();
        assertEquals(true, v.isClosed());
    }

}
