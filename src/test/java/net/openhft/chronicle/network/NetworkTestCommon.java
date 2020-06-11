package net.openhft.chronicle.network;

import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import org.junit.After;
import org.junit.Before;

public class NetworkTestCommon {

    @Before
    public void enableReferenceTracing() {
        AbstractReferenceCounted.enableReferenceTracing();
    }

    @After
    public void assertReferencesReleased() {
        TCPRegistry.reset();
        AbstractReferenceCounted.assertReferencesReleased();
    }
}
