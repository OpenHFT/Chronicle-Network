package net.openhft.chronicle.network;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.TimingPauser;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;

public class VanillaNetworkContextTest extends NetworkTestCommon {

    @Test
    public void networkStatsListenerShouldNotBeClosedOnBackgroundResourceReleaserThread() throws TimeoutException {
        final VanillaNetworkContext<?> vanillaNetworkContext = new VanillaNetworkContext<>();
        AtomicReference<Boolean> wasClosedInBackgroundReleaserThread = new AtomicReference<>();
        vanillaNetworkContext.networkStatsListener(new NetworkStatsAdapter() {
            @Override
            public void close() {
                wasClosedInBackgroundReleaserThread.set(BackgroundResourceReleaser.isOnBackgroundResourceReleaserThread());
            }
        });
        vanillaNetworkContext.close();
        TimingPauser pauser = Pauser.balanced();
        while (wasClosedInBackgroundReleaserThread.get() == null) {
            pauser.pause(5, TimeUnit.SECONDS);
        }
        assertFalse(wasClosedInBackgroundReleaserThread.get());
    }
}
