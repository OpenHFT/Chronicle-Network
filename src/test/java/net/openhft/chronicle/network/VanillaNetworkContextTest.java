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

package net.openhft.chronicle.network;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.TimingPauser;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.testframework.CloseableUtil.closeQuietly;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.*;

class VanillaNetworkContextTest extends NetworkTestCommon {

    @Test
    void networkStatsListenerShouldNotBeClosedOnBackgroundResourceReleaserThread() throws TimeoutException {
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

    @Test
    void closeListenersAreCalledOnClose() {
        Closeable closeListener1 = mock(Closeable.class);
        Closeable closeListener2 = mock(Closeable.class);

        final VanillaNetworkContext<?> vanillaNetworkContext = new VanillaNetworkContext<>();
        // Add 1 twice, it should only get called once
        vanillaNetworkContext.addCloseListener(closeListener1);
        vanillaNetworkContext.addCloseListener(closeListener1);

        vanillaNetworkContext.addCloseListener(closeListener2);

        verify(closeListener1, never()).close();
        verify(closeListener2, never()).close();

        closeQuietly(vanillaNetworkContext);

        verify(closeListener1).close();
        verify(closeListener2).close();
    }
}
