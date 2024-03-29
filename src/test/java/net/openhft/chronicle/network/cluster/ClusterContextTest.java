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

package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.network.NetworkTestCommon;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.TimingPauser;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class ClusterContextTest extends NetworkTestCommon {
    @Override
    @BeforeEach
    protected void threadDump() {
        super.threadDump();
    }

    @Test
    void testStatesAreStillInTheCorrectOrder() {
        // We rely on the ordering of these
        assertArrayEquals(new ClusterContext.Status[]{
                ClusterContext.Status.NOT_CLOSED,
                ClusterContext.Status.STOPPING,
                ClusterContext.Status.CLOSING,
                ClusterContext.Status.CLOSED,
        }, ClusterContext.Status.values());
    }

    @Test
    void isClosingAndIsClosedReturnFalseWhenNotClosed() {
        final TestClusterContext testClusterContext = new TestClusterContext();
        assertFalse(testClusterContext.isClosed());
        assertFalse(testClusterContext.isClosing());
    }

    @Test
    void isClosingAndIsClosedReturnsFalseWhenWeAreInPerformStopMethod() throws InterruptedException {
        BlockingTestClusterContext tcc = new BlockingTestClusterContext();
        tcc.closeGate.release();
        Thread t = new Thread(tcc::close);
        t.start();
        while (!tcc.stopGate.hasQueuedThreads()) {
            Jvm.pause(1);
        }
        assertFalse(tcc.isClosing());
        assertFalse(tcc.isClosed());
        tcc.stopGate.release();
        t.join();
    }

    @Test
    void isClosingReturnsTrueAndIsClosedReturnsFalseWhenWeAreInPerformCloseMethod() throws InterruptedException {
        BlockingTestClusterContext tcc = new BlockingTestClusterContext();
        tcc.stopGate.release();
        Thread t = new Thread(tcc::close);
        t.start();
        while (!tcc.closeGate.hasQueuedThreads()) {
            Jvm.pause(1);
        }
        assertTrue(tcc.isClosing());
        assertFalse(tcc.isClosed());
        tcc.closeGate.release();
        t.join();
    }

    @Test
    void isClosingAndIsClosedReturnTrueWhenClosed() throws TimeoutException {
        TestClusterContext tcc = new TestClusterContext();
        tcc.close();
        TimingPauser pauser = Pauser.balanced();
        while (!(tcc.isClosed() && tcc.isClosing())) {
            pauser.pause(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void subsequentThreadsBlockUntilClosedWhenCloseIsCalledByMultiThreads() throws InterruptedException {
        BlockingTestClusterContext tcc = new BlockingTestClusterContext();
        tcc.stopGate.release();
        Thread firstCloser = new Thread(tcc::close);
        firstCloser.start();
        while (!tcc.closeGate.hasQueuedThreads()) {
            Jvm.pause(1);
        }
        AtomicBoolean closeReturnForSecondCloser = new AtomicBoolean(false);
        Thread secondCloser = new Thread(() -> {
            tcc.close();
            closeReturnForSecondCloser.set(true);
        });
        secondCloser.start();
        long endTime = System.currentTimeMillis() + 500;
        while (System.currentTimeMillis() < endTime) {
            assertFalse(closeReturnForSecondCloser.get());
            Jvm.pause(1);
        }
        tcc.closeGate.release();
        firstCloser.join();
        secondCloser.join();
    }

    class BlockingTestClusterContext extends TestClusterContext {

        public Semaphore stopGate = new Semaphore(0);
        public Semaphore closeGate = new Semaphore(0);

        @Override
        protected void performStop() {
            super.performStop();
            waitAtGate(stopGate);
        }

        @Override
        protected void performClose() {
            super.performClose();
            waitAtGate(closeGate);
        }

        private void waitAtGate(Semaphore semaphore) {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    class TestClusterContext extends ClusterContext<TestClusterContext, TestNetworkContext> {

        @Override
        public @NotNull ThrowingFunction<TestNetworkContext, TcpEventHandler<TestNetworkContext>, IOException> tcpEventHandlerFactory() {
            return TcpEventHandler::new;
        }

        @Override
        protected void defaults() {
        }

        @Override
        protected String clusterNamePrefix() {
            return "test";
        }
    }

    class TestNetworkContext extends VanillaClusteredNetworkContext<TestNetworkContext, TestClusterContext> {

        public TestNetworkContext(@NotNull TestClusterContext clusterContext) {
            super(clusterContext);
        }
    }
}
