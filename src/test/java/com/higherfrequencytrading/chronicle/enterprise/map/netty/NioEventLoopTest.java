package com.higherfrequencytrading.chronicle.enterprise.map.netty;

import io.netty.channel.nio.NioEventLoopGroup;
import net.openhft.chronicle.network.internal.netty.NioEventLoop;
import org.junit.Test;

import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.ThreadFactory;

/**
 * @author Rob Austin.
 */
public class NioEventLoopTest {

    @Test
    public void testPingPong() throws Exception {
        ThreadFactory f = new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("netty-io");
                thread.setDaemon(true);
                return thread;
            }
        };

        NioEventLoop eventExecutors = new NioEventLoop(new NioEventLoopGroup(1), f,
                SelectorProvider.provider());

        eventExecutors.rebuildSelector();


    }
}
