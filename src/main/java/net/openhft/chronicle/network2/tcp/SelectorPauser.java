package net.openhft.chronicle.network2.tcp;

import net.openhft.chronicle.network.internal.netty.NettyBasedNetworkHub;
import net.openhft.lang.thread.Pauser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
public class SelectorPauser implements Pauser {

    private static final Logger LOG = LoggerFactory.getLogger(NettyBasedNetworkHub.class.getName());

    private final Selector selector;
    private final long parkPeriod;

    public SelectorPauser(Selector selector, long parkPeriod) {
        this.selector = selector;
        this.parkPeriod = parkPeriod;
    }

    @Override
    public void reset() {

    }

    @Override
    public void pause() {
        pause(parkPeriod);
    }

    @Override
    public void pause(long maxPauseNS) {
        long millis = TimeUnit.NANOSECONDS.toMillis(maxPauseNS);

        try {
            if (millis > 0) {
                selector.select(millis);
            } else {
                selector.select(1);
            }
        } catch (IOException e) {
            LOG.error("", e);
        }
    }

    @Override
    public void unpause() {
        selector.wakeup();
    }
}
