package net.openhft.chronicle.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rob on 25/08/2015.
 */
public enum ServerThreadingStrategy {

    SINGLE_THREADED("uses a single threaded prioritised event loop," +
            " where the reads take priority over the asynchronous writes"),
    MULTI_THREADED_BUSY_WAITING("each client connection is devoted to its own busy waiting thread, " +
            "Ideal when you have a small number of connection on a server with a large number of available free cores"),;


    private final String description;

    ServerThreadingStrategy(String description) {
        this.description = description;
    }

    public static ServerThreadingStrategy value = ServerThreadingStrategy.SINGLE_THREADED;
    private static final Logger LOG = LoggerFactory.getLogger(ServerThreadingStrategy.class);

    static {
        final String serverThreadingStrategy = System.getProperty("ServerThreadingStrategy");
        if (serverThreadingStrategy != null)

            try {
                value = Enum.valueOf(ServerThreadingStrategy.class, serverThreadingStrategy);
            } catch (Exception e) {
                LOG.error("unable to apply -DServerThreadingStrategy=" + serverThreadingStrategy +
                        ", so defaulting to " + value);
            }
    }


    public String getDescription() {
        return description;
    }

    static ServerThreadingStrategy serverThreadingStrategy() {
        return value;
    }
}
