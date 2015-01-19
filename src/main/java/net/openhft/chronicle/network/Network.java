package net.openhft.chronicle.network;

import net.openhft.chronicle.network.internal.NetworkConfig;
import net.openhft.chronicle.network.internal.NetworkHub;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * @author Rob Austin.
 */
public class Network {


    public Network() {
    }


    public static NetworkHub of(@NotNull final NetworkConfig replicationConfig,
                                @NotNull final NioCallbackFactory nioCallbackFactory) throws IOException {

        return new NetworkHub(replicationConfig, nioCallbackFactory);

    }


}
