package net.openhft.chronicle.network;

import net.openhft.chronicle.network.internal.NetworkConfig;
import net.openhft.chronicle.network.internal.netty.NettyBasedNetworkHub;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Rob Austin.
 */
public class Network {


    public Network() {
    }


    public static Closeable of(@NotNull final NetworkConfig replicationConfig,
                                @NotNull final NioCallbackFactory nioCallbackFactory) throws IOException {


        return new NettyBasedNetworkHub(replicationConfig, nioCallbackFactory);
        //  return new NetworkHub(replicationConfig, nioCallbackFactory);

    }


}
