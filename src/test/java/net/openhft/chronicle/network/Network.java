/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
