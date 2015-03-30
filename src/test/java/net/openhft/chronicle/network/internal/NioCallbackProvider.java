package net.openhft.chronicle.network.internal;

import net.openhft.chronicle.network.NioCallback;

/**
 * @author Rob Austin.
 */
public interface NioCallbackProvider {
    NioCallback getUserAttached();
}
