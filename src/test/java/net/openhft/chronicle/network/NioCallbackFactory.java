package net.openhft.chronicle.network;

import net.openhft.chronicle.network.internal.Actions;

/**
 * @author Rob Austin.
 */
public interface NioCallbackFactory {

    NioCallback onCreate(Actions withActions);

}
