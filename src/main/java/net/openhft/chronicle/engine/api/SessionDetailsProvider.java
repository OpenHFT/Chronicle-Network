package net.openhft.chronicle.engine.api;

import java.net.InetSocketAddress;

/**
 * Created by Rob Austin
 */
public interface SessionDetailsProvider extends SessionDetails {

    void setConnectTimeMS(long connectTimeMS);

    void setConnectionAddress(InetSocketAddress connectionAddress);

    void setSecurityToken(String securityToken);

    void setUserId(String userId);

}
