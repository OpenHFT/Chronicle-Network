package net.openhft.chronicle.engine.session;

import net.openhft.chronicle.engine.api.SessionDetailsProvider;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by peter on 01/06/15.
 */
public class VanillaSessionDetails implements SessionDetailsProvider {
    private final Map<Class, Object> infoMap = new LinkedHashMap<>();
    private String userId, securityToken;
    private InetSocketAddress connectionAddress;
    private long connectTimeMS;
    private UUID sessionId;

    public VanillaSessionDetails() {
        this.sessionId = UUID.randomUUID();
    }

    /***
     * used to uniquely identify the session
     */
    @Override
    public UUID sessionId() {
        return sessionId;
    }

    @Override
    public String userId() {
        return userId;
    }

    @Override
    public String securityToken() {
        return securityToken;
    }

    @Override
    public InetSocketAddress connectionAddress() {
        return connectionAddress;
    }

    @Override
    public long connectTimeMS() {
        return connectTimeMS;
    }

    @Override
    public <I> void set(Class<I> infoClass, I info) {
        infoMap.put(infoClass, info);
    }

    @Override
    public <I> I get(Class<I> infoClass) {
        return (I) infoMap.get(infoClass);
    }

    public void setConnectTimeMS(long connectTimeMS) {
        this.connectTimeMS = connectTimeMS;
    }

    public void setConnectionAddress(InetSocketAddress connectionAddress) {
        this.connectionAddress = connectionAddress;
    }

    public void setSecurityToken(String securityToken) {
        this.securityToken = securityToken;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
