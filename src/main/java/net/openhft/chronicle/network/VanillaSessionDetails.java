/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network;

import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by peter on 01/06/15.
 */
public class VanillaSessionDetails implements SessionDetailsProvider {
    private final Map<Class, Object> infoMap = new LinkedHashMap<>();
    private String userId = "";
    private String securityToken = "";
    private String domain = "";
    private SessionMode sessionMode = SessionMode.ACTIVE;
    private UUID clientId;

    // only set on a server not on a client
    private InetSocketAddress clientAddress;
    private long connectTimeMS;
    private UUID sessionId;
    @Nullable
    private WireType wireType;

    private byte hostId;

    public VanillaSessionDetails() {
    }

    @NotNull
    public static VanillaSessionDetails of(String userId, String securityToken, String domain) {
        @NotNull final VanillaSessionDetails vanillaSessionDetails = new VanillaSessionDetails();
        vanillaSessionDetails.userId(userId);
        vanillaSessionDetails.securityToken(securityToken);
        vanillaSessionDetails.domain(domain);
        return vanillaSessionDetails;
    }

    /***
     * used to uniquely identify the session
     */
    @Override
    public UUID sessionId() {
        if (sessionId == null)
            sessionId = UUID.randomUUID();
        return sessionId;
    }

    @Override
    public UUID clientId() {
        if (clientId == null)
            clientId = UUID.randomUUID();
        return clientId;
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
    public String domain() {
        return this.domain;
    }

    @Override
    public SessionMode sessionMode() {
        return sessionMode;
    }

    @Override
    public void domain(String domain) {
        this.domain = domain;
    }

    @Override
    public void sessionMode(SessionMode sessionMode) {
        this.sessionMode = sessionMode;
    }

    @Override
    public void clientId(UUID clientId) {
        this.clientId = clientId;
    }

    @Override
    public void wireType(@Nullable WireType wireType) {
        this.wireType = wireType;
    }

    @Nullable
    @Override
    public WireType wireType() {
        return wireType;
    }

    public void hostId(byte hostId) {
        this.hostId = hostId;
    }

    @Override
    public byte hostId() {
        return this.hostId;
    }

    @Override
    public InetSocketAddress clientAddress() {
        return clientAddress;
    }

    @Override
    public long connectTimeMS() {
        return connectTimeMS;
    }

    @Override
    public <I> void set(Class<I> infoClass, I info) {
        infoMap.put(infoClass, info);
    }

    @NotNull
    @Override
    public <I> I get(Class<I> infoClass) {
        return (I) infoMap.get(infoClass);
    }

    @Override
    public void connectTimeMS(long connectTimeMS) {
        this.connectTimeMS = connectTimeMS;
    }

    @Override
    public void clientAddress(InetSocketAddress clientAddress) {
        this.clientAddress = clientAddress;
    }

    @Override
    public void securityToken(String securityToken) {
        this.securityToken = securityToken;
    }

    @Override
    public void userId(String userId) {
        this.userId = userId;
    }

    @NotNull
    @Override
    public String toString() {
        return "VanillaSessionDetails{" +
                "infoMap=" + infoMap +
                ", userId='" + userId + '\'' +
                ", securityToken='" + securityToken + '\'' +
                ", clientAddress=" + clientAddress +
                ", connectTimeMS=" + connectTimeMS +
                ", sessionId=" + sessionId +
                ", sessionMode=" + sessionMode +
                ", domain=" + domain +
                ", clientId=" + clientId +
                ", wiretype=" + wireType +
                ", hostId=" + hostId +
                '}';
    }
}
