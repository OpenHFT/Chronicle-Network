/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.network;

import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by peter on 01/06/15.
 */
public class VanillaSessionDetails implements SessionDetailsProvider {
    private final Map<Class, Object> infoMap = new LinkedHashMap<>();
    private String userId = System.getProperty("user.name");
    private String securityToken = "";

    // only set on a server not on a client
    private InetSocketAddress clientAddress;
    private long connectTimeMS;
    private UUID sessionId;

    public VanillaSessionDetails() {
    }

    @NotNull
    public static VanillaSessionDetails of(String userId, String securityToken) {
        final VanillaSessionDetails vanillaSessionDetails = new VanillaSessionDetails();
        vanillaSessionDetails.setUserId(userId);
        vanillaSessionDetails.setSecurityToken(securityToken);
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
    public String userId() {
        return userId;
    }

    @Override
    public String securityToken() {
        return securityToken;
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

    public void setConnectTimeMS(long connectTimeMS) {
        this.connectTimeMS = connectTimeMS;
    }

    public void setClientAddress(InetSocketAddress clientAddress) {
        this.clientAddress = clientAddress;
    }

    public void setSecurityToken(String securityToken) {
        this.securityToken = securityToken;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
