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

import net.openhft.chronicle.network.connection.SocketAddressSupplier;
import org.jetbrains.annotations.NotNull;

public class ConnectionDetails extends VanillaNetworkContext {
    private boolean isConnected;
    private String id;
    private SocketAddressSupplier socketAddressSupplier;
    private boolean disable;

    public ConnectionDetails(@NotNull String id, String hostPort) {
        this(id, new SocketAddressSupplier(new String[]{hostPort}, id));
    }

    public ConnectionDetails(String id, SocketAddressSupplier socketAddressSupplier) {
        this.id = id;
        this.socketAddressSupplier = socketAddressSupplier;
        sessionDetails(new VanillaSessionDetails());
        sessionDetails().userId(id);
    }

    public SocketAddressSupplier sessionProvider() {
        return socketAddressSupplier;
    }

    @NotNull
    public ConnectionDetails sessionProvider(SocketAddressSupplier sessionProvider) {
        this.socketAddressSupplier = sessionProvider;
        return this;
    }

    public String getID() {
        return id;
    }

    boolean isConnected() {
        return isConnected;
    }

    void setConnected(boolean connected) {
        isConnected = connected;
    }

    public boolean isDisable() {
        return disable;
    }

    public void setDisable(boolean disable) {
        this.disable = disable;
    }

    @NotNull
    @Override
    public String toString() {
        return "ConnectionDetails{" +
                "isConnected=" + isConnected +
                ", id='" + id + '\'' +
                ", scocketAddressSupplier='" + socketAddressSupplier + '\'' +
                ", disable=" + disable +
                '}';
    }
}
