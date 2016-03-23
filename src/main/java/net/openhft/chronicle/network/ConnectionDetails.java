/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.network;

/**
 * Created by daniel on 10/02/2016.
 */
public class ConnectionDetails extends VanillaNetworkContext {
    private boolean isConnected;
    private String id;
    private String hostNameDescription;
    private boolean disable;

    public ConnectionDetails(String id, String hostNameDescription) {
        this.id = id;
        this.hostNameDescription = hostNameDescription;
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

    public String getHostNameDescription() {
        return hostNameDescription;
    }

    public void setHostNameDescription(String hostNameDescription) {
        this.hostNameDescription = hostNameDescription;
    }

    public boolean isDisable() {
        return disable;
    }

    public void setDisable(boolean disable) {
        this.disable = disable;
    }

    @Override
    public String toString() {
        return "ConnectionDetails{" +
                "isConnected=" + isConnected +
                ", id='" + id + '\'' +
                ", hostNameDescription='" + hostNameDescription + '\'' +
                ", disable=" + disable +
                '}';
    }
}
