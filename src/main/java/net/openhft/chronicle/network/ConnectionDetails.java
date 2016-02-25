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

    public String getHostNameDescription() {
        return hostNameDescription;
    }

    public void setHostNameDescription(String hostNameDescription) {
        this.hostNameDescription = hostNameDescription;
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
