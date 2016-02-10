package net.openhft.chronicle.network;

/**
 * Created by daniel on 10/02/2016.
 */
public class ConnectionDetails {
    private boolean isConnected;
    private String name;
    private String hostNameDescription;
    private boolean disable;

    public ConnectionDetails(String name, String hostNameDescription) {
        this.name = name;
        this.hostNameDescription = hostNameDescription;
    }

    public String getName() {
        return name;
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
}
