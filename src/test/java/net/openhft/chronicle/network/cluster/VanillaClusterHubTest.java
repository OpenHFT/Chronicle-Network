package net.openhft.chronicle.network.cluster;

import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.threads.EventGroup;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by peter.lawrey on 31/01/2016.
 */
public class VanillaClusterHubTest {

    @Test
    public void testConnectionFor() throws IOException {
        TCPRegistry.createSSC("node1", new InetSocketAddress("localhost", 9191));
        TCPRegistry.createSSC("node2", new InetSocketAddress("localhost", 9292));

        EventGroup group1 = new EventGroup(true);
        ClusterHub hub1 = VanillaClusterHub.createServerNode(group1, "node1");
        hub1.setActorStart(Integer.MIN_VALUE, "node1");
        hub1.setActorStart(0, "node2");

        EventGroup group2 = new EventGroup(true);
        ClusterHub hub2 = VanillaClusterHub.createServerNode(group2, "node2");
        hub2.setActorStart(Integer.MIN_VALUE, "node1");
        hub2.setActorStart(0, "node2");

        ClusterConnection self1 = hub1.connectionFor(-12345);
        self1.asyncMessage(-12345, "cid1", () -> "test", v -> v.text("hello"));

        ClusterConnection node21 = hub1.connectionFor(12345);
        node21.asyncMessage(12345, "cid2", () -> "test", v -> v.text("hello2"));
    }
}