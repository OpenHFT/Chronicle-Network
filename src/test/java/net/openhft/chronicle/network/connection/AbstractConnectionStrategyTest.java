package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.network.NetworkTestCommon;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.net.*;
import java.util.Enumeration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class AbstractConnectionStrategyTest extends NetworkTestCommon {

    @Test
    void testLocalBindingFailsWhenUnknownInterfaceSpecified() throws SocketException, UnknownHostException {
        final ConcreteConnectionStrategy concreteConnectionStrategy = new ConcreteConnectionStrategy();
        concreteConnectionStrategy.localBindingNetworkInterface("nonExistentInterface");
        try {
            concreteConnectionStrategy.localSocketBinding();
            fail("Should have thrown exception");
        } catch (IllegalStateException e) {
            // expected
            assertTrue(e.getMessage().startsWith("No matching interface found for name nonExistentInterface, available interfaces:"));
        }
    }

    @Test
    void testLocalBindingFailsWhenUnknownHostnameSpecified() {
        final ConcreteConnectionStrategy concreteConnectionStrategy = new ConcreteConnectionStrategy();
        concreteConnectionStrategy.localSocketBindingHost("nonExistentHostName-" + UUID.randomUUID());
        assertThrows(UnknownHostException.class, concreteConnectionStrategy::localSocketBinding);
    }

    @Test
    void testLocalBindingWarnsWhenInterfaceAndHostnameAreSpecified() {
        final ConcreteConnectionStrategy concreteConnectionStrategy = new ConcreteConnectionStrategy();
        concreteConnectionStrategy.localBindingNetworkInterface("nonExistentInterface" + UUID.randomUUID());
        concreteConnectionStrategy.localSocketBindingHost("nonExistentHostName-" + UUID.randomUUID());
        expectException("You have specified both localSocketBindingHost and localBindingNetworkInterface, using localSocketBindingHost");

        assertThrows(UnknownHostException.class, concreteConnectionStrategy::localSocketBinding);
    }

    @Test
    void testWillWarnWhenInterfaceBindingIsAmbiguous() throws SocketException, UnknownHostException {
        final NetworkInterface interfaceWithMultipleAddresses = findInterfaceWithMultipleAddresses();
        assumeTrue(interfaceWithMultipleAddresses != null, "no interface with multiple addresses found");

        final ConcreteConnectionStrategy concreteConnectionStrategy = new ConcreteConnectionStrategy();
        concreteConnectionStrategy.localBindingNetworkInterface(interfaceWithMultipleAddresses.getName());

        assertNotNull(concreteConnectionStrategy.localSocketBinding());
        expectException("Multiple eligible addresses available on interface/protocol");
    }

    @Test
    void testWillWarnWhenHostnameBindingIsAmbiguous() throws SocketException, UnknownHostException {
        assumeTrue(InetAddress.getAllByName("localhost").length > 1, "localhost only maps to a single address");

        final ConcreteConnectionStrategy concreteConnectionStrategy = new ConcreteConnectionStrategy();
        concreteConnectionStrategy.localSocketBindingHost("localhost");

        assertNotNull(concreteConnectionStrategy.localSocketBinding());
        expectException("Multiple eligible addresses available for hostname/protocol localhost/ANY");
    }

    @Test
    void testWillHonourLocalBindingProtocol() throws SocketException, UnknownHostException {
        final NetworkInterface interfaceWithInet4AndInet6Addresses = findInterfaceWithInet4AndInet6Addresses();
        assumeTrue(interfaceWithInet4AndInet6Addresses != null, "no interface with inet4 and inet6 addresses");
        ignoreException("Multiple eligible addresses available on interface/protocol");

        final ConcreteConnectionStrategy concreteConnectionStrategy = new ConcreteConnectionStrategy();
        concreteConnectionStrategy.localBindingNetworkInterface(interfaceWithInet4AndInet6Addresses.getName());

        concreteConnectionStrategy.localBindingProtocolFamily(StandardProtocolFamily.INET);
        final InetSocketAddress inet4Binding = concreteConnectionStrategy.localSocketBinding();
        assertTrue(inet4Binding.getAddress() instanceof Inet4Address);

        concreteConnectionStrategy.localBindingProtocolFamily(StandardProtocolFamily.INET6);
        final InetSocketAddress inet6Binding = concreteConnectionStrategy.localSocketBinding();
        assertTrue(inet6Binding.getAddress() instanceof Inet6Address);
    }

    @Test
    void testWillReturnNullWhenNoHostnameOrInterfaceSpecified() throws SocketException, UnknownHostException {
        assertNull(new ConcreteConnectionStrategy().localSocketBinding());
    }

    @Test
    void testWillHonourConfiguredPort() throws SocketException, UnknownHostException {
        assumeTrue(InetAddress.getAllByName("localhost").length > 0, "localhost not bound to any address?");
        ignoreException("Multiple eligible addresses available for hostname/protocol");

        final ConcreteConnectionStrategy concreteConnectionStrategy = new ConcreteConnectionStrategy();
        concreteConnectionStrategy.localSocketBindingHost("localhost");
        concreteConnectionStrategy.localSocketBindingPort(9876);

        final InetSocketAddress binding = concreteConnectionStrategy.localSocketBinding();
        assertNotNull(binding);
        assertEquals(9876, binding.getPort());
    }

    @Nullable
    private NetworkInterface findInterfaceWithInet4AndInet6Addresses() throws SocketException, UnknownHostException {
        final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            final NetworkInterface networkInterface = networkInterfaces.nextElement();
            if (hasInet4AndInet6Bindings(networkInterface)) {
                return networkInterface;
            }
        }
        return null;
    }

    private boolean hasInet4AndInet6Bindings(NetworkInterface networkInterface) throws UnknownHostException {
        boolean seenInet4 = false;
        boolean seenInet6 = false;
        final Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
        while (inetAddresses.hasMoreElements()) {
            final InetAddress inetAddress = inetAddresses.nextElement();
            seenInet4 = seenInet4 || inetAddress instanceof Inet4Address;
            seenInet6 = seenInet6 || inetAddress instanceof Inet6Address;
        }
        return seenInet4 && seenInet6;
    }

    private static class ConcreteConnectionStrategy extends AbstractConnectionStrategy {
        @Override
        public ChronicleSocketChannel connect(@NotNull String name, @NotNull SocketAddressSupplier socketAddressSupplier, boolean didLogIn, @NotNull FatalFailureMonitor fatalFailureMonitor) throws InterruptedException {
            // Do nothing
            return null;
        }
    }

    @Nullable
    private NetworkInterface findInterfaceWithMultipleAddresses() throws SocketException {
        final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            final NetworkInterface networkInterface = networkInterfaces.nextElement();
            final Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
            int count = 0;
            while (inetAddresses.hasMoreElements()) {
                inetAddresses.nextElement();
                count++;
            }
            if (count > 1) {
                return networkInterface;
            }
        }
        return null;
    }
}