= Chronicle Network
Chronicle Software
:css-signature: demo
:toc: macro
:toclevels: 2
:icons: font

image:https://maven-badges.herokuapp.com/maven-central/net.openhft/chronicle-network/badge.svg[caption="",link=https://maven-badges.herokuapp.com/maven-central/net.openhft/chronicle-network]
image:https://javadoc.io/badge2/net.openhft/chronicle-network/javadoc.svg[link="https://www.javadoc.io/doc/net.openhft/chronicle-network/latest/index.html"]
//image:https://javadoc-badge.appspot.com/net.openhft/chronicle-network.svg?label=javadoc[JavaDoc, link=https://www.javadoc.io/doc/net.openhft/chronicle-network]
image:https://img.shields.io/github/license/OpenHFT/Chronicle-Network[GitHub]
image:https://img.shields.io/badge/release%20notes-subscribe-brightgreen[link="https://chronicle.software/release-notes/"]
image:https://sonarcloud.io/api/project_badges/measure?project=OpenHFT_Chronicle-Network&metric=alert_status[link="https://sonarcloud.io/dashboard?id=OpenHFT_Chronicle-Network"]

toc::[]

== About

Chronicle Network is a high performance network library.

== Purpose

This library is designed to be lower latency and support higher throughput
 by employing techniques used in low latency trading systems.

== Transports

Chronicle Network uses TCP.

Planned support for

* Shared Memory

UDP support can be found in Chronicle Network Enterprise (commercial product - contact sales@chronicle.software)

== Example

=== TCP Client/Server : Echo Example

The client sends a message to the server, the server immediately responds with the same message
back to the client.
The full source code of this example can be found at:

[source,java]
----
net.openhft.performance.tests.network.SimpleServerAndClientTest.test

----

Below are some of the key parts of this code explained, in a bit more detail.

==== TCPRegistry

The `TCPRegistry` is most useful for unit tests, it allows you to either provide a true host and port, say "localhost:8080"
or if you would rather let the application allocate you a free port at random, you can just provide a text reference to the port,
such as, "host.port", you can provide any text you want. It will always be taken as a reference.
That is unless it's correctly formed like "hostname:port", then it will use the exact host and port you provide.
The reason we offer this functionality is quite often in unit tests you wish to start a test via loopback,
followed often by another test, if the first test does not shut down correctly it can impact on the
second test. Giving each test a unique port is one solution, but then managing those ports can become a problem
in itself. So we created the `TCPRegistry` which manages those ports for you, when you come to clean up at the end
of each test, all you have to do is call `TCPRegistry.reset()` and this will ensure that any open ports, will be closed.

[source,java]
----
// This is the name of a reference to the host name and port,
// allocated automatically to a free port on localhost
final String desc = "host.port";
TCPRegistry.createServerSocketChannelFor(desc);

// We use an event loop rather than lots of threads
EventLoop eg = EventGroup.builder().build();
eg.start();
----

==== Create and Start the Server

The server is configured with `TextWire`, so
the client must also be configured with `TextWire`. The port that we will use will be ( in this example ) determined
by the `TCPRegistry`, of course in a real life production environment you may decide not to use the
`TCPRegistry` or if you still use the `TCPRegistry` you can use a fixed host:port.

[source,java]
----
final String expectedMessage = "<my message>";
AcceptorEventHandler eah = new AcceptorEventHandler(desc,
    () -> new WireEchoRequestHandler(WireType.TEXT), VanillaSessionDetails::new, 0, 0);
eg.addHandler(eah);
final SocketChannel sc = TCPRegistry.createSocketChannel(desc);
sc.configureBlocking(false);
----

==== Server Message Processing

The server code that processes a message:

In this simple example we receive and update a message and then immediately send back a response, however there are
other solutions that can be implemented using Chronicle Network, such as the server
responding later to a client subscription.

[source,java]
----
/**
 * This code is used to read the tid and payload from a wire message,
 * and send the same tid and message back to the client
 */
public class WireEchoRequestHandler extends WireTcpHandler {

    public WireEchoRequestHandler(@NotNull Function<Bytes<?>, Wire> bytesToWire) {
        super(bytesToWire);
    }

    /**
     * Simply reads the csp,tid and payload and sends back the tid and payload
     *
     * @param inWire  the wire from the client
     * @param outWire the wire to be sent back to the server
     * @param sd      details about this session
     */
    @Override
    protected void process(@NotNull WireIn inWire,
                           @NotNull WireOut outWire,
                           @NotNull SessionDetailsProvider sd) {

        inWire.readDocument(m -> {
            outWire.writeDocument(true, meta -> meta.write("tid")
                    .int64(inWire.read("tid").int64()));
        }, d -> {
            outWire.writeDocument(false, data -> data.write("payloadResponse")
                    .text(inWire.read("payload").text()));
        });
    }
}
----

==== Create and Start the Client

The client code that creates the `TcpChannelHub`:

The `TcpChannelHub` is used to send your messages to the server and then read the servers response.
The `TcpChannelHub` ensures that each response is marshalled back onto the appropriate client thread.
It does this through the use of a unique transaction ID ( we call this transaction ID the "tid" ),
 when the server responds to the client, its expected that the server sends back the tid as the very first field in the message.
The `TcpChannelHub` will look at each message and read the tid, and then marshall the message
onto your appropriate client thread.

[source,java]
----
TcpChannelHub tcpChannelHub = TcpChannelHub(null, eg, WireType.TEXT, "",
    SocketAddressSupplier.uri(desc), false);
----

In this example we are not implementing fail-over support, so the simple `SocketAddressSupplier.uri(desc)` is used.

==== Client Message

Creates the message the client sends to the server

[source,java]
----
// The tid must be unique, its reflected back by the server, it must be at the start
// of each message sent from the server to the client. Its use by the client to identify which
// thread will handle this message
final long tid = tcpChannelHub.nextUniqueTransaction(System.currentTimeMillis());

// We will use a text wire backed by a elasticByteBuffer
final Wire wire = new TextWire(Bytes.elasticByteBuffer());

wire.writeDocument(true, w -> w.write("tid").int64(tid));
wire.writeDocument(false, w -> w.write("payload").text(expectedMessage));
----

==== Write the Data to the Socket

When you have multiple client threads it's important to lock before writing the data to the socket.

[source,java]
----
tcpChannelHub.lock(() -> tcpChannelHub.writeSocket(wire));
----

==== Read the Reply from the Server

In order that the correct reply can be sent to your thread you have to specify the tid.

[source,java]
----
Wire reply = tcpChannelHub.proxyReply(TimeUnit.SECONDS.toMillis(1), tid);
----

==== Check the Result of the Reply

[source,java]
----
// Reads the reply and check the result
reply.readDocument(null, data -> {
    final String text = data.read("payloadResponse").text();
    Assert.assertEquals(expectedMessage, text);
});
----

==== Shutdown and Cleanup

[source,java]
----
eg.stop();
TcpChannelHub.closeAllHubs();
TCPRegistry.reset();
tcpChannelHub.close();
----

== Server Threading Strategy

By default the Chronicle Network server uses a single thread to process all messages.
However, if you wish to dedicate each client connection to its own thread,
then you can change the server threading strategy, to:

----
-DServerThreadingStrategy=CONCURRENT
----

see the following enum for more details `net.openhft.chronicle.network.ServerThreadingStrategy`

== Java Version

This library requires Java 8 or Java 11.

== Testing

The target environment is to support TCP over 10 Gigabit Ethernet. In prototype
testing, this library has half the latency and supports 30% more bandwidth.

A key test is that it shouldn't GC more than once (to allow for warm up) with -mx64m.

== Downsides

This comes at the cost of scalability for large number of connections.
In this situation, this library should perform at least as well as Netty.

== Comparisons

=== Netty

Netty has a much wider range of functionality, however it creates some
garbage in its operation (less than using plain NIO Selectors) and isn't
designed to support busy waiting which gives up a small but significant delay.
