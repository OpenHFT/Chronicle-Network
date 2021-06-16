package net.openhft.chronicle.network.tcp;

import net.openhft.chronicle.network.connection.TcpChannelHub;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

public enum ChronicleSocketFactory {
    ;

    public static ChronicleSocket toChronicleSocket(Socket socket) {
        return new ChronicleSocket() {

            @Override
            public void setTcpNoDelay(final boolean tcpNoDelay) throws SocketException {
                TcpChannelHub.setTcpNoDelay(socket, tcpNoDelay);
            }

            @Override
            public int getReceiveBufferSize() throws SocketException {
                return socket.getReceiveBufferSize();
            }

            @Override
            public void setReceiveBufferSize(final int tcpBuffer) throws SocketException {
                socket.setReceiveBufferSize(tcpBuffer);
            }

            @Override
            public int getSendBufferSize() throws SocketException {
                return socket.getSendBufferSize();
            }

            @Override
            public void setSendBufferSize(final int tcpBuffer) throws SocketException {
                socket.setSendBufferSize(tcpBuffer);
            }

            @Override
            public void setSoTimeout(final int i) throws SocketException {
                socket.setSoTimeout(i);
            }

            @Override
            public void setSoLinger(final boolean b, final int i) throws SocketException {
                socket.setSoLinger(b, i);
            }

            @Override
            public void shutdownInput() throws IOException {
                socket.shutdownInput();
            }

            @Override
            public void shutdownOutput() throws IOException {
                socket.shutdownOutput();
            }

            @Override
            public Object getRemoteSocketAddress() {
                return socket.getRemoteSocketAddress();
            }

            @Override
            public int getLocalPort() {
                return socket.getLocalPort();
            }

        }

                ;
    }
}
