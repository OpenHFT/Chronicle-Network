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

package net.openhft.performance.tests.third.party.frameworks.mina;

/**
 * @author Rob Austin.
 */

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MinaEchoServer {

    static final int PORT = Integer.getInteger("port", 9120);

    public static void main(String[] args) throws IOException {

        @NotNull final IoAcceptor acceptor = new NioSocketAcceptor();

        acceptor.setHandler(new IoHandlerAdapter() {
            @Override
            public void exceptionCaught(IoSession session, @NotNull Throwable cause) {
                cause.printStackTrace();
            }

            @Override
            public void messageReceived(@NotNull IoSession session, Object message) {
                session.write(message);
            }

            @Override
            public void sessionIdle(IoSession session, IdleStatus status) {

            }
        });

        acceptor.getSessionConfig().setReadBufferSize(2048);
        acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);
        acceptor.bind(new InetSocketAddress(PORT));
    }
}