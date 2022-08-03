/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.threads.Pauser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

final class StateMachineProcessor implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateMachineProcessor.class);

    private final SSLContext context;
    private final SslEngineStateMachine stateMachine;
    private final Pauser pauser = Pauser.balanced();
    private final ChronicleSocketChannel channel;
    private volatile boolean running = true;

    StateMachineProcessor(final ChronicleSocketChannel channel,
                          final boolean isAcceptor,
                          final SSLContext context,
                          final BufferHandler bufferHandler) {
        this.context = context;
        this.channel = channel;
        stateMachine = new SslEngineStateMachine(bufferHandler, isAcceptor);
    }

    @Override
    public void run() {
        try {
            stateMachine.initialise(context, channel);

            while (!Thread.currentThread().isInterrupted() && running) {
                while (running && stateMachine.action()) {
                    pauser.reset();
                }
                pauser.pause();
            }
        } catch (Throwable e) {
            if (running)
                LOGGER.error("Exception caught while processing SSL state machine. Exiting.", e);
        }
    }

    public void stop() {
        this.running = false;
    }
}
