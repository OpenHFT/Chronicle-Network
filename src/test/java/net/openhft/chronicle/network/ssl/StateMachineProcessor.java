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

    StateMachineProcessor(final ChronicleSocketChannel channel, final boolean isAcceptor,
                          final SSLContext context, final BufferHandler bufferHandler) {
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
