package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.threads.Pauser;

import javax.net.ssl.SSLContext;
import java.nio.channels.SocketChannel;

public final class StateMachineProcessor implements Runnable {
    private final SSLContext context;
    private final SslEngineStateMachine stateMachine;
    private final Pauser pauser = Pauser.balanced();
    private final SocketChannel channel;
    private volatile boolean running = true;

    public StateMachineProcessor(final SocketChannel channel, final boolean isAcceptor,
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
                    // process loop
                    pauser.reset();
                }
                pauser.pause();
            }

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        this.running = false;
    }
}
