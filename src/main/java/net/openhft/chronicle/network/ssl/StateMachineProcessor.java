package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.threads.Pauser;

import javax.net.ssl.SSLContext;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

public final class StateMachineProcessor implements Runnable {
    private final SocketChannel channel;
    private final SSLContext context;
    private final SslEngineStateMachine stateMachine;
    private final Pauser pauser = Pauser.balanced();
    private volatile boolean running = true;

    public StateMachineProcessor(final SocketChannel channel, final boolean isAcceptor,
                                 final Consumer<ByteBuffer> decodedMessageReceiver,
                                 final Consumer<ByteBuffer> applicationDataPopulator,
                                 final SSLContext context) {
        this.channel = channel;
        this.context = context;
        stateMachine = new SslEngineStateMachine(channel, decodedMessageReceiver,
                applicationDataPopulator, isAcceptor);
    }

    @Override
    public void run() {
        try {
            while (!channel.finishConnect()) {
                Thread.yield();
            }

            stateMachine.initialise(context);

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
