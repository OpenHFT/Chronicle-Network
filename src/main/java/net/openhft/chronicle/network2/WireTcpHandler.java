package net.openhft.chronicle.network2;

import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.lang.io.Bytes;

import java.io.StreamCorruptedException;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public abstract class WireTcpHandler implements TcpHandler {
    protected Wire inWire, outWire;

    @Override
    public void process(Bytes in, Bytes out) {
        checkWires(in, out);
        if (in.remaining() < 2) {
            publish(outWire);
            return;
        }
        // process all messages in this batch, provided there is plenty of output space.
        do {
            int length = in.readUnsignedShort(in.position());
            if (in.remaining() >= length + 2) {
                in.skip(2);
                long limit = in.limit();
                long end = in.position() + length;
                long outPos = out.position();
                try {
                    out.skip(2);
                    in.limit(end);
                    process(inWire, outWire);
                    long written = out.position() - outPos - 2;
                    assert written < 1 << 16;
                    out.writeUnsignedShort(outPos, (int) written);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    in.limit(limit);
                    in.position(end);
                }
            }
        } while (in.remaining() >= 2 && out.remaining() > out.capacity() / 2);
    }

    private void checkWires(Bytes in, Bytes out) {
        if (inWire == null || inWire.bytes() != in)
            inWire = createWriteFor(in);
        if (outWire == null || outWire.bytes() != out)
            outWire = createWriteFor(out);
    }

    protected Wire createWriteFor(Bytes bytes) {
        return new TextWire(bytes);

    }

    /**
     * Process an incoming request
     */

    protected abstract void process(Wire in, Wire out) throws StreamCorruptedException;

    /**
     * Publish some data
     */
    protected void publish(Wire out) {
    }
}
