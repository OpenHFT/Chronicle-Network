/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;

import java.io.StreamCorruptedException;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public abstract class WireTcpHandler implements TcpHandler {
    public static final int SIZE_OF_SIZE = 4;
    protected Wire inWire, outWire;

    @Override
    public void process(Bytes in, Bytes out) {
        checkWires(in, out);

        if (in.remaining() < SIZE_OF_SIZE) {
            long outPos = out.position();
            out.skip(SIZE_OF_SIZE);
            publish(outWire);

            long written = out.position() - outPos - SIZE_OF_SIZE;
            if (written == 0) {
                out.position(outPos);
                return;
            }
            assert written < 1 << 16;
            out.writeUnsignedInt(outPos, (int) written);
            return;
        }


        do {

            if (!processMessage(in, out))
                return;

        } while (in.remaining() > SIZE_OF_SIZE && out.remaining() > out.capacity() / SIZE_OF_SIZE);
    }


    /**
     * process all messages in this batch, provided there is plenty of output space.
     *
     * @param in  the source bytes
     * @param out the destination bytes
     * @return true if we can read attempt the next
     */
    private boolean processMessage(Bytes in, Bytes out) {

        long length = in.readUnsignedInt(in.position());

        assert length >= 0;

        if (length == 0) {
            in.skip(SIZE_OF_SIZE);
            return false;
        }

        if (in.remaining() < length + SIZE_OF_SIZE)
            // we have to first read more data befor this can be processed
            return false;
        else {

            in.skip(SIZE_OF_SIZE);
            long limit = in.limit();
            long end = in.position() + length;
            long outPos = out.position();
            try {
                out.skip(SIZE_OF_SIZE);
                in.limit(end);

                final long position = inWire.bytes().position();
                try {
                    process(inWire, outWire);
                } finally {
                    inWire.bytes().position(position + length);
                }

                long written = out.position() - outPos - SIZE_OF_SIZE;

                if (written == 0) {
                    out.position(outPos);

                    return false;
                }

                out.writeUnsignedInt(outPos, (int) written);

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                in.limit(limit);
                in.position(end);
            }


        }
        return true;
    }

    private boolean recreateWire;

    protected void recreateWire(boolean recreateWire) {
        this.recreateWire = recreateWire;
    }

    private void checkWires(Bytes in, Bytes out) {

        if (recreateWire) {
            recreateWire = false;
            inWire = createWriteFor(in);
            outWire = createWriteFor(out);
            return;
        }

        if ((inWire == null || inWire.bytes() != in)) {
            inWire = createWriteFor(in);
            recreateWire = false;
        }

        if ((outWire == null || outWire.bytes() != out)) {
            outWire = createWriteFor(out);
            recreateWire = false;
        }
    }

    protected Wire createWriteFor(Bytes bytes) {
        return new TextWire(bytes);
    }

    /**
     * Process an incoming request
     */

    /**
     * @param in the wire to be processed
     * @param out the result of processing the {@code in}
     * @throws StreamCorruptedException if the wire is corrupt
     */
    protected abstract void process(Wire in, Wire out) throws StreamCorruptedException;

    /**
     * Publish some data
     */
    protected void publish(Wire out) {
    }
}
