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
    protected Wire inWire, outWire;

    @Override
    public void process(Bytes in, Bytes out) {
        checkWires(in, out);

        if (in.remaining() < 2) {
            long outPos = out.position();
            out.skip(2);
            publish(outWire);

            long written = out.position() - outPos - 2;
            if (written == 0) {
                out.position(outPos);
                return;
            }
            assert written < 1 << 16;
            out.writeUnsignedShort(outPos, (int) written);
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

                    final long position = inWire.bytes().position();
                    try {
                        process(inWire, outWire);
                    } finally {
                        inWire.bytes().position(position + length);
                    }


                    long written = out.position() - outPos - 2;

                    if (written == 0) {
                        out.position(outPos);
                        return;
                    }


                    assert written < 1024;
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
