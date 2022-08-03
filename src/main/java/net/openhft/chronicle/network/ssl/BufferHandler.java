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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class handles the bridge between two sets of buffers:
 * - socket-side data - encrypted, handled by <code>readData</code>, <code>writeData</code>
 * - application-side data - decrypted, handled by <code>handleDecryptedData</code>
 */
public interface BufferHandler {
    /**
     * Read encrypted data from an input into the supplied buffer.
     *
     * @param target the target buffer for encrypted data
     * @return the number of bytes written
     * @throws IOException if an exception occurs
     */
    int readData(final ByteBuffer target) throws IOException;

    /**
     * Accept decrypted input data, previous collected by an invocation of <code>readData</code>.
     * Any plain-text output should be written to the output buffer for encryption.
     *
     * @param input  a buffer containing decrypted input data
     * @param output a buffer that can be used for writing plain-text output
     */
    void handleDecryptedData(final ByteBuffer input, final ByteBuffer output);

    /**
     * Write encrypted data to an output from the supplied buffer.
     *
     * @param encrypted the buffer containing encrypted data
     * @return the number of bytes written
     * @throws IOException if an exception occurs
     */
    int writeData(final ByteBuffer encrypted) throws IOException;
}
