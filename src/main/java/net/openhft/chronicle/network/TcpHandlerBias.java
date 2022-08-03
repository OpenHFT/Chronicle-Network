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

package net.openhft.chronicle.network;

import java.util.function.Supplier;

public enum TcpHandlerBias implements Supplier<TcpHandlerBias.BiasController> {
    /**
     * Defines a read-bias for TcpEventHandler. This will prioritise reads, and will only do 1 write per 8 reads
     */
    READ {
        @Override
        public BiasController get() {
            return new ReadBiasController();
        }
    },
    /**
     * Defines a fair TcpEventHandler biasing, with 1 write per each read
     */
    FAIR {
        @Override
        public BiasController get() {
            return new FairBiasController();
        }
    },
    /**
     * Defines a write-bias for TcpEventHandler. This will prioritise writes, and will only do 1 read per 8 writes
     */
    WRITE {
        @Override
        public BiasController get() {
            return new WriteBiasController();
        }
    };

    interface BiasController {

        /**
         * Returns if a read operation is permitted according
         * to this BiasController.
         *
         * @return if a read operation is permitted according
         *         to this BiasController
         */
        boolean canRead();

        /**
         * Returns if a write operation is permitted according
         * to this BiasController.
         *
         * @return if a write operation is permitted according
         *         to this BiasController
         */
        boolean canWrite();
    }

    private static final int RATIO = 8;

    private static final class ReadBiasController implements BiasController {
        private int reads = 0;

        @Override
        public boolean canRead() {
            reads++;
            return true;
        }

        @Override
        public boolean canWrite() {
            if (reads >= RATIO) {
                reads = 0;
                return true;
            }
            return false;
        }
    }

    private static final class WriteBiasController implements BiasController {
        private int writes = 0;

        @Override
        public boolean canWrite() {
            writes++;
            return true;
        }

        @Override
        public boolean canRead() {
            if (writes >= RATIO) {
                writes = 0;
                return true;
            }
            return false;
        }
    }

    private static final class FairBiasController implements BiasController {
        @Override
        public boolean canRead() {
            return true;
        }

        @Override
        public boolean canWrite() {
            return true;
        }
    }
}