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