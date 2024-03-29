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

package net.openhft.chronicle.network.internal.lookuptable;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.*;
import net.openhft.chronicle.network.HostnamePortLookupTable;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static net.openhft.chronicle.core.util.Time.sleep;

/**
 * Stores the mappings in a shared file, will work across processes
 */
public class FileBasedHostnamePortLookupTable extends AbstractCloseable implements HostnamePortLookupTable, java.io.Closeable, ReferenceOwner {

    private static final long MINIMUM_INITIAL_FILE_SIZE_BYTES = 1_024L * 512L; // We want to prevent resizing
    private static final long LOCK_TIMEOUT_MS = 10_000;
    private static final int DELETE_TABLE_FILE_TIMEOUT_MS = 1_000;
    private static final int PID = Jvm.getProcessId();
    private static final String DEFAULT_FILE_NAME = "shared_hostname_mappings";
    private static final ReentrantLock PROCESS_FILE_LOCK = new ReentrantLock();

    private final JSONWire sharedTableWire;
    private final MappedBytes sharedTableBytes;
    private final File sharedTableFile;
    private final long actualBytesSize;
    private final ConcurrentSkipListMap<String, ProcessScopedMapping> allMappings = new ConcurrentSkipListMap<>();

    public FileBasedHostnamePortLookupTable() {
        this(DEFAULT_FILE_NAME);
    }

    public FileBasedHostnamePortLookupTable(String fileName) {
        sharedTableFile = new File(fileName);
        try {
            if (sharedTableFile.createNewFile() && !sharedTableFile.canWrite()) {
                throw new IllegalStateException("Cannot write to existing shared mapping file " + sharedTableFile);
            }
            long pagesForMinimum = (long) Math.ceil(((float) MINIMUM_INITIAL_FILE_SIZE_BYTES) / OS.SAFE_PAGE_SIZE);
            actualBytesSize = pagesForMinimum * OS.SAFE_PAGE_SIZE;
            sharedTableBytes = MappedBytes.mappedBytes(sharedTableFile, actualBytesSize, OS.SAFE_PAGE_SIZE, false);
            sharedTableBytes.singleThreadedCheckDisabled(true);
            sharedTableWire = new JSONWire(sharedTableBytes);
            sharedTableWire.consumePadding();
        } catch (IOException e) {
            throw new RuntimeException("Error creating shared mapping file", e);
        }
    }

    @Override
    public synchronized InetSocketAddress lookup(String description) {
        return lockFileAndDo(() -> {
            readFromTable();
            final ProcessScopedMapping mapping = allMappings.get(description);
            return mapping != null ? mapping.inetSocketAddress() : null;
        }, true);
    }

    @Override
    public synchronized void clear() {
        lockFileAndDo(() -> {
            readFromTable();
            allMappings.keySet().forEach(key -> {
                if (allMappings.get(key).pid == PID) {
                    allMappings.remove(key);
                }
            });
            writeToTable();
        }, false);
    }

    @Override
    public synchronized Set<String> aliases() {
        return lockFileAndDo(() -> {
            readFromTable();
            return allMappings.keySet();
        }, true);
    }

    @Override
    public synchronized void put(String description, InetSocketAddress address) {
        lockFileAndDo(() -> {
            readFromTable();
            final ProcessScopedMapping newMapping = new ProcessScopedMapping(PID, address);
            ProcessScopedMapping oldValue = allMappings.put(description, newMapping);
            if (oldValue != null) {
                Jvm.error().on(FileBasedHostnamePortLookupTable.class,
                        format("Over-wrote hostname mapping for %s, old value=%s, new value=%s", description, oldValue, newMapping));
            }
            writeToTable();
        }, false);
    }

    @Override
    public synchronized void forEach(BiConsumer<String, InetSocketAddress> consumer) {
        lockFileAndDo(() -> {
            readFromTable();
            allMappings.forEach((description, mapping) -> consumer.accept(description, mapping.inetSocketAddress()));
        }, true);
    }

    private void writeToTable() {
        sharedTableBytes.reserve(this);
        try {
            sharedTableWire.clear();
            sharedTableWire.writeAllAsMap(String.class, ProcessScopedMapping.class, allMappings);
            zeroOutRemainingBytes((int) sharedTableBytes.writePosition());
        } finally {
            sharedTableBytes.release(this);
        }
    }

    private void zeroOutRemainingBytes(int fromIndex) {
        sharedTableBytes.readLimit(sharedTableBytes.realCapacity());
        for (int i = fromIndex; i < actualBytesSize; i++) {
            sharedTableBytes.readPosition(i);
            if (sharedTableBytes.readByte() == 0) {
                break;
            }
            sharedTableBytes.writeByte(i, (byte) 0);
        }
    }

    private void readFromTable() {
        final StringBuilder sb = Wires.acquireStringBuilder();
        final ProcessScopedMapping reusableMapping = new ProcessScopedMapping();
        sharedTableBytes.reserve(this);
        try {
            sharedTableBytes.readPosition(0);
            sharedTableBytes.readLimit(sharedTableBytes.realCapacity());

            Set<String> readMappings = new HashSet<>();
            while (true) {
                final ValueIn valueIn = sharedTableWire.readEventName(sb);
                if (sb.length() == 0) {
                    break;
                }
                valueIn.object(reusableMapping, ProcessScopedMapping.class);
                final String name = sb.toString();
                readMappings.add(name);
                insertOrUpdateEntry(name, reusableMapping);
            }

            // Remove removed
            Set<String> existingKeys = new HashSet<>(allMappings.keySet());
            for (String key : existingKeys) {
                if (!readMappings.contains(key)) {
                    allMappings.remove(key);
                }
            }
        } finally {
            sharedTableBytes.release(this);
        }
    }

    private void insertOrUpdateEntry(String name, ProcessScopedMapping mapping) {
        final ProcessScopedMapping existingMapping = allMappings.get(name);
        if (existingMapping == null || !existingMapping.equals(mapping)) {
            allMappings.put(name, new ProcessScopedMapping(mapping.pid, mapping.hostname, mapping.port));
        }
    }

    private void lockFileAndDo(Runnable runnable, boolean shared) {
        this.lockFileAndDo(() -> {
            runnable.run();
            return null;
        }, shared);
    }

    private <T> T lockFileAndDo(Supplier<T> supplier, boolean shared) {
        final long timeoutAt = System.currentTimeMillis() + LOCK_TIMEOUT_MS;
        final long startMs = System.currentTimeMillis();
        Throwable lastThrown = null;
        int count;
        for (count = 1; System.currentTimeMillis() < timeoutAt; count++) {
            if (PROCESS_FILE_LOCK.tryLock()) {
                try (FileLock fileLock = sharedTableBytes.mappedFile().tryLock(0, Long.MAX_VALUE, shared)) {
                    if (fileLock != null) {
                        try {
                            T t = supplier.get();
                            logDetailsOfSlowAcquire(startMs, lastThrown);
                            return t;
                        } catch (OverlappingFileLockException e) {
                            throw new RuntimeException("Attempted to resize the underlying bytes, increase the MINIMUM_INITIAL_FILE_SIZE_BYTES or make this work with resizing!", e);
                        }
                    }
                } catch (IOException | OverlappingFileLockException e) {
                    // failed to acquire the lock, wait until other operation completes
                    lastThrown = e;
                } finally {
                    PROCESS_FILE_LOCK.unlock();
                }
            }
            int delay = Math.min(250, count * count);
            sleep(delay, MILLISECONDS);
        }
        logVerboseDetailsOfAcquireLockFailure(startMs, lastThrown, count);
        throw new IllegalStateException("Couldn't acquire lock on shared mapping file " + sharedTableFile, lastThrown);
    }

    private void logDetailsOfSlowAcquire(long startMs, Throwable lastThrown) {
        long elapsedMs = System.currentTimeMillis() - startMs;
        if (elapsedMs > 100)
            Jvm.perf().on(getClass(), "Took " + elapsedMs / 1000.0 + " seconds to obtain the lock on " + sharedTableFile, lastThrown);
    }

    private void logVerboseDetailsOfAcquireLockFailure(long startMs, Throwable lastThrown, int count) {
        if (Jvm.isDebugEnabled(FileBasedHostnamePortLookupTable.class)) {
            final long elapsedMs = System.currentTimeMillis() - startMs;
            final String message = "Failed to acquire lock on the shared mappings file. Retrying, file=" + sharedTableFile + ", count=" + count + ", elapsed=" + elapsedMs + " ms";
            Jvm.debug().on(FileBasedHostnamePortLookupTable.class, message, lastThrown);
        }
    }

    @Override
    protected synchronized void performClose() {
        lockFileAndDo(() -> {
            allMappings.clear();
            sharedTableWire.clear();
            zeroOutRemainingBytes(0);
        }, false);
        Closeable.closeQuietly(sharedTableWire, sharedTableBytes);
        long endTime = System.currentTimeMillis() + DELETE_TABLE_FILE_TIMEOUT_MS;
        BackgroundResourceReleaser.releasePendingResources();
        while (sharedTableFile.exists()) {
            sharedTableFile.delete();
            if (System.currentTimeMillis() > endTime) {
                Jvm.warn().on(FileBasedHostnamePortLookupTable.class, "Error deleting the shared lookup table");
                break;
            }
        }
    }

    static class ProcessScopedMapping implements ReadMarshallable, WriteMarshallable {
        private int pid;
        private String hostname;
        private int port;
        private InetSocketAddress address;

        public ProcessScopedMapping() {
        }

        public ProcessScopedMapping(int pid, InetSocketAddress address) {
            if (address == null) {
                throw new IllegalArgumentException("Address must not be null");
            }
            this.pid = pid;
            this.hostname = address.getHostName();
            this.port = address.getPort();
            this.address = address;
        }

        public ProcessScopedMapping(int pid, String hostname, int port) {
            this.pid = pid;
            this.hostname = hostname;
            this.port = port;
        }

        public InetSocketAddress inetSocketAddress() {
            if (address == null) {
                address = new InetSocketAddress(hostname, port);
            }
            return address;
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
            pid = wire.read("pid").int32();
            hostname = wire.read("hostname").text();
            port = wire.read("port").readInt();
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write("pid").int32(pid)
                    .write("hostname").text(hostname)
                    .write("port").int32(port);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProcessScopedMapping that = (ProcessScopedMapping) o;
            return pid == that.pid && port == that.port && hostname.equals(that.hostname);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pid, hostname, port);
        }

        @Override
        public String toString() {
            return "ProcessScopedMapping{" +
                    "pid=" + pid +
                    ", hostname='" + hostname + '\'' +
                    ", port=" + port +
                    ", hasInetSocketAddress=" + (address != null) +
                    '}';
        }
    }
}
