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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.network.NetworkTestCommon;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.*;

class FileBasedHostnamePortLookupTableTest extends NetworkTestCommon {

    public static final String TEST_TABLE_FILENAME = OS.getTarget()+"/FileBasedHostnamePortLookupTableTest";
    private FileBasedHostnamePortLookupTable lookupTable;

    @BeforeEach
    void setUp() {
        if (OS.isWindows()) {
            ignoreException("Error deleting the shared lookup table");
        }
        IOTools.deleteDirWithFilesOrThrow(TEST_TABLE_FILENAME);
        lookupTable = new FileBasedHostnamePortLookupTable(TEST_TABLE_FILENAME);
    }
    @Override
    @BeforeEach
    protected void threadDump() {
        super.threadDump();
    }

    @AfterEach
    public void closeLookupTable() {
        Closeable.closeQuietly(lookupTable);
    }

    @Test
    void shouldStoreAndRetrieve() {
        final InetSocketAddress localhost = new InetSocketAddress("localhost", 1234);
        lookupTable.put("aaa", localhost);
        assertEquals(localhost, lookupTable.lookup("aaa"));
        lookupTable.put("bbb", localhost);
        assertEquals(localhost, lookupTable.lookup("bbb"));
    }

    @Test
    void shouldClear() {
        final InetSocketAddress localhost = new InetSocketAddress("localhost", 1234);
        lookupTable.put("aaa", localhost);
        assertEquals(localhost, lookupTable.lookup("aaa"));
        lookupTable.clear();
        assertNull(lookupTable.lookup("aaa"));
    }

    @Test
    void shouldGetAliases() {
        final InetSocketAddress localhost = new InetSocketAddress("localhost", 1234);
        lookupTable.put("aaa", localhost);
        lookupTable.put("bbb", localhost);
        lookupTable.put("ccc", localhost);
        assertEquals(new HashSet<>(Arrays.asList("aaa", "bbb", "ccc")), lookupTable.aliases());
    }

    @Test
    void shouldImplementForEach() {
        final InetSocketAddress localhost = new InetSocketAddress("localhost", 1234);
        lookupTable.put("aaa", localhost);
        lookupTable.put("bbb", localhost);
        lookupTable.put("ccc", localhost);
        Set<String> allValues = new HashSet<>();
        lookupTable.forEach((name, addr) -> {
            assertEquals(localhost, addr);
            allValues.add(name);
        });
        assertEquals(new HashSet<>(Arrays.asList("aaa", "bbb", "ccc")), allValues);
    }

    @Test
    void entriesShouldBeVisibleAcrossInstances() {
        try (FileBasedHostnamePortLookupTable table1 = new FileBasedHostnamePortLookupTable(TEST_TABLE_FILENAME);
             FileBasedHostnamePortLookupTable table2 = new FileBasedHostnamePortLookupTable(TEST_TABLE_FILENAME)) {
            table1.put("aaa", InetSocketAddress.createUnresolved("aaa", 111));
            assertEquals(table2.aliases(), Collections.singleton("aaa"));
            table2.put("bbb", InetSocketAddress.createUnresolved("bbb", 222));
            assertEquals(table1.aliases(), new HashSet<>(Arrays.asList("aaa", "bbb")));
        }
    }

    @Test
    @Timeout(20)
    void doShouldWorkConcurrently() {
        int seq = doShouldWorkConcurrently(false);
        int para = doShouldWorkConcurrently(true);
        assertTrue(seq > 0);
        assertTrue(para > 0);
        Jvm.startup().on(FileBasedHostnamePortLookupTable.class, "Sequential added: " + seq + ", parallel added: " + para);
    }

    public int doShouldWorkConcurrently(boolean parallel) {
        long start = System.currentTimeMillis();
        final int numProcessors = Math.min(16, Runtime.getRuntime().availableProcessors());
        IntStream stream = IntStream.range(0, numProcessors);
        if (parallel)
            stream = stream.parallel();

        List<FileBasedHostnamePortLookupTable> tables = IntStream.range(0, numProcessors)
                .mapToObj(id -> new FileBasedHostnamePortLookupTable(TEST_TABLE_FILENAME))
                .collect(Collectors.toList());

        final int sum = stream.map(myId -> {
            FileBasedHostnamePortLookupTable table = tables.get(myId);
            Set<String> allMyAliases = new HashSet<>();
            for (int i = 0; i < 200 && start + 2000 > System.currentTimeMillis(); i++) {
                String description = format("0." + (parallel ? "1" : "0") + ".%d.%d", myId, i);
                allMyAliases.add(description);
                InetSocketAddress address = InetSocketAddress.createUnresolved(description, i);
                table.put(description, address);
                InetSocketAddress lookup = table.lookup(description);
                assertNotNull(lookup, description);
            }
            Set<String> missing = new LinkedHashSet<>(allMyAliases);
            missing.removeAll(table.aliases());
            if (!missing.isEmpty())
                fail("Missing hosts " + missing);
            return allMyAliases.size();
        }).sum();

        // Need to do this in parallel because they all time out on Windows (and take 1s)
        tables.stream().parallel().forEach(Closeable::closeQuietly);
        return sum;
    }

    @Test
    void mappingsAreEqualRegardlessOfResolution() {
        final FileBasedHostnamePortLookupTable.ProcessScopedMapping unresolved
                = new FileBasedHostnamePortLookupTable.ProcessScopedMapping(123, InetSocketAddress.createUnresolved("localhost", 456));
        final FileBasedHostnamePortLookupTable.ProcessScopedMapping resolved
                = new FileBasedHostnamePortLookupTable.ProcessScopedMapping(123, new InetSocketAddress("localhost", 456));
        assertEquals(unresolved, resolved);
    }

    @Test
    void mappingsHaveSameHashCodeRegardlessOfResolution() {
        final FileBasedHostnamePortLookupTable.ProcessScopedMapping unresolved
                = new FileBasedHostnamePortLookupTable.ProcessScopedMapping(123, InetSocketAddress.createUnresolved("localhost", 456));
        final FileBasedHostnamePortLookupTable.ProcessScopedMapping resolved
                = new FileBasedHostnamePortLookupTable.ProcessScopedMapping(123, new InetSocketAddress("localhost", 456));
        assertEquals(unresolved.hashCode(), resolved.hashCode());
    }

    @Test
    void addressIsMandatory() {
        assertThrows(IllegalArgumentException.class, () -> lookupTable.put("something", null));
    }
}