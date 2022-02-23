package net.openhft.chronicle.network.internal.lookuptable;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.network.NetworkTestCommon;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.junit.Assert.*;

public class FileBasedHostnamePortLookupTableTest extends NetworkTestCommon {

    public static final String TEST_TABLE_FILENAME = "FileBasedHostnamePortLookupTableTest";
    private FileBasedHostnamePortLookupTable lookupTable;

    @Before
    public void setUp() {
        if (OS.isWindows()) {
            expectException("Error deleting the shared lookup table");
        }
        IOTools.deleteDirWithFilesOrThrow(TEST_TABLE_FILENAME);
        lookupTable = new FileBasedHostnamePortLookupTable(TEST_TABLE_FILENAME);
    }

    @After
    public void tearDown() {
        Closeable.closeQuietly(lookupTable);
    }

    @Test
    public void shouldStoreAndRetrieve() {
        final InetSocketAddress localhost = new InetSocketAddress("localhost", 1234);
        lookupTable.put("aaa", localhost);
        assertEquals(localhost, lookupTable.lookup("aaa"));
        lookupTable.put("bbb", localhost);
        assertEquals(localhost, lookupTable.lookup("bbb"));
    }

    @Test
    public void shouldClear() {
        final InetSocketAddress localhost = new InetSocketAddress("localhost", 1234);
        lookupTable.put("aaa", localhost);
        assertEquals(localhost, lookupTable.lookup("aaa"));
        lookupTable.clear();
        assertNull(lookupTable.lookup("aaa"));
    }

    @Test
    public void shouldGetAliases() {
        final InetSocketAddress localhost = new InetSocketAddress("localhost", 1234);
        lookupTable.put("aaa", localhost);
        lookupTable.put("bbb", localhost);
        lookupTable.put("ccc", localhost);
        assertEquals(new HashSet<>(Arrays.asList("aaa", "bbb", "ccc")), lookupTable.aliases());
    }

    @Test
    public void shouldImplementForEach() {
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
    public void entriesShouldBeVisibleAcrossInstances() throws IOException {
        try (FileBasedHostnamePortLookupTable table1 = new FileBasedHostnamePortLookupTable(TEST_TABLE_FILENAME);
             FileBasedHostnamePortLookupTable table2 = new FileBasedHostnamePortLookupTable(TEST_TABLE_FILENAME)) {
            table1.put("aaa", InetSocketAddress.createUnresolved("aaa", 111));
            assertEquals(table2.aliases(), Collections.singleton("aaa"));
            table2.put("bbb", InetSocketAddress.createUnresolved("bbb", 222));
            assertEquals(table1.aliases(), new HashSet<>(Arrays.asList("aaa", "bbb")));
        }
    }

    @Test(timeout = 20_000)
    public void doShouldWorkConcurrently() {
        int seq = doShouldWorkConcurrently(false);
        int para = doShouldWorkConcurrently(true);
        assertTrue(seq > 0);
        assertTrue(para > 0);
        Jvm.startup().on(FileBasedHostnamePortLookupTable.class, "Sequential added: " + seq + ", parallel added: " + para);
    }

    public int doShouldWorkConcurrently(boolean parallel) {
        long start = System.currentTimeMillis();
        IntStream stream = IntStream.range(0, Math.min(16, Runtime.getRuntime().availableProcessors()));
        if (parallel)
            stream = stream.parallel();

        return stream.map(myId -> {
            try (FileBasedHostnamePortLookupTable table = new FileBasedHostnamePortLookupTable(TEST_TABLE_FILENAME)) {
                Set<String> allMyAliases = new HashSet<>();
                for (int i = 0; i < 200 && start + 2000 > System.currentTimeMillis(); i++) {
                    String description = format("0." + (parallel ? "1" : "0") + ".%d.%d", myId, i);
                    allMyAliases.add(description);
                    InetSocketAddress address = InetSocketAddress.createUnresolved(description, i);
                    table.put(description, address);
                    InetSocketAddress lookup = table.lookup(description);
                    assertNotNull(description, lookup);
                }
                Set<String> missing = new LinkedHashSet<>(allMyAliases);
                missing.removeAll(table.aliases());
                if (!missing.isEmpty())
                    fail("Missing hosts " + missing);
                return allMyAliases.size();
            } catch (IOException e) {
                throw new AssertionError("Error creating lookup table", e);
            }
        }).sum();
    }

    @Test
    public void mappingsAreEqualRegardlessOfResolution() {
        final FileBasedHostnamePortLookupTable.ProcessScopedMapping unresolved
                = new FileBasedHostnamePortLookupTable.ProcessScopedMapping(123, InetSocketAddress.createUnresolved("localhost", 456));
        final FileBasedHostnamePortLookupTable.ProcessScopedMapping resolved
                = new FileBasedHostnamePortLookupTable.ProcessScopedMapping(123, new InetSocketAddress("localhost", 456));
        assertEquals(unresolved, resolved);
    }

    @Test
    public void mappingsHaveSameHashCodeRegardlessOfResolution() {
        final FileBasedHostnamePortLookupTable.ProcessScopedMapping unresolved
                = new FileBasedHostnamePortLookupTable.ProcessScopedMapping(123, InetSocketAddress.createUnresolved("localhost", 456));
        final FileBasedHostnamePortLookupTable.ProcessScopedMapping resolved
                = new FileBasedHostnamePortLookupTable.ProcessScopedMapping(123, new InetSocketAddress("localhost", 456));
        assertEquals(unresolved.hashCode(), resolved.hashCode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void addressIsMandatory() {
        lookupTable.put("something", null);
    }
}