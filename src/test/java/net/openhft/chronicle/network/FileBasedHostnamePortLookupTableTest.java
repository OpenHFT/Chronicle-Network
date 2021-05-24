package net.openhft.chronicle.network;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.network.internal.lookuptable.FileBasedHostnamePortLookupTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.junit.Assert.*;

public class FileBasedHostnamePortLookupTableTest {

    private FileBasedHostnamePortLookupTable lookupTable;

    @Before
    public void setUp() {
        final File tmpFile = IOTools.createTempFile("FileBasedHostnamePortLookupTableTest");
        lookupTable = new FileBasedHostnamePortLookupTable(tmpFile.getName());
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

    @Ignore(/* https://github.com/OpenHFT/Chronicle-Network/issues/103 */)
    @Test(timeout = 20_000)
    public void shouldWorkConcurrently() {
        int para = doShouldWorkConcurrently(true);
        int seq = doShouldWorkConcurrently(false);
        System.out.println(seq + " " + para);
        assertTrue(para > seq);
    }

    public int doShouldWorkConcurrently(boolean parallel) {
        long start = System.currentTimeMillis();
        IntStream stream = IntStream.range(0, Runtime.getRuntime().availableProcessors());
        if (parallel)
            stream.parallel();

        return stream.map(myId -> {
            Set<String> allMyAliases = new HashSet<>();
            for (int i = 0; i < 50 && start + 2000 > System.currentTimeMillis(); i++) {
                String description = format("0." + (parallel ? "1" : "0") + ".%d.%d", myId, i);
                allMyAliases.add(description);
                InetSocketAddress address = new InetSocketAddress(description, i);
                lookupTable.put(description, address);
                InetSocketAddress lookup = lookupTable.lookup(description);
                assertNotNull(description, lookup);
            }
            Set<String> missing = new LinkedHashSet<>(allMyAliases);
            missing.removeAll(lookupTable.aliases());
            if (!missing.isEmpty())
                fail("Missing hosts " + missing);
            return allMyAliases.size();
        }).sum();
    }
}