package net.openhft.chronicle.network;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

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

    @Test
    public void shouldWorkConcurrently() throws InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(10);
        CompletionService<Integer> cs = new ExecutorCompletionService<>(es);
        int numProcessors = Runtime.getRuntime().availableProcessors();
        for (int i = 0; i < numProcessors; i++) {
            cs.submit(new UpdatingThread(i), i);
        }
        for (int i = 0; i < numProcessors; i++) {
            cs.take();
        }
        es.shutdown();
        assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
    }

    private class UpdatingThread implements Runnable {
        private final int myId;

        private UpdatingThread(int myId) {
            this.myId = myId;
        }

        @Override
        public void run() {
            Set<String> allMyAliases = new HashSet<>();
            for (int i = 0; i < 50; i++) {
                String description = format("%d-%d", myId, i);
                allMyAliases.add(description);
                lookupTable.put(description, new InetSocketAddress(description, i));
            }
            assertTrue(lookupTable.aliases().containsAll(allMyAliases));
        }
    }
}