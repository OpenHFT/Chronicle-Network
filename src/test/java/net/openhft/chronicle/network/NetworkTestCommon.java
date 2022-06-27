package net.openhft.chronicle.network;

import net.openhft.chronicle.core.internal.JvmExceptionTracker;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.threads.CleaningThread;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.testframework.internal.ExceptionTracker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.function.Predicate;

public class NetworkTestCommon {

    protected ThreadDump threadDump;
    private ExceptionTracker<ExceptionKey> exceptionTracker;

    @BeforeEach
    void enableReferenceTracing() {
        AbstractReferenceCounted.enableReferenceTracing();
    }

    public void assertReferencesReleased() {
        AbstractReferenceCounted.assertReferencesReleased();
    }

    @BeforeEach
    void threadDump() {
        threadDump = new ThreadDump();
    }

    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @BeforeEach
    void recordExceptions() {
        exceptionTracker = JvmExceptionTracker.create();
        exceptionTracker.ignoreException("unable to connect to any of the hosts");
    }

    public void expectException(String message) {
        exceptionTracker.expectException(message);
    }

    public void ignoreException(String message) {
        exceptionTracker.ignoreException(message);
    }

    public void ignoreException(Predicate<ExceptionKey> predicate, String description) {
        exceptionTracker.ignoreException(predicate, description);
    }

    public void checkExceptions() {
        exceptionTracker.checkExceptions();
    }

    @AfterEach
    void afterChecks() {
        SystemTimeProvider.CLOCK = SystemTimeProvider.INSTANCE;

        CleaningThread.performCleanup(Thread.currentThread());

        // find any discarded resources.
        System.gc();
        AbstractCloseable.waitForCloseablesToClose(100);

        TCPRegistry.reset();

        assertReferencesReleased();
        checkThreadDump();
        checkExceptions();
    }
}
