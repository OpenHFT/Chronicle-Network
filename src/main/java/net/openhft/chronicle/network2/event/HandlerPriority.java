package net.openhft.chronicle.network2.event;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public enum HandlerPriority {
    /**
     * Critical task run in a tight loop
     */
    HIGH,
    /**
     * Less critical tasks called 10% of the time
     */
    MEDIUM,
    /**
     * Timing based tasks called every give interval. Note: this task will not be called if the thread pauses e.g. due a
     * GC or when debugging is used.  This makes the timer more robust to delays than using absolute time differences.
     */
    TIMER,
    /**
     * Run when there is nothing else to do
     */
    DAEMON,
    /**
     * Task run in a back ground thread periodically
     */
    MONITOR,
    /**
     * Task is a blocking operation, added to a cached thread pool
     */
    BLOCKING;
}
