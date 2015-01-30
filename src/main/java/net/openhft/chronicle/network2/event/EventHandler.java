package net.openhft.chronicle.network2.event;

/**
 * Created by peter on 22/01/15.
 */
public interface EventHandler {
    default void eventLoop(EventLoop eventLoop) {
    }

    default HandlerPriority priority() {
        return HandlerPriority.MEDIUM;
    }

    /**
     * perform all tasks once and return ASAP.
     *
     * @return true if you expect more work very soon.
     */
    boolean runOnce();

    default boolean isDead() {
        return false;
    }
}
