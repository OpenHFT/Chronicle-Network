package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;

/**
 * Created by peter.lawrey on 16/07/2015.
 */
public enum WanSimulator {
    ;
    private static final int NET_BANDWIDTH = Integer.getInteger("wanMB", 0);
    private static final int BYTES_PER_MS = NET_BANDWIDTH * 1000;
    private static long totalRead = 0;

    public static void dataRead(int bytes) {
        if (NET_BANDWIDTH <= 0) return;
        totalRead += bytes + 128;
        int delay = (int) (totalRead / BYTES_PER_MS);
        if (delay > 0) {
            Jvm.pause(delay);
            totalRead = 0;
        }
    }
}
