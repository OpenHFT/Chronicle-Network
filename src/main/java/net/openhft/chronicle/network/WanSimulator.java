/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
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
package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;

import java.util.Random;

@Deprecated(/* to be removed in x.21*/)
public enum WanSimulator {
    ;
    private static final int NET_BANDWIDTH = Integer.getInteger("wanMB", 0);
    private static final int BYTES_PER_MS = NET_BANDWIDTH * 1000;
    private static final Random RANDOM = new Random();
    private static long totalRead;

    public static void dataRead(int bytes) {
        if (NET_BANDWIDTH <= 0) return;
        totalRead += bytes + RANDOM.nextInt(BYTES_PER_MS / 2);
        int delay = (int) (totalRead / BYTES_PER_MS);
        if (delay > 0) {
            Jvm.pause(delay);
            totalRead = 0;
        }
    }
}
