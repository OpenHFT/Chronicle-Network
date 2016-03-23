/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.network;

import net.openhft.chronicle.core.Jvm;

import java.util.Random;

/**
 * Created by peter.lawrey on 16/07/2015.
 */
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
