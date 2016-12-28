/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;

/**
 * Created by Rob Austin
 */
public enum CoreFields implements WireKey {
    tid,
    csp,
    cid,
    reply,
    exception,
    lastUpdateTime,
    handler;

    @NotNull
    static final ThreadLocal<StringBuilder> cpsBuilder = ThreadLocal.withInitial(StringBuilder::new);

    private static long longEvent(@NotNull final WireKey expecting, @NotNull final WireIn wire) {
        final StringBuilder eventName = Wires.acquireStringBuilder();
        long position = wire.bytes().readPosition();
        @NotNull final ValueIn valueIn = wire.readEventName(eventName);
        if (expecting.contentEquals(eventName))
            return valueIn.int64();

        throw new IllegalArgumentException("expecting a " + expecting
                + " was\n" + wire.bytes().toHexString(position, wire.bytes().readLimit() - position));
    }

    @NotNull
    public static StringBuilder stringEvent(@NotNull final WireKey expecting, @NotNull StringBuilder using,
                                            @NotNull final WireIn wire) {
        final StringBuilder eventName = Wires.acquireStringBuilder();
        @NotNull final ValueIn valueIn = wire.readEventName(eventName);
        if (expecting.contentEquals(eventName)) {
            valueIn.textTo(using);
            return using;
        }

        throw new IllegalArgumentException("expecting a " + expecting);
    }

    public static long tid(@NotNull final WireIn wire) {
        return longEvent(CoreFields.tid, wire);
    }

    public static long cid(@NotNull final WireIn wire) {
        return longEvent(CoreFields.cid, wire);
    }

    @NotNull
    public static StringBuilder csp(@NotNull final WireIn wire) {
        return stringEvent(CoreFields.csp, cpsBuilder.get(), wire);
    }
}
