/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.performance.tests.network;

import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

/**
 * Created by peter.lawrey on 31/01/15.
 */
class TestData implements DoubleConsumer, LongConsumer, IntConsumer {
    int value1;
    long value2;
    double value3;

    public TestData() {
    }

    public void write(@NotNull WireOut wire) {
        wire.writeDocument(false, d ->
                d.write(Field.key1).int32(value1)
                        .write(Field.key2).int64(value2)
                        .write(Field.key3).float64(value3));
    }

    public void read(@NotNull WireIn wire) {
        wire.readDocument(null, data ->
                        data.read(Field.key1).int32(this, (o, i) -> value1 = i)
                                .read(Field.key2).int64(this, (o, i) -> value2 = i)
                                .read(Field.key3).float64(this, (o, i) -> value3 = i)
        );
    }

    @Override
    public void accept(double value) {
        value3 = value;
    }

    @Override
    public void accept(int value) {
    }

    @Override
    public void accept(long value) {
    }

    enum Field implements WireKey {
        key1, key2, key3
    }
}
