package net.openhft.performance.tests.network2;

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireOut;

/**
 * Created by peter.lawrey on 31/01/15.
 */
class TestData implements Marshallable {
    int key1;
    long key2;
    double key3;

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(Field.key1).int32(key1)
                .write(Field.key2).int64(key2)
                .write(Field.key3).float64(key3);
    }

    @Override
    public void readMarshallable(WireIn wire) {
        wire.read(Field.key1).int32(i -> key1 = i)
                .read(Field.key2).int64(i -> key2 = i)
                .read(Field.key3).float64(i -> key3 = i);
    }

    enum Field implements WireKey {
        key1, key2, key3;
    }
}
