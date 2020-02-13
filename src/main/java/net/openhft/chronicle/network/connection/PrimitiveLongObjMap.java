package net.openhft.chronicle.network.connection;

import com.koloboke.compile.KolobokeMap;
import com.koloboke.function.LongObjConsumer;

import java.util.Collection;

@KolobokeMap
public abstract class PrimitiveLongObjMap<V>  {

    public static <V> PrimitiveLongObjMap<V> withExpectedSize(int expectedSize) {
        final KolobokePrimitiveLongObjMap<V> map = new KolobokePrimitiveLongObjMap<>(expectedSize);
        return map;
    }

    public abstract V put(long key, V value);

    public abstract void justPut(long key, V value);

    public abstract V get(long key);

    public abstract V remove(long key);

    public abstract boolean justRemove(long key);

    public abstract void clear();

    public abstract Collection<V> values();

    public abstract void forEach(LongObjConsumer<? super V> longObjConsumer);

}