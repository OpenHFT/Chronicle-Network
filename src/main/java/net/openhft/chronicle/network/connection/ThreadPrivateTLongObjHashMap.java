/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
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
package net.openhft.chronicle.network.connection;

import gnu.trove.function.TObjectFunction;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.procedure.TLongObjectProcedure;
import gnu.trove.procedure.TLongProcedure;
import gnu.trove.procedure.TObjectProcedure;
import gnu.trove.set.TLongSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class to determine if several threads are using a TLongObjectMap.
 *
 * This class should not be used in production code.
 *
 * @param <V> value type
 */
public final class ThreadPrivateTLongObjHashMap<V> implements TLongObjectMap<V> {

    private final TLongObjectMap<V> delegate;
    private final AtomicLong threadId;
    private final AtomicReference<String> threadName;

    public ThreadPrivateTLongObjHashMap(@NotNull TLongObjectMap<V> delegate) {
        this.delegate = delegate;
        this.threadId = new AtomicLong();
        this.threadName = new AtomicReference<>();
    }

    @Override
    public long getNoEntryKey() {
        assertThreadPrivate();
        return delegate.getNoEntryKey();
    }

    @Override
    public int size() {
        assertThreadPrivate();
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        assertThreadPrivate();
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(long key) {
        assertThreadPrivate();
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        assertThreadPrivate();
        return delegate.containsValue(value);
    }

    @Override
    public V get(long key) {
        assertThreadPrivate();
        return delegate.get(key);
    }

    @Override
    public V put(long key, V value) {
        assertThreadPrivate();
        return delegate.put(key, value);
    }

    @Override
    public V putIfAbsent(long key, V value) {
        assertThreadPrivate();
        return delegate.putIfAbsent(key, value);
    }

    @Override
    public V remove(long key) {
        assertThreadPrivate();
        return delegate.remove(key);
    }

    @Override
    public void putAll(Map<? extends Long, ? extends V> m) {
        assertThreadPrivate();
        delegate.putAll(m);
    }

    @Override
    public void putAll(TLongObjectMap<? extends V> map) {
        assertThreadPrivate();
        delegate.putAll(map);
    }

    @Override
    public void clear() {
        assertThreadPrivate();
        delegate.clear();
    }

    @Override
    public TLongSet keySet() {
        assertThreadPrivate();
        return delegate.keySet();
    }

    @Override
    public long[] keys() {
        assertThreadPrivate();
        return delegate.keys();
    }

    @Override
    public long[] keys(long[] array) {
        assertThreadPrivate();
        return delegate.keys(array);
    }

    @Override
    public Collection<V> valueCollection() {
        assertThreadPrivate();
        return delegate.valueCollection();
    }

    @Override
    public Object[] values() {
        assertThreadPrivate();
        return delegate.values();
    }

    @Override
    public V[] values(V[] array) {
        assertThreadPrivate();
        return delegate.values(array);
    }

    @Override
    public TLongObjectIterator<V> iterator() {
        assertThreadPrivate();
        return delegate.iterator();
    }

    @Override
    public boolean forEachKey(TLongProcedure procedure) {
        assertThreadPrivate();
        return delegate.forEachKey(procedure);
    }

    @Override
    public boolean forEachValue(TObjectProcedure<? super V> procedure) {
        assertThreadPrivate();
        return delegate.forEachValue(procedure);
    }

    @Override
    public boolean forEachEntry(TLongObjectProcedure<? super V> procedure) {
        assertThreadPrivate();
        return delegate.forEachEntry(procedure);
    }

    @Override
    public void transformValues(TObjectFunction<V, V> function) {
        assertThreadPrivate();
        delegate.transformValues(function);
    }

    @Override
    public boolean retainEntries(TLongObjectProcedure<? super V> procedure) {
        assertThreadPrivate();
        return delegate.retainEntries(procedure);
    }

    @Override
    public boolean equals(Object o) {
        assertThreadPrivate();
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        assertThreadPrivate();
        return delegate.hashCode();
    }

    private void assertThreadPrivate() {
        if (threadId.get() == 0L) {
            // This is the first call
            threadId.set(Thread.currentThread().getId());
            threadName.set(Thread.currentThread().getName());
        } else {
            if (threadId.get() != Thread.currentThread().getId()) {
                throw new ConcurrentModificationException(
                        String.format("This map was first used by thread %d (%s) " +
                                        "but are now being used by thread %d, (%s)",
                                threadId.get(), threadName.get(),
                                Thread.currentThread().getId(), Thread.currentThread().getName()
                        ));
            }
        }
 }
}
