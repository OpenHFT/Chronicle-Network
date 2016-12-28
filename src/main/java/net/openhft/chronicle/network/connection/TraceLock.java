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

import net.openhft.chronicle.core.Jvm;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Rob Austin.
 */
class TraceLock extends ReentrantLock {

    private static final long serialVersionUID = 1997992705529515418L;
    @Nullable
    private volatile Throwable here;

    @NotNull
    public static ReentrantLock create() {
        return Jvm.isDebug() ? new TraceLock() : new ReentrantLock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        super.lockInterruptibly();
        here = new Throwable();
    }

    @Override
    public void lock() {

        super.lock();
        here = new Throwable();
    }

    @Override
    public String toString() {

        @Nullable Throwable here = this.here;
        if (here == null)
            return super.toString();

        @NotNull final StringBuilder sb = new StringBuilder(super.toString());

        for (StackTraceElement s : here.getStackTrace()) {
            sb.append("\n\tat ").append(s);
        }

        return sb.toString();

    }

    @Override
    public void unlock() {
        if (getHoldCount() == 1)
            here = null;
        super.unlock();
    }

    @Override
    public boolean tryLock() {
        final boolean b = super.tryLock();
        if (b)
            here = new Throwable();
        return b;
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {

        final boolean b = super.tryLock(timeout, unit);
        if (b)
            here = new Throwable();
        return b;
    }
}
