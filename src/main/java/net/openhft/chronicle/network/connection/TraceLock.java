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

package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.core.Jvm;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Rob Austin.
 */
public class TraceLock extends ReentrantLock {

    private static final long serialVersionUID = 1997992705529515418L;
    private volatile Throwable here;

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

        Throwable here = this.here;
        if (here == null)
            return super.toString();

        final StringBuilder sb = new StringBuilder(super.toString());

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
