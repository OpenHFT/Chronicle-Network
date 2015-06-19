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

package net.openhft.chronicle.network.event;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public enum References {
    ;

    @NotNull
    static <T> T or(@Nullable T t1, @NotNull T t2) {
        return t1 == null ? t2 : t1;
    }

    @Nullable
    static <A, R> R call(@Nullable A a,
                         @NotNull Function<? super A, R> function) {
        return a == null ? null : function.apply(a);
    }

    @Nullable
    static <A, B, R> R call(@Nullable A a,
                            @NotNull Function<? super A, B> function1,
                            @NotNull Function<? super B, R> function2) {
        if (a == null) return null;
        B b = function1.apply(a);
        if (b == null) return null;
        return function2.apply(b);
    }

    @Nullable
    static <A, B, C, R> R call(@Nullable A a,
                               @NotNull Function<? super A, B> function1,
                               @NotNull Function<? super B, C> function2,
                               @NotNull Function<? super C, R> function3) {
        if (a == null) return null;
        B b = function1.apply(a);
        if (b == null) return null;
        C c = function2.apply(b);
        if (c == null) return null;
        return function3.apply(c);
    }
}
