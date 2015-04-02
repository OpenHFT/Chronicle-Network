/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network2.event;

import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.model.constraints.Nullable;

import java.util.function.Function;

/**
 * Created by peter.lawrey on 22/01/15.
 */
public enum References {
    ;

    static <T> T or(@Nullable T t1, @NotNull T t2) {
        return t1 == null ? t2 : t1;
    }

    static <A, R> R call(@Nullable A a,
                         Function<? super A, R> function) {
        return a == null ? null : function.apply(a);
    }

    static <A, B, R> R call(@Nullable A a,
                            Function<? super A, B> function1,
                            Function<? super B, R> function2) {
        if (a == null) return null;
        B b = function1.apply(a);
        if (b == null) return null;
        return function2.apply(b);
    }

    static <A, B, C, R> R call(@Nullable A a,
                               Function<? super A, B> function1,
                               Function<? super B, C> function2,
                               Function<? super C, R> function3) {
        if (a == null) return null;
        B b = function1.apply(a);
        if (b == null) return null;
        C c = function2.apply(b);
        if (c == null) return null;
        return function3.apply(c);
    }
}
