package net.openhft.chronicle.network.event;



import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
