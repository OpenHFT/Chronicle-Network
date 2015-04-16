package net.openhft.chronicle.engine.client;

import org.jetbrains.annotations.NotNull;

import static java.lang.Character.toLowerCase;

/**
 * Created by Rob Austin
 */
public enum StringUtils {
    ;

    public static boolean endsWith(@NotNull final CharSequence source,
                                   @NotNull final String endsWith) {

        for (int i = 1; i <= endsWith.length(); i++) {

            if (toLowerCase(source.charAt(source.length() - i)) !=
                    toLowerCase(endsWith.charAt(endsWith.length() - i))) {
                return false;
            }
        }

        return true;
    }
}
