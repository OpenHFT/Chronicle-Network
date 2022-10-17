/*
 * Copyright 2016-2022 chronicle.software
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

package net.openhft.chronicle.network.internal;

import net.openhft.chronicle.core.Jvm;

import java.io.IOException;
import java.net.SocketException;
import java.util.Locale;

@Deprecated(/* to be removed in x.25 */)
public final class SocketExceptionUtil {

    private SocketExceptionUtil() {
    }

    /**
     * Is the passed exception one that results from reading from or writing to a reset
     * connection?
     * <p>
     * NOTE: This is not reliable and shouldn't be used for anything critical. We use it
     * to make logging less noisy, false negatives are acceptable and expected. It should
     * not produce false positives, but there's no guarantees it doesn't.
     *
     * @deprecated To be removed in x.25
     * @see net.openhft.chronicle.core.io.IOTools#isClosedException(java.lang.Exception)
     *
     * @param e The exception
     * @return true if the exception is one that signifies the connection was reset
     */
    @Deprecated(/* to be removed in x.25 */)
    public static boolean isAConnectionResetException(IOException e) {
        Language.warnOnce();

        return isALinuxJava12OrLessConnectionResetException(e)
                || isAWindowsConnectionResetException(e)
                || isALinuxJava13OrGreaterConnectionResetException(e)
                || isAWindowsEstablishedConnectionAbortedException(e)
                || isAnOSXBrokenPipeException(e);
    }

    private static boolean isALinuxJava13OrGreaterConnectionResetException(IOException e) {
        return e.getClass().equals(SocketException.class) && "Connection reset".equals(e.getMessage());
    }

    private static boolean isAWindowsConnectionResetException(IOException e) {
        return e.getClass().equals(IOException.class) && "An existing connection was forcibly closed by the remote host".equals(e.getMessage());
    }

    private static boolean isALinuxJava12OrLessConnectionResetException(IOException e) {
        return e.getClass().equals(IOException.class) && "Connection reset by peer".equals(e.getMessage());
    }

    private static boolean isAWindowsEstablishedConnectionAbortedException(Exception e) {
        return e.getClass().equals(IOException.class) && "An established connection was aborted by the software in your host machine".equals(e.getMessage());
    }

    private static boolean isAnOSXBrokenPipeException(Exception e) {
        return e.getClass().equals(IOException.class) && "Broken pipe".equals(e.getMessage());
    }

    private static final class Language {
        static {
            if (!Locale.getDefault().getLanguage().equals(Locale.ENGLISH.getLanguage())) {
                Jvm.warn().on(SocketExceptionUtil.class,
                        "Running under non-English locale '" + Locale.getDefault().getLanguage() +
                                "', transient exceptions will be reported with higher level than necessary.");
            }
        }

        static void warnOnce() {
            // No-op.
        }
    }
}
