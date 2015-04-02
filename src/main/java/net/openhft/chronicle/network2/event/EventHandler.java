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

/**
 * Created by peter.lawrey on 22/01/15.
 */
public interface EventHandler {
    default void eventLoop(EventLoop eventLoop) {
    }

    default HandlerPriority priority() {
        return HandlerPriority.MEDIUM;
    }

    /**
     * perform all tasks once and return ASAP.
     *
     * @return true if you expect more work very soon.
     */
    boolean runOnce();

    default boolean isDead() {
        return false;
    }
}
