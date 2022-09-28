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
package net.openhft.chronicle.network;

@Deprecated(/* To be considered for removal in x.25 */)
public enum ServerThreadingStrategy {

    SINGLE_THREADED("uses a single threaded prioritised event loop," +
            " where the reads take priority over the asynchronous writes"),
    @Deprecated(/* To be considered for removal in x.25 */)
    CONCURRENT("each client connection is partitioned to a limit number of threads, " +
            "This is ideal when you have a small number of client connections on a server with a large number of free cores");

    private final String description;

    ServerThreadingStrategy(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
