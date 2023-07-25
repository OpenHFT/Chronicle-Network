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

package net.openhft.chronicle.network.ssl;

import net.openhft.chronicle.network.NetworkContext;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.security.cert.Certificate;

public interface SslNetworkContext<T extends NetworkContext<T>> extends NetworkContext<T> {
    /**
     * @return SSL/TLS context used by this session, or {@code null} if SSL/TLS is not used
     */
    SSLContext sslContext();

    /**
     * @return SSL/TLS parameters used by this session, or {@code null} for JSSE defaults
     */
    SSLParameters sslParameters();

    /**
     * @param sslParameters SSL/TLS parameters used by this session, or {@code null} for JSSE defaults
     */
    T sslParameters(SSLParameters sslParameters);

    /**
     * Sets certificate chain presented by peer.
     */
    void peerCertificates(Certificate[] certificates);
}