package net.openhft.chronicle.network.ssl;

import org.jetbrains.annotations.NotNull;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;

final class SSLContextLoader {
    static final File KEYSTORE_FILE = new File("src/test/resources", "test.jks");

    @NotNull
    static SSLContext getInitialisedContext() throws NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException, UnrecoverableKeyException, KeyManagementException {
        final SSLContext context = SSLContext.getInstance("TLS");
        KeyManagerFactory kmf =
                KeyManagerFactory.getInstance("SunX509");
        final KeyStore keyStore = KeyStore.getInstance("JKS");
        final char[] password = "password".toCharArray();
        keyStore.load(new FileInputStream(KEYSTORE_FILE), password);
        kmf.init(keyStore, password);

        final KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(new FileInputStream(KEYSTORE_FILE), password);

        TrustManagerFactory tmf =
                TrustManagerFactory.getInstance("SunX509");
        tmf.init(trustStore);
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
        return context;
    }
}
