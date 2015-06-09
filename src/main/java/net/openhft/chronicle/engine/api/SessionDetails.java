package net.openhft.chronicle.engine.api;

import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Session local details stored here.
 * <p>
 * Created by peter on 01/06/15.
 */
public interface SessionDetails {

    // a unique id used to identify this session, this field is by contract immutable
    UUID sessionId();

    @Nullable
    String userId();

    @Nullable
    String securityToken();

    @Nullable
    InetSocketAddress connectionAddress();

    long connectTimeMS();

    <I> void set(Class<I> infoClass, I info);

    <I> I get(Class<I> infoClass);
}
