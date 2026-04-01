package org.example.sirianalyzer.config;

import static org.lmdbjava.Env.create;

import java.io.File;
import java.nio.ByteBuffer;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LmdbConfig {

    /**
     * Create and configure an LMDB environment bean
     */
    @Bean(destroyMethod = "close")
    public Env<ByteBuffer> lmdbEnv() {
        var path = new File(System.getProperty("user.home"), ".cache/lmdb");

        if (!path.exists() && !path.mkdirs()) {
            throw new IllegalStateException(
                "Failed to create LMDB directory at: " + path.getAbsolutePath()
            );
        }

        return create()
            .setMapSize(1024L * 1024 * 1024) // 1GB
            .setMaxDbs(1)
            .open(path, EnvFlags.MDB_NOSYNC);
    }

    /**
     * Create and configure an LMDB database bean
     */
    @Bean(destroyMethod = "close")
    public Dbi<ByteBuffer> lmdbDb(Env<ByteBuffer> env) {
        return env.openDbi("gtfs", DbiFlags.MDB_CREATE);
    }
}
