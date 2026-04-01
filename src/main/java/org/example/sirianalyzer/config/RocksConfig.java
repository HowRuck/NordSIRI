package org.example.sirianalyzer.config;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RocksConfig {

    @Bean
    public RocksDB rocksDB(@Value("${rocksdb.path}") String path)
        throws RocksDBException {
        RocksDB.loadLibrary();

        try (
            var cache = new LRUCache(256 * 1024 * 1024);
            var filter = new BloomFilter(10, false);
            var options = new Options()
        ) {
            var tableConfig = new BlockBasedTableConfig()
                .setFilterPolicy(filter)
                .setBlockCache(cache);

            options
                .setCreateIfMissing(true)
                .setTableFormatConfig(tableConfig)
                .setIncreaseParallelism(
                    Runtime.getRuntime().availableProcessors()
                )
                .setCompressionType(CompressionType.LZ4_COMPRESSION);

            return RocksDB.open(options, path);
        }
    }
}
