package org.example.gtfsynq.store;

import org.example.gtfsynq.store.config.HotDataRetentionConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "org.example.gtfsynq")
@EnableScheduling
@EnableKafkaStreams
@ConfigurationPropertiesScan
@EnableConfigurationProperties(HotDataRetentionConfig.class)
public class StoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(StoreApplication.class, args);
    }
}
