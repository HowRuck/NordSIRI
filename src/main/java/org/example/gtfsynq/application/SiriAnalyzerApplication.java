package org.example.gtfsynq.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "org.example.gtfsynq")
@EnableScheduling
@EnableAsync
@EnableKafkaStreams
@ConfigurationPropertiesScan
public class SiriAnalyzerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SiriAnalyzerApplication.class, args);
    }
}
