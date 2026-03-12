package org.example.sirianalyzer;

import de.codecentric.boot.admin.server.config.EnableAdminServer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableAdminServer
public class SiriAnalyzerApplication {

    static void main(String[] args) {
        SpringApplication.run(SiriAnalyzerApplication.class, args);
    }
}
