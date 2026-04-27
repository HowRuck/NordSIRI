package org.example.gtfsynq.ingest;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
    classes = SiriAnalyzerApplication.class,
    properties = "spring.kafka.streams.auto-startup=false"
)
class SiriAnalyzerApplicationTests {

    @Test
    void contextLoads() {}
}
