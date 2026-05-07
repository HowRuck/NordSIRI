package org.example.gtfsynq.store.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "gtfsynq.retention")
public class HotDataRetentionConfig {

    private Duration hours = Duration.ofHours(1);
    private Integer rateMinutes = 15;
}
