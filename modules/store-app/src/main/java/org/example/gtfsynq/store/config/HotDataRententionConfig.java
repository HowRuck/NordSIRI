package org.example.gtfsynq.store.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "gtfsynq.retention")
public record HotDataRententionConfig(
    @DefaultValue("1h") Duration hours,
    @DefaultValue("15") Integer rateMinutes
) {}
