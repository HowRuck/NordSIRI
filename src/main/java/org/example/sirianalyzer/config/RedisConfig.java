package org.example.sirianalyzer.config;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Configuration class for Redis setup, providing a RedisTemplate bean
 */
@Configuration
public class RedisConfig {

    @Value("${spring.data.redis.host:localhost}")
    private String redisHost;

    @Value("${spring.data.redis.port:6379}")
    private int redisPort;

    /**
     * Creates a RedisConnectionFactory bean for connecting to Redis.
     *
     * @return a RedisConnectionFactory instance
     */
    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration configuration =
            new RedisStandaloneConfiguration(redisHost, redisPort);

        // Use LettucePoolingClientConfiguration to manage connections
        var clientConfig = LettucePoolingClientConfiguration.builder()
            .commandTimeout(Duration.ofSeconds(2))
            .shutdownTimeout(Duration.ZERO)
            .build();

        return new LettuceConnectionFactory(configuration, clientConfig);
    }

    private static final StringRedisSerializer STRING_SERIALIZER =
        new StringRedisSerializer();
    private static final GenericToStringSerializer<Long> LONG_SERIALIZER =
        new GenericToStringSerializer<>(Long.class);

    /**
     * Creates a RedisTemplate bean for interacting with Redis.
     *
     * @param connectionFactory the RedisConnectionFactory to use
     * @return a RedisTemplate instance
     */
    @Bean
    public RedisTemplate<String, Long> redisTemplate(
        RedisConnectionFactory connectionFactory
    ) {
        var template = new RedisTemplate<String, Long>();
        template.setConnectionFactory(connectionFactory);
        template.setEnableDefaultSerializer(false);

        template.setKeySerializer(STRING_SERIALIZER);
        template.setValueSerializer(LONG_SERIALIZER);

        template.setHashKeySerializer(STRING_SERIALIZER);
        template.setHashValueSerializer(LONG_SERIALIZER);

        return template;
    }
}
