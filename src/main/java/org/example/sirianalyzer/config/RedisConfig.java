package org.example.sirianalyzer.config;

import io.lettuce.core.RedisClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Configuration class for Redis setup, providing a RedisTemplate bean
 */
@Configuration
public class RedisConfig {

    /**
     * Creates a RedisClient bean for connecting to Redis.
     *
     * @return a RedisClient instance
     */
    @Bean
    public RedisClient redisClient() {
        return RedisClient.create("redis://localhost:6379");
    }

    /**
     * Creates a RedisConnectionFactory bean for connecting to Redis.
     *
     * @return a RedisConnectionFactory instance
     */
    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        // Use Lettuce as the Redis client library
        return new LettuceConnectionFactory();
    }

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
        return template;
    }
}
