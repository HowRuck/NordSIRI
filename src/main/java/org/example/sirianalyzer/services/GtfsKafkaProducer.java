package org.example.sirianalyzer.services;

import com.google.transit.realtime.GtfsRealtime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.sirianalyzer.services.filtering.GtfsFilterWorkerService.BatchEntity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * Kafka producer for GTFS trip updates
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class GtfsKafkaProducer {

    /**
     * Kafka template for sending messages
     */
    private final KafkaTemplate<String, byte[]> kafka;

    /**
     * Kafka topic to send messages to
     */
    @Value("${spring.kafka.topic:gtfs-trip-updates}")
    private String topic;

    /**
     * Sends a batch of trip updates to Kafka.
     *
     * @param feedId the feed ID
     * @param entities the entities to send
     */
    @Async
    public void sendTripUpdates(String feedId, List<BatchEntity> entities) {
        var startTime = System.currentTimeMillis();
        log.info("Sending {} trip updates to Kafka", entities.size());

        for (BatchEntity entity : entities) {
            kafka.send(topic, feedId, entity.payload().toByteArray());
        }

        var endTime = System.currentTimeMillis();
        log.info(
            "Sent {} trip updates to Kafka in {}ms",
            entities.size(),
            endTime - startTime
        );
    }

    /**
     * Send a GTFS trip update to Kafka
     *
     * @param id ID of the trip update
     * @param entity The GTFS trip update entity
     */
    public void send(String id, GtfsRealtime.FeedEntity entity) {
        kafka.send(topic, id, entity.toByteArray());
    }
}
