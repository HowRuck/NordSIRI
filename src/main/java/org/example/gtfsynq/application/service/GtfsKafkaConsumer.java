package org.example.gtfsynq.application.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.example.gtfsynq.infrastructure.protobuf.GtfsNativeFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class GtfsKafkaConsumer {

    private final Serde<String> keySerde;
    private final Serde<byte[]> valueSerde;

    @Value("${spring.kafka.topic:gtfs-trip-updates}")
    private String topic;

    public FeedEntity parseFeedEntity(byte[] value) {
        try {
            var typedEntity = GtfsNativeFilter.TypedEntity.decode(value);

            return FeedEntity.parseFrom(typedEntity.bytes());
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse FeedEntity", e);
            return null;
        } catch (Exception e) {
            // catch any other unexpected runtime exceptions during parsing
            log.error("Unexpected error while parsing FeedEntity", e);
            return null;
        }
    }

    @Autowired
    public void consume(StreamsBuilder builder) {
        var messageStream = builder.stream(
            topic,
            Consumed.with(keySerde, valueSerde)
        );

        messageStream
            .mapValues(this::parseFeedEntity)
            .filter((key, value) -> value != null);
    }
}
