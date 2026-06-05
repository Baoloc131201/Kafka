package com.example.kafkademo.consumer;

import com.example.kafkademo.config.KafkaTopicConfig;
import com.example.kafkademo.idempotency.IdempotencyService;
import com.example.kafkademo.model.MessageDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final IdempotencyService idempotency;

    @KafkaListener(
            topics = KafkaTopicConfig.DEMO_TOPIC,
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(
            @Payload MessageDto dto,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            Acknowledgment ack
    ) {
        String messageId = dto.getId();

        if (idempotency.alreadyProcessed(messageId)) {
            log.warn("Skipping duplicate messageId={} | partition={} offset={}",
                    messageId, partition, offset);
            ack.acknowledge();
            return;
        }

        String content = dto.getContent() == null ? "" : dto.getContent().toLowerCase();

        if (content.contains("invalid")) {
            throw new IllegalArgumentException(
                    "Non-retryable error for content: " + dto.getContent());
        }
        if (content.contains("fail")) {
            throw new RuntimeException(
                    "Simulated transient failure for content: " + dto.getContent());
        }

        log.info("Consumed MessageDto: {} | key={} partition={} offset={}",
                dto, key, partition, offset);

        idempotency.markProcessed(messageId);
        ack.acknowledge();
    }

    @KafkaListener(
            topics = KafkaTopicConfig.DEMO_TOPIC_DLT,
            groupId = "${spring.kafka.consumer.group-id}-dlt"
    )
    public void consumeDlt(
            @Payload MessageDto dto,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String dltExceptionMessage,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
            Acknowledgment ack
    ) {
        log.error("DLT received: payload={} | key={} partition={} offset={} | originalTopic={} reason={}",
                dto, key, partition, offset, originalTopic, dltExceptionMessage);
        ack.acknowledge();
    }
}
