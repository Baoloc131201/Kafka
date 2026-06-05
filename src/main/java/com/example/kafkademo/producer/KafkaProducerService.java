package com.example.kafkademo.producer;

import com.example.kafkademo.config.KafkaTopicConfig;
import com.example.kafkademo.model.MessageDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendMessage(String message) {
        log.info("Producing plain message: {}", message);
        kafkaTemplate.send(KafkaTopicConfig.DEMO_TOPIC, message);
    }

    public void sendMessage(MessageDto message) {
        String key = (message.getSender() != null && !message.getSender().isBlank())
                ? message.getSender()
                : message.getId();

        log.info("Producing object message: {} | key={}", message, key);
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(KafkaTopicConfig.DEMO_TOPIC, key, message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent message=[{}] key=[{}] offset=[{}] partition=[{}]",
                        message,
                        key,
                        result.getRecordMetadata().offset(),
                        result.getRecordMetadata().partition());
            } else {
                log.error("Unable to send message=[{}] due to: {}", message, ex.getMessage());
            }
        });
    }
}
