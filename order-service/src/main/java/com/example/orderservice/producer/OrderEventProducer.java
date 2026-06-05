package com.example.orderservice.producer;

import com.example.orderservice.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Value("${app.kafka.topic.order-events}")
    private String topic;

    public CompletableFuture<SendResult<String, OrderEvent>> send(OrderEvent event) {
        // Dung customerId lam key -> moi customer luon vao cung 1 partition (giu thu tu).
        String key = event.getCustomerId();
        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[producer] send FAILED orderId={} err={}", event.getOrderId(), ex.getMessage(), ex);
                return;
            }
            var md = result.getRecordMetadata();
            log.info("[producer] sent orderId={} key={} topic={} partition={} offset={}",
                    event.getOrderId(), key, md.topic(), md.partition(), md.offset());
        });
        return future;
    }
}
