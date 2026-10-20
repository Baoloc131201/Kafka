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
        // Dung orderId lam key -> moi event cua cung 1 don luon vao cung 1 partition
        // => consumer nhan dung thu tu CREATED -> PAID/CANCELLED.
        String key = event.getOrderId();
        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[producer] send FAILED eventType={} orderId={} err={}",
                        event.getEventType(), event.getOrderId(), ex.getMessage(), ex);
                return;
            }
            var md = result.getRecordMetadata();
            log.info("[producer] sent eventType={} orderId={} eventId={} topic={} partition={} offset={}",
                    event.getEventType(), key, event.getEventId(), md.topic(), md.partition(), md.offset());
        });
        return future;
    }
}
