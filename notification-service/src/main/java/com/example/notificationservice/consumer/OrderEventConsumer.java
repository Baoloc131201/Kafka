package com.example.notificationservice.consumer;

import com.example.notificationservice.model.OrderEvent;
import com.example.notificationservice.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final NotificationService notificationService;

    @KafkaListener(
            topics = "${app.kafka.topic.order-events}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onOrderEvent(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();
        log.info("[consumer] received eventType={} eventId={} key={} partition={} offset={}",
                event.getEventType(), event.getEventId(), record.key(), record.partition(), record.offset());

        if (event.getEventType() == null) {
            // Event type moi ma service nay chua biet -> bo qua thay vi lam ket consumer
            log.warn("[consumer] unknown/missing eventType, skipping eventId={}", event.getEventId());
            return;
        }
        notificationService.notifyCustomer(event);
    }
}
