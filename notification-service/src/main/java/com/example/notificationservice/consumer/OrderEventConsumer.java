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
        log.info("[consumer] received key={} partition={} offset={}",
                record.key(), record.partition(), record.offset());
        notificationService.notifyCustomer(record.value());
    }
}
