package com.example.orderservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.time.Duration;

/**
 * order-service la chu so huu topic orders.events nen khai bao topic o day.
 * Production thuong tao topic bang ha tang (Terraform/Strimzi) voi replicas=3, min.insync.replicas=2.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topic.order-events}")
    private String orderEventsTopic;

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(orderEventsTopic)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(7).toMillis()))
                .build();
    }
}
