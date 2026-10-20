package com.example.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String DEMO_TOPIC = "demo-topic";
    public static final String DEMO_TOPIC_DLT = "demo-topic.DLT";

    @Bean
    public NewTopic demoTopic() {
        return TopicBuilder.name(DEMO_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic demoTopicDlt() {
        return TopicBuilder.name(DEMO_TOPIC_DLT)
                .partitions(3)
                .replicas(1)
                .build();f
    }
}
