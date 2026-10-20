package com.example.notificationservice.config;

import com.example.notificationservice.model.OrderEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${app.kafka.topic.order-events-dlt}")
    private String dltTopic;

    @Bean
    public ConsumerFactory<String, OrderEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Wrap voi ErrorHandlingDeserializer -> message hong khong lam consumer ket lien tuc
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);

        // KHONG xai type-header (order-service va notification-service khac package) -> luon doc thanh OrderEvent.
        // Tuong thich nguoc: producer them field moi hoac eventType moi thi consumer cu van doc duoc
        // (field la bi bo qua, eventType la -> null).
        ObjectMapper mapper = JacksonUtils.enhancedObjectMapper()
                .enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);
        JsonDeserializer<OrderEvent> json = new JsonDeserializer<>(OrderEvent.class, mapper, false);
        json.addTrustedPackages("*");

        DefaultKafkaConsumerFactory<String, OrderEvent> factory = new DefaultKafkaConsumerFactory<>(props);
        factory.setValueDeserializerSupplier(() -> new ErrorHandlingDeserializer<>(json));
        return factory;
    }

    /**
     * Template dung de day message loi vao DLT.
     * - byte[]: message khong deserialize duoc -> giu nguyen bytes goc
     * - String: key cua message
     * - Object: message deserialize OK nhung xu ly loi -> ghi lai dang JSON
     */
    @Bean
    public KafkaTemplate<Object, Object> dltKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<Object, Object>(props,
                byTypeSerializer(), byTypeSerializer()));
    }

    private static Serializer<Object> byTypeSerializer() {
        // LinkedHashMap: kiem tra theo thu tu, Object.class phai dung cuoi
        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(String.class, new StringSerializer());
        JsonSerializer<Object> json = new JsonSerializer<>();
        json.setAddTypeInfo(false);
        delegates.put(Object.class, json);
        return new DelegatingByTypeSerializer(delegates, true);
    }

    /**
     * DLT thuoc ve consumer (notification-service), khong phai cua order-service.
     * Cung so partition voi topic goc de message loi giu nguyen partition.
     */
    @Bean
    public NewTopic orderEventsDltTopic() {
        return TopicBuilder.name(dltTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> dltKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dltKafkaTemplate,
                (record, ex) -> new TopicPartition(dltTopic, record.partition()));

        // Thu lai 3 lan, moi lan cach 2s. Het lan thu -> day vao DLT va commit offset de di tiep.
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3L));

        // Loi do du lieu sai thi thu lai cung vo ich -> vao thang DLT.
        // (DeserializationException da nam san trong danh sach mac dinh)
        handler.addNotRetryableExceptions(IllegalArgumentException.class);

        handler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("[retry] attempt={} partition={} offset={} err={}",
                        deliveryAttempt, record.partition(), record.offset(), ex.getMessage()));
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> consumerFactory, DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
