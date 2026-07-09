package com.garlic.shortlink.common.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 生产者配置。
 *
 * <p>显式定义 {@link ProducerFactory} 与 {@link KafkaTemplate}，统一生产者参数：
 * acks=all、retries=3、batch.size=16384、linger.ms=10，保证消息可靠性与吞吐量平衡。</p>
 *
 * <p>消费者由 spring-kafka 自动配置（依据 application.yml 中 spring.kafka.consumer.*），
 * 后续 Task 18 通过 {@code @KafkaListener} 注解即可消费。</p>
 *
 * @author garlic
 */
@Configuration
public class KafkaConfig {

    /** Kafka bootstrap servers，从 application.yml 注入 */
    @Value("${spring.kafka.bootstrap-servers:127.0.0.1:9092}")
    private String bootstrapServers;

    /**
     * 生产者工厂：String key/value 序列化，acks=all + retries=3 保证可靠性。
     *
     * @return ProducerFactory 实例
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>(8);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 全部副本确认，保证消息不丢失
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 发送失败重试 3 次
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 批量大小 16KB
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 聚合等待 10ms，提升吞吐
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * KafkaTemplate：发送 String 消息。
     *
     * @return KafkaTemplate 实例
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
