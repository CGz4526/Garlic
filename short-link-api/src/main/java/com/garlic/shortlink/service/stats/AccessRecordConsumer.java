package com.garlic.shortlink.service.stats;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.garlic.shortlink.common.constant.ShortLinkConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka 访问记录消费者。
 *
 * <p>消费者端入口：监听 topic {@link ShortLinkConstants#TOPIC_ACCESS_RECORD}，
 * 接收生产端（{@link com.garlic.shortlink.service.mq.KafkaAccessRecordProducer}）发送的访问记录 JSON，
 * 解析后委托给 {@link StatsAggregator} 进行内存聚合与明细落库。</p>
 *
 * <p>设计要点：</p>
 * <ul>
 *   <li>groupId = {@code short-link-consumer}，与 application.yml 中
 *       {@code spring.kafka.consumer.group-id} 保持一致；</li>
 *   <li>生产端以 shortCode 作为 Kafka 消息 key 发送，Kafka 按 key 哈希分区，
 *       保证同一短链的访问记录落到同一分区，本消费者按分区顺序消费 → 同短链聚合有序；</li>
 *   <li>消费并发度由 {@code spring.kafka.listener.concurrency} 控制（默认 1），
 *       多线程消费时不同短链可并行处理，同短链仍串行（分区保证）；</li>
 *   <li>异常处理：JSON 解析失败仅记录日志不重试，避免毒丸消息阻塞分区消费。</li>
 * </ul>
 *
 * @author garlic
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccessRecordConsumer {

    /** 注入内存聚合器（聚合 PV/UV/IP 与维度分布，落库明细） */
    private final StatsAggregator statsAggregator;

    /**
     * 消费访问记录消息。
     *
     * <p>流程：</p>
     * <ol>
     *   <li>解析 JSON 消息；</li>
     *   <li>委托给 {@link StatsAggregator#aggregate(JSONObject)} 进行内存聚合与明细落库；</li>
     *   <li>解析失败记录日志，不重试（毒丸消息隔离）。</li>
     * </ol>
     *
     * <p>注：消费线程模型由 Spring Kafka 容器控制（concurrency 参数），本方法无状态，
     * 多线程并发调用安全（StatsAggregator 内部 ConcurrentHashMap + synchronized 保证线程安全）。</p>
     *
     * @param message Kafka 消息体（访问记录 JSON 字符串）
     */
    @KafkaListener(topics = ShortLinkConstants.TOPIC_ACCESS_RECORD, groupId = "short-link-consumer")
    public void onAccessRecord(String message) {
        try {
            // 1. 解析 JSON 消息
            JSONObject record = JSONUtil.parseObj(message);
            // 2. 委托聚合器处理（内存聚合 + Redis UV/IP 去重 + 明细落库）
            statsAggregator.aggregate(record);
        } catch (Exception e) {
            // 解析失败记录日志，不重试（避免毒丸消息阻塞分区消费）
            log.error("访问记录消息解析失败，丢弃：message={}", message, e);
        }
    }
}
